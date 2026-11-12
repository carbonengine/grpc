// Copyright © 2025 CCP ehf.
#include "carbongrpc/client/connection.h"
using namespace monolith_grpc::client;

// prometheus
#include "counter_interface.h"
#include "gauge_interface.h"
#include "metric_registry_interface.h"

std::atomic<int> Connection::next_connection_id_(0);
std::mutex Connection::connection_id_mutex_;

Connection::Connection()
  : connection_id_(-1),

    channel_(nullptr),
    channel_state_(grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN),
    channel_replaced_(false),
    channel_state_notification_pending_(false),
    old_channel_state_(grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN) {
}

Connection::~Connection() {
  // Make sure the cq shuts down cleanly
  channel_state_cq_.Shutdown();
  bool cq_not_empty = true;
  while (cq_not_empty) {
    void* tag;
    bool ok;
    cq_not_empty = channel_state_cq_.Next(&tag, &ok);
  }

  // Finish up any pending connection attempts
  connection_attempts_.Wait();
}

void Connection::Connect(const ConnectParams& params) {
  connection_attempts_.Run([=] {
    auto cancel = std::make_shared<std::atomic<bool>>(false);

    {
      std::scoped_lock<std::mutex> cancel_connect_lock(cancel_connect_lock_);

      // Cancel previous ongoing connection attempt
      if (cancel_connect_) {
        cancel_connect_->store(true);
      }

      // Mark this connection attempt as the active one
      cancel_connect_ = cancel;
    }

    {
      std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);
      if (metrics_.connect_called_ != nullptr) {
        metrics_.connect_called_->Increment();
      }
    }

    auto credentials = grpc::InsecureChannelCredentials();
    if (!params.root.empty()) {
      grpc::SslCredentialsOptions ssl_options;
      ssl_options.pem_root_certs = params.root;
      ssl_options.pem_cert_chain = params.cert;
      ssl_options.pem_private_key = params.key;
      credentials = grpc::SslCredentials(ssl_options);
    }

    grpc::ChannelArguments channel_args;
    if (!params.server_name_override.empty()) {
      channel_args.SetSslTargetNameOverride(params.server_name_override);
    }
    if (params.initial_reconnect_backoff_millis >= 0) {
      channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, params.initial_reconnect_backoff_millis);
    }
    if (params.max_reconnect_backoff_millis >= 0) {
      channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, params.max_reconnect_backoff_millis);
    }

    auto new_channel = grpc::CreateCustomChannel(params.host, credentials, channel_args);
    new_channel->GetState(true);

    {
      std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);
      if (metrics_.connections_attempted_ != nullptr) {
        metrics_.connections_attempted_->Increment();
      }
    }

    // Wait until either the old channel disconnects or the new channel has
    // finished connecting This is to prevent unnecessary interruptions in the
    // message flow
    while (channel_ != nullptr && channel_->GetState(false) == grpc_connectivity_state::GRPC_CHANNEL_READY &&
           new_channel->GetState(true) != grpc_connectivity_state::GRPC_CHANNEL_READY && !cancel->load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));

      auto old_channel_state = channel_state();
      if (old_channel_state == grpc_connectivity_state::GRPC_CHANNEL_READY) {
        auto new_channel_state = new_channel->GetState(false);
        if (new_channel_state == grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN) {
          // Bail out completely
          {
            std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);
            if (metrics_.connections_failed_ != nullptr) {
              metrics_.connections_failed_->Increment();
            }
          }
          return;
        }
      }
    }

    std::scoped_lock<std::mutex> lock(channel_lock_);

    // Do nothing if this connection attempt has been cancelled
    {
      std::scoped_lock<std::mutex> cancel_connect_lock(cancel_connect_lock_);

      // Un-mark this current call as the ongoing connection attempt
      if (cancel_connect_ == cancel) {
        cancel_connect_ = nullptr;
      }

      if (cancel->load()) {
        {
          std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);
          if (metrics_.connections_cancelled_ != nullptr) {
            metrics_.connections_cancelled_->Increment();
          }
        }
        return;
      }
    }

    if (new_channel->GetState(false) == grpc_connectivity_state::GRPC_CHANNEL_READY) {
      std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);
      if (metrics_.connections_succeeded_ != nullptr) {
        metrics_.connections_succeeded_->Increment();
      }
    }

    // Notify metrics that the old channel is going away
    UpdateChannelStateMetrics(channel_state_, grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN);
    channel_state_ = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;

    connection_id_ = AssignConnectionId();

    // Replace the active channel
    channel_ = new_channel;
    channel_replaced_ = true;
    channel_condition_.notify_all();

    // Notify listeners
    connection_state_publisher_.Write(ConnectionMessage::kConnected);

    return;
  });
}

void Connection::Disconnect() {
  int target_id = connection_id_;
  std::scoped_lock<std::mutex> disconnect_lock(disconnect_lock_);

  if (target_id != connection_id_) {
    return;
  }

  {
    // Kill the channel
    std::scoped_lock<std::mutex> lock(channel_lock_);
    channel_ = nullptr;
    channel_condition_.notify_all();
  }

  UpdateChannelStateMetrics(channel_state_, grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN);
  channel_state_ = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;

  // Notify listeners
  connection_state_publisher_.Write(ConnectionMessage::kDisconnected);
}

// cppcheck-suppress unusedFunction
bool Connection::WaitForChannelState(
  const std::vector<grpc_connectivity_state>& target_states, int timeout_millis, const std::atomic<bool>& cancel
) {
  if (target_states.empty()) {
    return false;
  }

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_millis);
  auto current_state = GetChannelState(true, BlockingMode::kBlocking, cancel);

  // Watch until deadline, or forever (until the desired state is reached) if
  // timeout_millis is negative.
  while (std::chrono::steady_clock::now() < deadline || timeout_millis < 0) {
    current_state = GetChannelState(true, BlockingMode::kBlocking, cancel);
    if (std::find(target_states.begin(), target_states.end(), current_state) != target_states.end()) {
      break;
    }

    if (cancel == true) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (std::find(target_states.begin(), target_states.end(), current_state) != target_states.end()) {
    return true;
  }

  return false;
}

grpc_connectivity_state Connection::channel_state() {
  return channel_state_;
}

bool Connection::ready() const {
  if (channel_state_ == grpc_connectivity_state::GRPC_CHANNEL_READY) {
    return true;
  }

  if (channel_state_ == grpc_connectivity_state::GRPC_CHANNEL_IDLE) {
    return true;
  }

  return false;
}

// cppcheck-suppress unusedFunction
void Connection::UpdateChannelState() {
  std::atomic<bool> cancel;
  WithNonNullChannel(
    [=] {
    if (channel_state_notification_pending_ == false) {
      auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
      channel_->NotifyOnStateChange(
        old_channel_state_, deadline, &channel_state_cq_, static_cast<void*>(channel_.get())
      );
      channel_state_notification_pending_ = true;
    }

    void* tag;
    bool ok = false;
    auto status = channel_state_cq_.AsyncNext(&tag, &ok, gpr_inf_past(GPR_CLOCK_REALTIME));
    channel_state_ = channel_->GetState(true);

    // If Connect() replaced the channel, then we need to re-initialize
    // old_state so we're not tracking changes from the old channel to the
    // new channel.  This also prevents duplicate connections_attempted
    // metrics.
    if (channel_replaced_ == true) {
      old_channel_state_ = channel_state_;
      channel_replaced_ = false;
      channel_state_notification_pending_ = false;
    }

    bool got_notification = false;
    if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT && ok == true && tag == static_cast<void*>(channel_.get())) {
      got_notification = true;
      channel_state_notification_pending_ = false;
    }

    if (got_notification == true || (channel_state_ != old_channel_state_)) {
      UpdateChannelStateMetrics(old_channel_state_, channel_state_);
      old_channel_state_ = channel_state_;
    }
    },
    monolith_grpc::BlockingMode::kNonBlocking, cancel);
}

std::shared_ptr<grpc::Channel> Connection::channel() {
  return channel_;
}

bool Connection::WithNonNullChannel(
  const std::function<void()>& lambda, monolith_grpc::BlockingMode blocking, const std::atomic<bool>& cancel
) {
  std::unique_lock<std::mutex> lock(channel_lock_);
  if (blocking == monolith_grpc::BlockingMode::kBlocking) {
    while (!channel_ && cancel == false) {
      channel_condition_.wait(lock);
    }
    if (channel_) {
      lambda();
      return true;
    }
  } else {
    if (channel_) {
      lambda();
      return true;
    }
  }

  return false;
}

std::shared_ptr<Channel<Connection::ConnectionMessage>> Connection::GetListener() {
  return connection_state_publisher_.GetListener();
}

grpc_connectivity_state Connection::GetChannelState(
  bool try_to_reconnect, monolith_grpc::BlockingMode blocking, const std::atomic<bool>& cancel
) {
  // GRPC_CHANNEL_SHUTDOWN is the value that maps most closely to "channel does
  // not exist", so that's what we'll return if channel_==nullptr and
  // blocking==false
  grpc_connectivity_state result = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;
  WithNonNullChannel([&] { result = channel_->GetState(try_to_reconnect); }, blocking, cancel);
  channel_state_ = result;
  return result;
}

void Connection::set_metric_registry(
  prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
  const std::map<std::string, std::string>& labels
) {
  std::scoped_lock<std::mutex> metrics_lock(metrics_lock_);

  if (registry == nullptr) {
    Metrics empty_metrics;
    metrics_ = empty_metrics;
  }

  else if (registry != metrics_.metric_registry_ || prefix != metrics_.metric_prefix_ || labels != metrics_.metric_labels_) {
    Metrics new_metrics = CreateMetrics(registry, prefix, labels);
    metrics_ = new_metrics;
  }
}

int Connection::id() {
  return connection_id_;
}

int Connection::AssignConnectionId() {
  std::scoped_lock<std::mutex> lock(connection_id_mutex_);
  int result = next_connection_id_;
  next_connection_id_++;
  return result;
}

Connection::Metrics Connection::CreateMetrics(
  prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
  const std::map<std::string, std::string>& labels
) {
  Metrics metrics;

  metrics.metric_registry_ = registry;
  metrics.metric_prefix_ = prefix;
  metrics.metric_labels_ = labels;

  if (registry == nullptr) {
    return metrics;
  }

  std::vector<const char*> label_keys;
  std::vector<const char*> label_values;

  for (auto& iter : labels) {
    label_keys.push_back(iter.first.c_str());
    label_values.push_back(iter.second.c_str());
  }
  int num_labels = (int)label_keys.size();

  std::string metric_name = prefix + "connect_called";
  metrics.connect_called_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                              ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "connections_attempted";
  metrics.connections_attempted_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                     ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "connections_failed";
  metrics.connections_failed_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                  ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "connections_cancelled";
  metrics.connections_cancelled_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                     ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "connections_succeeded";
  metrics.connections_succeeded_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                     ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "disconnections_due_to_failure";
  metrics.disconnections_due_to_failure_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                             ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "disconnections_due_to_idleness";
  metrics.disconnections_due_to_idleness_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                              ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "disconnections_due_to_shutdown";
  metrics.disconnections_due_to_shutdown_ = registry->MakeCounter(metric_name.c_str(), num_labels, label_keys.data())
                                              ->WithLabelValues(num_labels, label_values.data());

  metric_name = prefix + "channel_state";
  metrics.channel_state_metric_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                    ->WithLabelValues(num_labels, label_values.data());

  num_labels++;
  label_keys.push_back("state");
  label_values.push_back("");
  auto channel_state_index = label_values.size() - 1;

  label_values[channel_state_index] = "unknown";
  metrics.channel_state_unknown_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                     ->WithLabelValues(num_labels, label_values.data());

  label_values[channel_state_index] = "idle";
  metrics.channel_state_idle_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                  ->WithLabelValues(num_labels, label_values.data());

  label_values[channel_state_index] = "connecting";
  metrics.channel_state_connecting_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                        ->WithLabelValues(num_labels, label_values.data());

  label_values[channel_state_index] = "ready";
  metrics.channel_state_ready_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                   ->WithLabelValues(num_labels, label_values.data());

  label_values[channel_state_index] = "transient_failure";
  metrics.channel_state_transient_failure_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                               ->WithLabelValues(num_labels, label_values.data());

  label_values[channel_state_index] = "shut_down";
  metrics.channel_state_shut_down_ = registry->MakeGauge(metric_name.c_str(), num_labels, label_keys.data())
                                       ->WithLabelValues(num_labels, label_values.data());

  num_labels--;
  label_keys.pop_back();
  label_values.pop_back();

  return metrics;
}

void Connection::UpdateChannelStateMetrics(grpc_connectivity_state old_state, grpc_connectivity_state new_state) {
  static std::map<grpc_connectivity_state, const char*> states = {
    {grpc_connectivity_state::GRPC_CHANNEL_IDLE, "Idle"},
    {grpc_connectivity_state::GRPC_CHANNEL_CONNECTING, "Connecting"},
    {grpc_connectivity_state::GRPC_CHANNEL_READY, "Ready"},
    {grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE, "Failure"},
    {grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN, "Shutdown"}};
  // printf("channel state change %s -> %s\n", states[old_state],
  // states[new_state]);

  std::scoped_lock<std::mutex> lock(metrics_lock_);

  if (metrics_.channel_state_metric_ != nullptr) {
    metrics_.channel_state_metric_->Set((int)new_state);
  }

  if (metrics_.channel_state_unknown_ != nullptr) {
    metrics_.channel_state_unknown_->Set(0);
  }
  if (metrics_.channel_state_idle_ != nullptr) {
    metrics_.channel_state_idle_->Set(0);
  }
  if (metrics_.channel_state_connecting_ != nullptr) {
    metrics_.channel_state_connecting_->Set(0);
  }
  if (metrics_.channel_state_ready_ != nullptr) {
    metrics_.channel_state_ready_->Set(0);
  }
  if (metrics_.channel_state_transient_failure_ != nullptr) {
    metrics_.channel_state_transient_failure_->Set(0);
  }
  if (metrics_.channel_state_shut_down_ != nullptr) {
    metrics_.channel_state_shut_down_->Set(0);
  }

  // Connecting
  if (new_state == grpc_connectivity_state::GRPC_CHANNEL_CONNECTING) {
    // Connecting -> Connecting = Incremental progress during connection
    // establishment Anything else -> Connecting = New connection attempt
    if (old_state != grpc_connectivity_state::GRPC_CHANNEL_CONNECTING) {
      if (metrics_.connections_attempted_ != nullptr) {
        metrics_.connections_attempted_->Increment();
      }
    }

    if (metrics_.channel_state_connecting_ != nullptr) {
      metrics_.channel_state_connecting_->Set(1);
    }
  }

  // Ready
  else if (new_state == grpc_connectivity_state::GRPC_CHANNEL_READY) {
    // Ready -> Ready = Incremental successful communication on established
    // channel Anything else -> Ready = Connection succeeded
    if (old_state != grpc_connectivity_state::GRPC_CHANNEL_READY) {
      if (metrics_.connections_succeeded_ != nullptr) {
        metrics_.connections_succeeded_->Increment();
      }
    }

    if (metrics_.channel_state_ready_ != nullptr) {
      metrics_.channel_state_ready_->Set(1);
    }
  }

  // Transient Failure
  else if (new_state == grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE) {
    // Ready -> Transient Failure = Any failure encountered while expecting
    // successful communication on established channel
    if (old_state == grpc_connectivity_state::GRPC_CHANNEL_READY) {
      if (metrics_.disconnections_due_to_failure_ != nullptr) {
        metrics_.disconnections_due_to_failure_->Increment();
      }
    }

    // Connecting -> Transient Failure = Any failure in any of the steps needed to establish connection
    // Anything else -> Transient Failure = Undefined.  Assume a missing Connecting state inbetween.
    else {
      if (metrics_.connections_failed_ != nullptr) {
        metrics_.connections_failed_->Increment();
      }
    }

    if (metrics_.channel_state_transient_failure_ != nullptr) {
      metrics_.channel_state_transient_failure_->Set(1);
    }
  }

  // Idle
  else if (new_state == grpc_connectivity_state::GRPC_CHANNEL_IDLE) {
    // Connecting -> Idle = No RPC activity on channel for IDLE_TIMEOUT
    // Ready -> Idle = No RPC activity on channel for IDLE_TIMEOUT OR upon
    // receiving a GOAWAY while there are no pending RPCs
    if (old_state == grpc_connectivity_state::GRPC_CHANNEL_CONNECTING || old_state == grpc_connectivity_state::GRPC_CHANNEL_READY) {
      if (metrics_.disconnections_due_to_idleness_ != nullptr) {
        metrics_.disconnections_due_to_idleness_->Increment();
      }
    }

    // Anything else -> Idle = Undefined
    // It's too hard to make any assumptions about those cases, so ignore them.

    if (metrics_.channel_state_idle_ != nullptr) {
      metrics_.channel_state_idle_->Set(1);
    }
  }

  // Shutdown
  else if (new_state == grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN) {
    // Shutdown -> Shutdown = Undefined
    if (old_state != grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN) {
      // Anything else -> Shutdown = Shutdown triggered by application
      if (metrics_.disconnections_due_to_shutdown_ != nullptr) {
        metrics_.disconnections_due_to_shutdown_->Increment();
      }

      if (metrics_.channel_state_shut_down_ != nullptr) {
        metrics_.channel_state_shut_down_->Set(1);
      }
    }
  }

  // Unknown
  else {
    if (metrics_.channel_state_unknown_ != nullptr) {
      metrics_.channel_state_unknown_->Set(1);
    }
  }
}
