// Copyright © 2025 CCP ehf.
#include <memory>

#include "consumer.h"
using namespace monolith_grpc;
using namespace monolith_grpc::client;

// client/common
#include "carbongrpc/client/metadata.h"
#include "carbongrpc/client/stream_counter.h"

// prometheus
#include "counter_interface.h"
#include "gauge_interface.h"
#include "metric_registry_interface.h"

template<typename MessageType>
Consumer<MessageType>::Consumer()
  : request_disconnect_(false),
    context_(nullptr),
    connection_listener_(std::make_shared<monolith_grpc::client::Channel<Connection::ConnectionMessage>>()),

    is_subscribed_(false),  // managed by the child classes

    acks_enabled_(false),
    max_message_size_bytes_(4194304),  //< 4 MiB, https://github.com/grpc/grpc-go/issues/746

    stream_id_(0),

    metric_registry_(nullptr) {
  set_consumer_state(ClientState::kUnknown);
}

template<typename MessageType>
void Consumer<MessageType>::Initialize() {
  consumer_thread_ = std::thread([this] { this->ConsumerThread(); });
  metrics_thread_ = std::thread([this] {
    while (this->shutting_down_ == false) {
      this->CommitMetrics();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
}

template<typename MessageType>
void Consumer<MessageType>::Shutdown() {
  shutting_down_ = true;

  {
    std::scoped_lock<std::mutex> request_lock(request_lock_);
    request_disconnect_ = true;
    request_condition_.notify_all();
  }

  {
    std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
    message_queue_condition_.notify_all();
  }

  {
    std::scoped_lock<std::mutex> context_lock(context_lock_);
    if (context_) {
      context_->TryCancel();
    }
  }

  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }

  if (metrics_thread_.joinable()) {
    metrics_thread_.join();
  }

  jobs_.Wait();
}

template<typename MessageType>
void Consumer<MessageType>::SetConnection(std::shared_ptr<Connection> connection) {
  Async([=] {
    connection_.reset();

    // End the writer and reader threads
    {
      std::scoped_lock<std::mutex> request_lock(request_lock_);
      request_disconnect_ = true;
      request_condition_.notify_all();
    }

    {
      std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
      message_queue_condition_.notify_all();
    }

    {
      std::scoped_lock<std::mutex> context_lock(context_lock_);
      if (context_) {
        context_->TryCancel();
      }
    }

    // Wait for the consumer thread to reset
    while (consumer_state() == ClientState::kActive) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    };

    // Now we can set the new connection and resume the consumer thread
    auth_rejected_ = false;
    request_disconnect_ = false;
    connection_ = connection;
    if (connection) {
      connection_listener_ = connection->GetListener();
    }
  });
}

template<typename MessageType>
typename Consumer<MessageType>::ReceivedMessage Consumer<MessageType>::GetNextMessage() {
  ReceivedMessage message;
  message.valid = false;
  bool queue_empty = false;

  while (message.valid == false && !queue_empty) {
    // Retrieve a message
    std::scoped_lock<std::mutex> lock(message_queue_lock_);
    if (!message_queue_.empty()) {
      message = message_queue_.front();
      message_queue_.pop();
    } else {
      queue_empty = true;
    }
    num_messages_ready_ = (unsigned int)message_queue_.size();
    message_queue_condition_.notify_all();

    // Invalid if the stream id doesn't match the current stream (can't ack for
    // old streams)
    if (message.valid == true && message.stream_id != stream_id_) {
      message.valid = false;
      total_messages_dropped_.Increment();
    }
  }

  // Update metrics
  if (message.valid) {
    reconnect_backoff_.NoteHealthy();

    current_messages_queued_.Decrement();
    current_bytes_queued_.Decrement((double)message.data.length());

    total_messages_consumed_.Increment();
    current_messages_processing_.Increment();

    std::scoped_lock<std::mutex> message_times_lock(message_times_lock_);
    auto iter = message_times_.find(message.id);
    if (iter != message_times_.end()) {
      iter->second.retrieved = clock::now();
    }
  }

  if (!acks_enabled_) {
    UpdateTimingMetrics(message.id);
  }

  return message;
}

template<typename MessageType>
PublishResult Consumer<MessageType>::AcknowledgeMessage(
  unsigned long long id, unsigned int stream_id, bool positive, bool requeue, ::google::protobuf::Message* response,
  ::google::protobuf::Message* response_payload
) {
  if (!acks_enabled_) {
    return PublishResult::kFailure;
  }

  // Check message size
  size_t byte_size = 0;

  if (response != nullptr) {
    byte_size += response->ByteSizeLong();
  }

  if (response_payload != nullptr) {
    byte_size += response_payload->ByteSizeLong();
  }

  if (max_message_size_bytes_ > 0 && byte_size > max_message_size_bytes_) {
    return PublishResult::kMessageSizeLimitExceeded;
  }

  // Build ack
  Acknowledgement ack;
  ack.id = id;
  ack.stream_id = stream_id;
  ack.positive = positive;
  ack.requeue = requeue;
  ack.response = response;
  ack.response_payload = response_payload;

  current_messages_processing_.Decrement();

  {
    std::scoped_lock<std::mutex> acks_lock(acks_lock_);
    completed_messages_.push_back(ack);
  }

  {
    std::scoped_lock<std::mutex> message_times_lock(message_times_lock_);
    auto iter = message_times_.find(id);
    if (iter != message_times_.end()) {
      iter->second.processed = clock::now();
    }
  }

  return PublishResult::kSuccess;
}

template<typename MessageType>
unsigned int Consumer<MessageType>::num_messages_ready() const {
  return num_messages_ready_;
}

template<typename MessageType>
void Consumer<MessageType>::set_max_message_size_bytes(size_t max_message_size_bytes) {
  max_message_size_bytes_ = max_message_size_bytes;
}

template<typename MessageType>
std::list<StreamStatus> Consumer<MessageType>::GetStatusLog() {
  return stream_status_log_.GetStatusLog();
}

template<typename MessageType>
void Consumer<MessageType>::set_metric_registry(
  prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
  const std::map<std::string, std::string>& labels, AsyncMode async
) {
  auto lambda = [=] {
    std::unique_lock<std::mutex> lock(metrics_lock_);

    metric_registry_ = registry;
    metric_prefix_ = prefix;
    metric_labels_ = labels;

    MetricsCreator metrics_creator;
    metrics_creator.set_registry(registry);
    metrics_creator.set_prefix(prefix);
    metrics_creator.set_labels(labels);

    if (registry == nullptr) {
      total_messages_consumed_ = CachedCounter();
      total_messages_acknowledged_ = CachedCounter();
      total_acks_dropped_ = CachedCounter();
      total_messages_dropped_ = CachedCounter();

      current_messages_queued_ = CachedGauge();
      current_bytes_queued_ = CachedGauge();
      current_messages_processing_ = CachedGauge();

      time_in_queue_ = CachedHistogram();
      time_processing_ = CachedHistogram();
      time_acknowledging_ = CachedHistogram();
      total_time_to_completion_ = CachedHistogram();
    } else {
      metric_registry_ = registry;

      static std::string component_key = "component";
      static std::string component_value = "consumer";

      metrics_creator.assign_label("pb_domain", metrics_domain());
      metrics_creator.assign_label("pb_message", metrics_message());

      // Message and queue metrics

      total_messages_consumed_ = metrics_creator.MakeCounter("total_messages_consumed");

      total_messages_dropped_ = metrics_creator.MakeCounter("total_messages_dropped");
      metrics_creator.assign_label(component_key, component_value);
      current_messages_queued_ = metrics_creator.MakeGauge("messages_queued");
      current_bytes_queued_ = metrics_creator.MakeGauge("bytes_queued");
      metrics_creator.remove_label(component_key);  // https://github.com/ccpgames/eve-monolith-grpc/issues/251

      if (acks_enabled_) {
        total_messages_acknowledged_ = metrics_creator.MakeCounter("total_messages_acknowledged");
        total_acks_dropped_ = metrics_creator.MakeCounter("total_acks_dropped");
        current_messages_processing_ = metrics_creator.MakeGauge("messages_processing");
      }

      // Timing metrics

      std::vector<double> boundaries = {1, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000};

      metrics_creator.assign_label(component_key, component_value);
      time_in_queue_ = metrics_creator.MakeHistogram("time_in_queue_ms", boundaries);
      metrics_creator.remove_label(component_key);  // https://github.com/ccpgames/eve-monolith-grpc/issues/251

      total_time_to_completion_ = metrics_creator.MakeHistogram("total_time_to_completion_ms", boundaries);

      if (acks_enabled_) {
        time_processing_ = metrics_creator.MakeHistogram("time_processing_ms", boundaries);
        time_acknowledging_ = metrics_creator.MakeHistogram("time_acknowledging_ms", boundaries);
      }

      // Consumer state

      const std::string state_metric = "consumer_state";
      const std::string state_key = "state";

      metrics_creator.assign_label(state_key, "unknown");
      consumer_state_unknown_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "connecting");
      consumer_state_connecting_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "active");
      consumer_state_active_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "shut_down");
      consumer_state_shut_down_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "auth_rejected");
      consumer_state_auth_rejected_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.remove_label(state_key);

      lock.unlock();
      UpdateConsumerStateMetrics(consumer_state_);
    }
  };

  if (async == AsyncMode::kAsync) {
    Async(lambda);
  } else {
    lambda();
  }
}

template<typename MessageType>
grpc_connectivity_state Consumer<MessageType>::channel_state() {
  grpc_connectivity_state result = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;
  WithConnection([&result](Connection& connection) { result = connection.channel_state(); });
  return result;
}

template<typename MessageType>
int Consumer<MessageType>::active_connection_id() {
  int result = -1;
  WithConnection([&result](Connection& connection) { result = connection.id(); });
  return result;
}

template<typename MessageType>
unsigned int Consumer<MessageType>::active_stream_id() const {
  return stream_id_;
}

template<typename MessageType>
void Consumer<MessageType>::set_consumer_state(ClientState state) {
  if (consumer_state_ == state) {
    return;
  }

  static std::map<ClientState, const char*> states = {
    {ClientState::kUnknown, "Unknown"},
    {ClientState::kConnecting, "Connecting"},
    {ClientState::kActive, "Active"},
    {ClientState::kShutDown, "Shutdown"},
    {ClientState::kAuthRejected, "AuthRejected"},
  };
  // printf("consumer state %s -> %s\n", states[consumer_state_],
  // states[state]);

  consumer_state_ = state;

  UpdateConsumerStateMetrics(state);
}

template<typename MessageType>
ClientState Consumer<MessageType>::consumer_state() const {
  return consumer_state_;
}

template<typename MessageType>
void Consumer<MessageType>::set_auth_stop_enabled(bool enabled) {
  auth_stop_enabled_ = enabled;
}

template<typename MessageType>
bool Consumer<MessageType>::auth_stop_enabled() const {
  return auth_stop_enabled_;
}

template<typename MessageType>
void Consumer<MessageType>::RecordFinishStatus(const grpc::Status& status) {
  last_finish_code_ = (int)status.error_code();
  stream_status_log_.RecordStatus(stream_id_, status);
}

template<typename MessageType>
void Consumer<MessageType>::WaitBeforeReconnect() {
  reconnect_backoff_.NoteStreamEnd();

  if (shutting_down_ || request_disconnect_) {
    return;
  }

  if (auth_stop_enabled_ && last_finish_code_ == (int)grpc::StatusCode::UNAUTHENTICATED) {
    // Retrying with the same credentials can never succeed; park until new
    // credentials arrive, the connection is replaced, or we shut down.
    auth_rejected_ = true;
    set_consumer_state(ClientState::kAuthRejected);
    const auto parked_generation = Metadata::auth_generation();
    while (!shutting_down_ && !request_disconnect_ && auth_rejected_ &&
           Metadata::auth_generation() == parked_generation) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auth_rejected_ = false;
    last_finish_code_ = -1;
    reconnect_backoff_.Reset();
    return;
  }

  reconnect_backoff_.SleepFor(reconnect_backoff_.NextDelay(), [this] {
    return shutting_down_.load() || request_disconnect_.load();
  });
}

template<typename MessageType>
void Consumer<MessageType>::Async(const std::function<void()>& lambda) {
  if (shutting_down_) {
    return;
  }

  jobs_.Run(lambda);
}

template<typename MessageType>
void Consumer<MessageType>::WithConnection(const std::function<void(Connection&)>& f) {
  std::shared_ptr<Connection> connection(connection_);
  if (connection) {
    f(*connection);
  }
}

template<typename MessageType>
void Consumer<MessageType>::ConnectionMonitor() {
  while (!shutting_down_) {
    if (connection_listener_ && !connection_listener_->closed()) {
      Connection::ConnectionMessage message;
      if (connection_listener_->Read(message)) {
        if (message == Connection::ConnectionMessage::kConnected) {
          auth_rejected_ = false;
          request_disconnect_ = false;
        } else if (message == Connection::ConnectionMessage::kDisconnected) {
          {
            std::scoped_lock<std::mutex, std::mutex> locks(request_lock_, context_lock_);

            request_disconnect_ = true;
            request_condition_.notify_all();
          }

          {
            std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
            message_queue_condition_.notify_all();
          }

          {
            std::scoped_lock<std::mutex> context_lock(context_lock_);
            if (context_ != nullptr) {
              context_->TryCancel();
            }
          }
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

template<typename MessageType>
void Consumer<MessageType>::ConsumerThread() {

  auto connection_monitor = std::async(std::launch::async, [=] { ConnectionMonitor(); });
  while (!shutting_down_) {
    set_consumer_state(ClientState::kConnecting);
    while (true) {
      // Don't burn CPU while waiting for a connection
      std::this_thread::sleep_for(std::chrono::milliseconds(1));

      if (shutting_down_) {
        break;
      }

      if (request_disconnect_) {
        // Connect() wants to replace the existing channel, so wait for it to
        // finish doing that first
        continue;
      }

      bool ready = false;
      WithConnection([=, &ready](Connection& connection) {
        connection.UpdateChannelState();
        ready = connection.ready();
      });

      if (ready) {
        // channel is ready for messages now, so move on
        break;
      }
    }

    std::shared_ptr<Connection> connection(connection_);
    if (!connection) {
      continue;
    }

    channel_ = connection->channel();
    if (!channel_) {
      continue;
    }

    bool stream_prepared = false;
    {
      std::scoped_lock<std::mutex> context_lock(context_lock_);

      context_ = std::make_unique<grpc::ClientContext>();
      monolith_grpc::client::Metadata::ApplyToContext(*context_);

      stream_prepared = PrepareStream(context_.get());
    }

    if (!stream_prepared) {
      // outside context_lock_: SetConnection/ConnectionMonitor must be able
      // to grab the lock (and abort this wait) while we sleep
      WaitBeforeReconnect();
      continue;
    }
    stream_id_ = StreamCounter::AssignStreamId();

    last_finish_code_ = -1;
    reconnect_backoff_.NoteStreamStart();
    set_consumer_state(ClientState::kActive);

    acks_exit_requested_ = false;
    std::future<void> acks = std::async(std::launch::async, [=] { SendAcknowledgements(); });

    reader_exit_requested_ = false;
    std::future<void> reader = std::async(std::launch::async, [=] { ReadMessages(); });

    // Handle operations
    std::unique_lock<std::mutex> request_lock(request_lock_);
    while (!request_disconnect_ && !shutting_down_) {
      request_condition_.wait_for(request_lock, std::chrono::milliseconds(1));

      connection->UpdateChannelState();

      auto reader_status = reader.wait_for(std::chrono::milliseconds(0));
      if (reader_status == std::future_status::ready) {
        break;
      }

      if (acks_enabled_ && !operations_.empty()) {
        // Retrieve request
        std::function<bool()> operation = std::move(operations_.front());
        operations_.pop();

        request_lock.unlock();

        // Only perform operations if the queue is empty
        std::unique_lock<std::mutex> queue_lock(message_queue_lock_);
        while (!request_disconnect_ && !shutting_down_ && !message_queue_.empty()) {
          message_queue_condition_.wait(queue_lock);
        }
        queue_lock.unlock();

        if (request_disconnect_ || shutting_down_) {
          break;
        }

        // Execute request
        bool ok = operation();
        if (!ok) {
          break;
        }

        // Need to reacquire request_lock since we released it above and
        // request_condition_.wait_for(...) expects it to be held
        request_lock.lock();
      }
    }

    acks_exit_requested_ = true;
    reader_exit_requested_ = true;

    // the operations loop can exit with request_lock_ held or released;
    // never carry it into stream teardown and the reconnect wait
    if (request_lock.owns_lock()) {
      request_lock.unlock();
    }

    acks.get();
    reader.get();

    FinishStream();

    stream_id_ = 0;
    {
      std::scoped_lock<std::mutex> context_lock(context_lock_);
      context_.reset(nullptr);
    }
    channel_ = nullptr;

    WaitBeforeReconnect();
  }

  // todo: cleanup remaining messages

  connection_monitor.get();
  set_consumer_state(ClientState::kShutDown);
}

template<typename MessageType>
void Consumer<MessageType>::AddOperation(std::function<bool()> operation) {
  std::scoped_lock<std::mutex> request_lock(request_lock_);
  operations_.push(operation);
  request_condition_.notify_all();
}

template<typename MessageType>
bool Consumer<MessageType>::subscribed() const {
  return is_subscribed_;
}

template<typename MessageType>
void Consumer<MessageType>::ReadMessages() {
  ReadMessagesInternal();
}

template<typename MessageType>
void Consumer<MessageType>::SendAcknowledgements() {
  while (!shutting_down_ && !request_disconnect_ && !acks_exit_requested_) {
    // Wait for subscription
    while (!shutting_down_ && !request_disconnect_ && !acks_exit_requested_ && !is_subscribed_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::vector<Acknowledgement> pending_acks;
    {
      std::unique_lock<std::mutex> lock(acks_lock_);
      if (!completed_messages_.empty()) {
        pending_acks = std::move(completed_messages_);
        completed_messages_ = std::vector<Acknowledgement>();
      }
    }

    std::vector<Acknowledgement> acks;
    for (auto ack : pending_acks) {
      if (ack.stream_id == stream_id_) {
        acks.push_back(ack);
      } else {
        total_acks_dropped_.Increment();
      }
    }

    if (acks.empty() == false && acks_enabled_) {
      bool ok = Acknowledge(acks);

      if (ok) {
        for (auto ack : acks) {
          if (ack.response != nullptr) {
            // Python layer cloned the original response and passed ownership to
            // us. Message has a virtual destructor, so this is safe.
            delete ack.response;
          }
          if (ack.response_payload != nullptr) {
            // Python layer cloned the original response and passed ownership to
            // us. Message has a virtual destructor, so this is safe.
            delete ack.response_payload;
          }
        }
      } else {
        std::unique_lock<std::mutex> lock(acks_lock_);
        // Can't push_front on a vector, so make a new vector and replace the old one
        pending_acks.insert(pending_acks.end(), completed_messages_.begin(), completed_messages_.end());
        completed_messages_ = std::move(pending_acks);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

template<typename MessageType>
void Consumer<MessageType>::CommitMetrics() {
  std::scoped_lock<std::mutex> lock(metrics_lock_);

  total_messages_consumed_.Commit();
  total_messages_acknowledged_.Commit();
  total_messages_dropped_.Commit();

  current_messages_queued_.Commit();
  current_bytes_queued_.Commit();
  current_messages_processing_.Commit();

  time_in_queue_.Commit();
  if (acks_enabled_) {
    time_processing_.Commit();
    time_acknowledging_.Commit();
    total_acks_dropped_.Commit();
  }
  total_time_to_completion_.Commit();

  consumer_state_unknown_.Commit();
  consumer_state_connecting_.Commit();
  consumer_state_active_.Commit();
  consumer_state_shut_down_.Commit();
  consumer_state_auth_rejected_.Commit();
}

template<typename MessageType>
void Consumer<MessageType>::UpdateTimingMetrics(unsigned long long id) {
  auto time_completed = clock::now();

  MessageTiming times;

  {
    std::scoped_lock<std::mutex> lock(message_times_lock_);

    auto iter = message_times_.find(id);
    if (iter == message_times_.end()) {
      return;
    }

    times = iter->second;
    message_times_.erase(iter);
  }

  time_in_queue_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(times.retrieved - times.pulled).count()
  );
  time_processing_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(times.processed - times.retrieved).count()
  );
  time_acknowledging_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(time_completed - times.processed).count()
  );
  total_time_to_completion_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(time_completed - times.pulled).count()
  );
}

template<typename MessageType>
void Consumer<MessageType>::UpdateConsumerStateMetrics(ClientState state) {
  if (state == ClientState::kUnknown) {
    consumer_state_unknown_.Set((double)1);
  } else {
    consumer_state_unknown_.Set((double)0);
  }

  if (state == ClientState::kConnecting) {
    consumer_state_connecting_.Set((double)1);
  } else {
    consumer_state_connecting_.Set((double)0);
  }

  if (state == ClientState::kActive) {
    consumer_state_active_.Set((double)1);
  } else {
    consumer_state_active_.Set((double)0);
  }

  if (state == ClientState::kShutDown) {
    consumer_state_shut_down_.Set((double)1);
  } else {
    consumer_state_shut_down_.Set((double)0);
  }

  if (state == ClientState::kAuthRejected) {
    consumer_state_auth_rejected_.Set((double)1);
  } else {
    consumer_state_auth_rejected_.Set((double)0);
  }
}
