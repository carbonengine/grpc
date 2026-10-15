// Copyright © 2025 CCP ehf.
#include <memory>

#include "broker.h"
using namespace monolith_grpc;
using namespace monolith_grpc::client;

// client/common
#include "carbongrpc/client/metadata.h"
#include "carbongrpc/client/stream_counter.h"

// prometheus
#include "counter_interface.h"
#include "gauge_interface.h"
#include "metric_registry_interface.h"

template<typename RequestType, typename ResponseType, typename ServiceType>
Broker<RequestType, ResponseType, ServiceType>::UnpackedMessage::UnpackedMessage(
  RequestType* in_message, void* in_py_message, google::protobuf::Message* in_payload, void* in_py_payload,
  unsigned long long in_id
)
  : message(in_message),
    py_message(in_py_message),
    payload(in_payload),
    py_payload(in_py_payload),
    id(in_id),
    time_received(std::chrono::steady_clock::now()) {
}

template<typename RequestType, typename ResponseType, typename ServiceType>
Broker<RequestType, ResponseType, ServiceType>::Broker()
  : shutting_down_(false),
    request_disconnect_(false),
    connection_listener_(std::make_shared<monolith_grpc::client::Channel<Connection::ConnectionMessage>>()),

    context_(nullptr),

    pending_messages_(0),
    paused_(false),

    max_message_size_bytes_(4194304),  //< 4 MiB, https://github.com/grpc/grpc-go/issues/746

    max_queue_size_bytes_(1024ll * 1024ll * 1024ll * 2ll),  //< 2 GiB
    queue_size_bytes_(0),

    next_assignable_id_(1),

    stream_id_(0),

    num_messages_ready_(0),
    num_bytes_ready_(0),

    pings_enabled_(false),
    ping_in_progress_(false),
    ping_result_(clock::duration(std::chrono::milliseconds(-1))),

    metric_registry_(nullptr),

    broker_state_unknown_(nullptr),
    broker_state_connecting_(nullptr),
    broker_state_active_(nullptr),
    broker_state_shut_down_(nullptr),
    broker_state_auth_rejected_(nullptr) {
  set_broker_state(ClientState::kUnknown);
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::Initialize() {
  broker_thread_ = std::thread([this] { this->BrokerThread(); });
  metrics_thread_ = std::thread([this] {
    while (this->shutting_down_ == false) {
      this->CommitMetrics();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::Shutdown() {
  shutting_down_ = true;

  {
    std::scoped_lock<std::mutex> request_lock(request_lock_);
    request_disconnect_ = true;
    request_condition_.notify_all();
  }

  {
    std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
    queue_condition_.notify_all();
  }

  if (broker_thread_.joinable()) {
    broker_thread_.join();
  }

  if (metrics_thread_.joinable()) {
    metrics_thread_.join();
  }

  jobs_.Wait();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::SetConnection(std::shared_ptr<Connection> connection) {
  Async([=] {
    connection_.reset();

    // End the writer and reader threads
    {
      std::scoped_lock<std::mutex, std::mutex> locks(request_lock_, context_lock_);

      request_disconnect_ = true;
      request_condition_.notify_all();

      if (context_ != nullptr) {
        context_->TryCancel();
      }
    }

    {
      std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
      queue_condition_.notify_all();
    }

    // Wait for the broker thread to reset
    while (broker_state() == ClientState::kActive) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    };

    // Now we can set the new connection and resume the broker thread
    auth_rejected_ = false;
    request_disconnect_ = false;
    connection_ = connection;
    if (connection) {
      connection_listener_ = connection->GetListener();
    }
  });
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::Ping() {
  ping_in_progress_ = true;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
Broker<RequestType, ResponseType, ServiceType>::clock::duration
Broker<RequestType, ResponseType, ServiceType>::PingResult() {
  return ping_result_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
bool Broker<RequestType, ResponseType, ServiceType>::PingInProgress() {
  return ping_in_progress_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::PingThread() {
  while (!shutting_down_ && !request_disconnect_ && !writer_exit_requested_ && !reader_exit_requested_) {
    if (ping_in_progress_) {
      clock::time_point start_time = clock::now();
      bool ok = SendPing();
      if (ok) {
        clock::time_point end_time = clock::now();
        ping_result_ = end_time - start_time;
        ping_in_progress_ = false;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
std::list<StreamStatus> Broker<RequestType, ResponseType, ServiceType>::GetStatusLog() {
  return stream_status_log_.GetStatusLog();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
PublishResult Broker<RequestType, ResponseType, ServiceType>::SendRequest(UnpackedMessage* message) {
  if (message == nullptr || message->message == nullptr) {
    return PublishResult::kFailure;
  }

  size_t byte_size = message->message->ByteSizeLong();
  if (message->payload != nullptr) {
    byte_size += message->payload->ByteSizeLong();
  }

  if (max_message_size_bytes_ > 0 && byte_size > max_message_size_bytes_) {
    messages_rejected_.Increment();
    return PublishResult::kMessageSizeLimitExceeded;
  }

  std::scoped_lock<std::mutex> lock(queue_lock_);

  if (max_queue_size_bytes_ > 0 && queue_size_bytes_ + byte_size > max_queue_size_bytes_) {
    messages_rejected_.Increment();
    return PublishResult::kQueueFull;
  }

  queue_size_bytes_ += byte_size;
  message->id = next_assignable_id_;
  next_assignable_id_++;

  queued_messages_.push_back(message);

  total_messages_seen_.Increment();
  total_bytes_seen_.Increment((double)byte_size);

  // Notify PublisherThread
  pending_messages_++;
  UpdateQueueMetrics();
  queue_condition_.notify_all();

  return PublishResult::kSuccess;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::WaitUntilPublishesComplete() {
  if (!broker_thread_.joinable()) {
    return;
  }

  if (paused_) {
    return;
  }

  if (!connection_) {
    return;
  }

  // Wait until pending_messages_ reaches zero
  std::unique_lock<std::mutex> lock(queue_lock_);
  while (!queued_messages_.empty()) {
    queue_condition_.wait(lock);
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_on_message_published(
  std::function<void(UnpackedMessage*)> callback
) {
  std::scoped_lock<std::mutex> lock(callback_lock_);
  on_message_published_ = callback;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_max_message_size_bytes(size_t max_bytes) {
  max_message_size_bytes_ = max_bytes;
  UpdateQueueMetrics();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_max_queue_size_bytes(size_t max_bytes) {
  max_queue_size_bytes_ = max_bytes;
  UpdateQueueMetrics();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
unsigned int Broker<RequestType, ResponseType, ServiceType>::num_pending_messages() const {
  return pending_messages_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::PauseSending() {
  std::scoped_lock<std::mutex> lock(queue_lock_);
  paused_ = true;
  queue_condition_.notify_all();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::ResumeSending() {
  std::scoped_lock<std::mutex> lock(queue_lock_);
  paused_ = false;
  queue_condition_.notify_all();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
typename Broker<RequestType, ResponseType, ServiceType>::ReceivedMessage
Broker<RequestType, ResponseType, ServiceType>::GetResponse() {
  ReceivedMessage message;
  message.valid = false;

  // Retrieve the message
  std::scoped_lock<std::mutex> lock(message_queue_lock_);
  if (!message_queue_.empty()) {
    message = message_queue_.front();
    message_queue_.pop();
  }
  num_messages_ready_ = (unsigned int)message_queue_.size();

  // Update metrics
  if (message.valid) {
    current_messages_queued_.Decrement();
    current_bytes_queued_.Decrement((double)message.data.length());

    total_messages_consumed_.Increment();

    std::scoped_lock<std::mutex> message_times_lock(message_times_lock_);
    auto iter = message_times_.find(message.id);
    if (iter != message_times_.end()) {
      iter->second.retrieved = clock::now();
    }
  }

  UpdateTimingMetrics(message.id);

  return message;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
unsigned int Broker<RequestType, ResponseType, ServiceType>::num_messages_ready() const {
  return num_messages_ready_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_metric_registry(
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
      // Receive

      total_messages_consumed_ = CachedCounter();

      current_messages_queued_ = CachedGauge();
      current_bytes_queued_ = CachedGauge();

      time_in_receive_queue_ = CachedHistogram();
      total_time_to_completion_ = CachedHistogram();

      // Send

      attempted_messages_published_ = CachedCounter();
      attempted_bytes_published_ = CachedCounter();
      messages_published_success_ = CachedCounter();
      bytes_published_success_ = CachedCounter();

      messages_rejected_ = CachedCounter();
      total_messages_seen_ = CachedCounter();
      total_bytes_seen_ = CachedCounter();
      messages_queued_ = CachedGauge();
      bytes_queued_ = CachedGauge();
      queue_utilization_percent_ = CachedGauge();

      time_in_send_queue_ = CachedHistogram();
      time_sending_ = CachedHistogram();
      total_time_to_delivery_ = CachedHistogram();

      // State
    } else {
      metric_registry_ = registry;

      static std::string component_key = "component";
      static std::string component_value = "broker";

      metrics_creator.assign_label("pb_domain", metrics_domain());
      metrics_creator.assign_label("pb_message", metrics_message());

      total_messages_consumed_ = metrics_creator.MakeCounter("incoming_total_messages_consumed");
      current_messages_queued_ = metrics_creator.MakeGauge("incoming_messages_queued");
      current_bytes_queued_ = metrics_creator.MakeGauge("incoming_bytes_queued");

      // Publishing
      attempted_messages_published_ = metrics_creator.MakeCounter("outgoing_attempted_messages_published");
      attempted_bytes_published_ = metrics_creator.MakeCounter("outgoing_attempted_bytes_published");
      messages_published_success_ = metrics_creator.MakeCounter("outgoing_messages_published_success");
      bytes_published_success_ = metrics_creator.MakeCounter("outgoing_bytes_published_success");

      // Send Queue
      messages_rejected_ = metrics_creator.MakeCounter("outgoing_messages_rejected");
      total_messages_seen_ = metrics_creator.MakeCounter("outgoing_total_messages_seen");
      total_bytes_seen_ = metrics_creator.MakeCounter("outgoing_total_bytes_seen");
      messages_queued_ = metrics_creator.MakeGauge("outgoing_messages_queued");
      bytes_queued_ = metrics_creator.MakeGauge("outgoing_bytes_queued");
      queue_utilization_percent_ = metrics_creator.MakeGauge("outgoing_queue_utilization_percent");

      // Timing metrics
      std::vector<double> boundaries = {1, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000};
      time_in_receive_queue_ = metrics_creator.MakeHistogram("incoming_time_in_queue_ms", boundaries);
      total_time_to_completion_ = metrics_creator.MakeHistogram("incoming_total_time_to_completion_ms", boundaries);
      time_in_send_queue_ = metrics_creator.MakeHistogram("outgoing_time_in_queue_ms", boundaries);
      time_sending_ = metrics_creator.MakeHistogram("outgoing_time_sending_ms", boundaries);
      total_time_to_delivery_ = metrics_creator.MakeHistogram("outgoing_total_time_to_delivery_ms", boundaries);

      // Broker state

      const std::string state_metric = "broker_state";
      const std::string state_key = "state";

      metrics_creator.assign_label(state_key, "unknown");
      broker_state_unknown_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "connecting");
      broker_state_connecting_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "active");
      broker_state_active_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "shut_down");
      broker_state_shut_down_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "auth_rejected");
      broker_state_auth_rejected_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.remove_label(state_key);

      lock.unlock();
      UpdateBrokerStateMetrics(broker_state_);
    }
  };

  if (async == AsyncMode::kAsync) {
    Async(lambda);
  } else {
    lambda();
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
grpc_connectivity_state Broker<RequestType, ResponseType, ServiceType>::channel_state() {
  grpc_connectivity_state result = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;
  WithConnection([&result](Connection& connection) { result = connection.channel_state(); });
  return result;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
int Broker<RequestType, ResponseType, ServiceType>::active_connection_id() {
  int result = -1;
  WithConnection([&result](Connection& connection) { result = connection.id(); });
  return result;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
unsigned int Broker<RequestType, ResponseType, ServiceType>::active_stream_id() const {
  return stream_id_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_broker_state(ClientState state) {
  if (broker_state_ == state) {
    return;
  }

  static std::map<ClientState, const char*> states = {
    {ClientState::kUnknown, "Unknown"},
    {ClientState::kConnecting, "Connecting"},
    {ClientState::kActive, "Active"},
    {ClientState::kShutDown, "Shutdown"},
    {ClientState::kAuthRejected, "AuthRejected"},
  };
  // printf("broker state %s -> %s\n", states[broker_state_], states[state]);

  broker_state_ = state;

  UpdateBrokerStateMetrics(state);
}

template<typename RequestType, typename ResponseType, typename ServiceType>
ClientState Broker<RequestType, ResponseType, ServiceType>::broker_state() const {
  return broker_state_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::set_auth_stop_enabled(bool enabled) {
  auth_stop_enabled_ = enabled;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
bool Broker<RequestType, ResponseType, ServiceType>::auth_stop_enabled() const {
  return auth_stop_enabled_;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::RecordFinishStatus(const grpc::Status& status) {
  last_finish_code_ = (int)status.error_code();
  stream_status_log_.RecordStatus(stream_id_, status);
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::WaitBeforeReconnect() {
  reconnect_backoff_.NoteStreamEnd();

  if (shutting_down_ || request_disconnect_) {
    return;
  }

  if (auth_stop_enabled_ && last_finish_code_ == (int)grpc::StatusCode::UNAUTHENTICATED) {
    // Retrying with the same credentials can never succeed; park until new
    // credentials arrive (Metadata::set_auth_token bumps the generation),
    // the connection is replaced, or we shut down.
    auth_rejected_ = true;
    set_broker_state(ClientState::kAuthRejected);
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

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::Async(const std::function<void()>& lambda) {
  if (shutting_down_) {
    return;
  }

  jobs_.Run(lambda);
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::WithConnection(const std::function<void(Connection&)>& f) {
  std::shared_ptr<Connection> connection(connection_);
  if (connection) {
    f(*connection);
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::ConnectionMonitor() {
  while (!shutting_down_) {
    if (connection_listener_ && !connection_listener_->closed()) {
      Connection::ConnectionMessage message;
      if (connection_listener_->Read(message)) {
        if (message == Connection::ConnectionMessage::kConnected) {
          auth_rejected_ = false;
          request_disconnect_ = false;
        } else if (message == Connection::ConnectionMessage::kDisconnected) {
          {
            std::scoped_lock<std::mutex> request_lock(request_lock_);
            request_disconnect_ = true;
            request_condition_.notify_all();
          }

          {
            std::scoped_lock<std::mutex> queue_lock(message_queue_lock_);
            queue_condition_.notify_all();
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

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::BrokerThread() {
  auto connection_monitor = std::async(std::launch::async, [=] { ConnectionMonitor(); });
  while (!shutting_down_) {
    set_broker_state(ClientState::kConnecting);
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
    set_broker_state(ClientState::kActive);

    {
      std::scoped_lock<std::mutex> lock(queue_lock_);
      unsigned long long id = 1;
      for (auto m : queued_messages_) {
        m->id = id;
        id++;
      }
      next_assignable_id_ = id;
    }

    writer_exit_requested_.store(false);
    std::future<void> writer = std::async([=] { PublishMessages(); });

    reader_exit_requested_.store(false);
    std::future<void> reader = std::async(std::launch::async, [=] { ReadMessages(); });

    std::future<void> ping_thread;
    if (pings_enabled_) {
      ping_thread = std::async([=] { PingThread(); });
    }

    // Wait until finished
    while (!request_disconnect_ && !shutting_down_) {
      connection->UpdateChannelState();

      auto reader_status = reader.wait_for(std::chrono::milliseconds(1));
      if (reader_status == std::future_status::ready) {
        writer_exit_requested_.store(true);
        queue_condition_.notify_all();
        break;
      }

      auto writer_status = writer.wait_for(std::chrono::milliseconds(1));
      if (writer_status == std::future_status::ready) {
        reader_exit_requested_.store(true);
        break;
      }
    }

    writer_exit_requested_.store(true);
    reader_exit_requested_.store(true);
    writer.get();
    reader.get();

    if (pings_enabled_) {
      ping_thread.get();
    }

    FinishStream();
    {
      std::scoped_lock<std::mutex> context_lock(context_lock_);
      context_.reset(nullptr);
    }

    WaitBeforeReconnect();
  }

  // todo: cleanup remaining messages

  connection_monitor.get();
  set_broker_state(ClientState::kShutDown);
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::PublishMessages() {
  while (true) {
    UnpackedMessage* unpacked = GetNextMessage();

    if (shutting_down_) {
      break;
    }

    if (request_disconnect_) {
      break;
    }

    if (writer_exit_requested_) {
      break;
    }

    if (unpacked == nullptr || unpacked->message == nullptr) {
      continue;
    }

    unpacked->time_retrieved = clock::now();

    if (unpacked->payload != nullptr) {
      unpacked->message->mutable_payload()->PackFrom(*unpacked->payload, "type.evetech.net");
    }

    auto message_size = unpacked->message->ByteSizeLong();
    attempted_messages_published_.Increment();
    attempted_bytes_published_.Increment((double)message_size);

    bool write_ok = WriteMessageToStream(*unpacked->message);

    if (write_ok) {
      unpacked->time_delivered = clock::now();
      messages_published_success_.Increment();
      bytes_published_success_.Increment((double)message_size);

      {
        std::scoped_lock<std::mutex> queue_lock(queue_lock_);
        queued_messages_.pop_front();

        pending_messages_ = (int)queued_messages_.size();
        queue_size_bytes_ = 0;
        for (UnpackedMessage* message : queued_messages_) {
          queue_size_bytes_ += message->message->ByteSizeLong();
        }
      }

      std::scoped_lock<std::mutex> callback_lock(callback_lock_);
      if (on_message_published_ != nullptr) {
        on_message_published_(unpacked);
      }

      UpdateQueueMetrics();
      UpdateTimingMetrics(unpacked);
    } else {
      break;  // write failure means the stream is dead, so exit the loop and
              // restart the stream
    }
  }

  WritesDone();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
bool Broker<RequestType, ResponseType, ServiceType>::WriteMessageToStream(const RequestType& message) {
  bool result = false;

  if (stream_) {
    result = stream_->Write(message);
  }

  return result;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::WritesDone() {
  if (stream_) {
    stream_->WritesDone();
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
typename Broker<RequestType, ResponseType, ServiceType>::UnpackedMessage*
Broker<RequestType, ResponseType, ServiceType>::GetNextMessage() {
  std::unique_lock<std::mutex> queue_lock(queue_lock_);
  Broker<RequestType, ResponseType, ServiceType>::UnpackedMessage* result = PeekNextMessage(queue_lock);

  while (result == nullptr && !shutting_down_ && !request_disconnect_ && !writer_exit_requested_) {
    queue_condition_.wait(queue_lock);
    result = PeekNextMessage(queue_lock);
  }

  return result;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
typename Broker<RequestType, ResponseType, ServiceType>::UnpackedMessage*
Broker<RequestType, ResponseType, ServiceType>::PeekNextMessage(std::unique_lock<std::mutex>& queue_lock) {
  if (paused_) {
    return nullptr;
  }

  if (queued_messages_.empty()) {
    return nullptr;
  }

  return *queued_messages_.begin();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::ReadMessages() {
  ReadMessagesInternal();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
bool Broker<RequestType, ResponseType, ServiceType>::PrepareStream(grpc::ClientContext* context) {
  stream_ = nullptr;

  stub_ = ServiceType::NewStub(channel_);

  if (stub_) {
    stream_ = stub_->Send(context);
  }

  if (stream_ == nullptr) {
    return false;
  }

  return true;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::FinishStream() {
  if (stream_) {
    // gRPC will block forever on Finish() if there are still messages to be
    // read so we first have to Read() until that returns false
    ResponseType message;
    bool ok = true;
    while (ok) {
      ok = stream_->Read(&message);
    }

    auto status = stream_->Finish();
    RecordFinishStatus(status);
  }

  stream_ = nullptr;

  stub_ = nullptr;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::ReadMessagesInternal() {
  while (!request_disconnect_ && !shutting_down_ && !reader_exit_requested_) {
    ReceivedMessage m;

    // Read a message from the gateway
    ResponseType message;
    bool read_ok = false;
    {
      std::scoped_lock<std::mutex> stream_lock(stream_read_lock_);
      read_ok = stream_->Read(&message);
    }
    m.times.pulled = clock::now();
    if (!read_ok) {
      return;
    }

    reconnect_backoff_.NoteHealthy();

    // Update metrics
    total_messages_pulled_.Increment();
    total_bytes_pulled_.Increment((double)message.ByteSizeLong());

    // Add the message to the queue
    m.id = IdFromString(message.correlation_uuid());
    m.data = message.SerializeAsString();
    m.valid = true;

    {
      std::scoped_lock<std::mutex> lock(message_queue_lock_);
      message_queue_.push(m);

      num_messages_ready_ = (unsigned int)message_queue_.size();
    }
    m.times.queued = clock::now();

    // Update metrics
    current_messages_queued_.Increment();
    current_bytes_queued_.Increment((double)m.data.length());

    // Store metrics object
    {
      std::scoped_lock<std::mutex> message_times_lock(message_times_lock_);
      message_times_.insert(std::make_pair(m.id, m.times));
    }
  }

  return;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
unsigned long long Broker<RequestType, ResponseType, ServiceType>::IdFromString(const std::string& str) {
  unsigned long long result = 0;

  std::scoped_lock<std::mutex> lock(id_lock_);

  auto iter = id_map_.find(str);
  if (iter != id_map_.end()) {
    result = iter->second;
  } else {
    result = next_id_;
    next_id_++;
  }

  return result;
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::CommitMetrics() {
  std::scoped_lock<std::mutex> lock(metrics_lock_);

  // Receive

  total_messages_consumed_.Commit();

  current_messages_queued_.Commit();
  current_bytes_queued_.Commit();

  time_in_receive_queue_.Commit();
  total_time_to_completion_.Commit();

  // Send

  attempted_messages_published_.Commit();
  attempted_bytes_published_.Commit();
  messages_published_success_.Commit();
  bytes_published_success_.Commit();

  messages_rejected_.Commit();
  total_messages_seen_.Commit();
  total_bytes_seen_.Commit();
  messages_queued_.Commit();
  bytes_queued_.Commit();
  queue_utilization_percent_.Commit();

  time_in_send_queue_.Commit();
  time_sending_.Commit();
  total_time_to_delivery_.Commit();

  // State
  broker_state_unknown_.Commit();
  broker_state_connecting_.Commit();
  broker_state_active_.Commit();
  broker_state_shut_down_.Commit();
  broker_state_auth_rejected_.Commit();
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::UpdateQueueMetrics() {
  messages_queued_.Set(pending_messages_);
  bytes_queued_.Set((double)queue_size_bytes_);
  if (max_queue_size_bytes_ > 0) {
    queue_utilization_percent_.Set((double)queue_size_bytes_ / (double)max_queue_size_bytes_);
  } else {
    queue_utilization_percent_.Set(0.0);
  }
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::UpdateTimingMetrics(UnpackedMessage* message) {
  time_in_send_queue_.Observe((double
  )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_retrieved - message->time_received)
                                .count());
  time_sending_.Observe((double
  )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_delivered - message->time_retrieved)
                          .count());
  total_time_to_delivery_.Observe((double
  )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_delivered - message->time_received)
                                    .count());
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::UpdateTimingMetrics(unsigned long long id) {
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

  time_in_receive_queue_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(times.retrieved - times.pulled).count()
  );
  total_time_to_completion_.Observe(
    (double)std::chrono::duration_cast<std::chrono::milliseconds>(time_completed - times.pulled).count()
  );
}

template<typename RequestType, typename ResponseType, typename ServiceType>
void Broker<RequestType, ResponseType, ServiceType>::UpdateBrokerStateMetrics(ClientState state) {
  std::scoped_lock<std::mutex> lock(metrics_lock_);

  if (state == ClientState::kUnknown) {
    broker_state_unknown_.Set((double)1);
  } else {
    broker_state_unknown_.Set((double)0);
  }

  if (state == ClientState::kConnecting) {
    broker_state_connecting_.Set((double)1);
  } else {
    broker_state_connecting_.Set((double)0);
  }

  if (state == ClientState::kActive) {
    broker_state_active_.Set((double)1);
  } else {
    broker_state_active_.Set((double)0);
  }

  if (state == ClientState::kShutDown) {
    broker_state_shut_down_.Set((double)1);
  } else {
    broker_state_shut_down_.Set((double)0);
  }

  if (state == ClientState::kAuthRejected) {
    broker_state_auth_rejected_.Set((double)1);
  } else {
    broker_state_auth_rejected_.Set((double)0);
  }
}
