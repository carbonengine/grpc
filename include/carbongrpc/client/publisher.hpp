// Copyright © 2025 CCP ehf.
#include "publisher.h"
using namespace monolith_grpc;
using namespace monolith_grpc::client;

// std
#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// client/common
#include "carbongrpc/client/metadata.h"
#include "carbongrpc/client/stream_counter.h"

// prometheus
#include "counter_interface.h"
#include "gauge_interface.h"
#include "histogram_interface.h"
#include "metric_registry_interface.h"
#include "summary_interface.h"

template<typename MessageType>
Publisher<MessageType>::UnpackedMessage::UnpackedMessage(
  MessageType* in_message, void* in_py_message, google::protobuf::Message* in_payload, void* in_py_payload,
  unsigned long long in_id
)
  : message(in_message),
    py_message(in_py_message),
    payload(in_payload),
    py_payload(in_py_payload),
    id(in_id),
    time_received(std::chrono::steady_clock::now()) {
}

template<typename MessageType>
Publisher<MessageType>::Publisher()
  : request_disconnect_(false),
    connection_listener_(std::make_shared<monolith_grpc::client::Channel<Connection::ConnectionMessage>>()),

    max_message_size_bytes_(4194304),                       //< 4 MiB, https://github.com/grpc/grpc-go/issues/746
    max_queue_size_bytes_(1024ll * 1024ll * 1024ll * 2ll),  //< 2 GiB

    queue_size_bytes_(0),

    compression_algorithm_(-1),

    last_confirmed_message_(0),
    next_message_to_send_(1),
    next_assignable_id_(1),

    stream_id_(0),

    metric_registry_(nullptr),

    pings_enabled_(false),
    ping_in_progress_(false),
    ping_result_(clock::duration(std::chrono::milliseconds(-1))),

    pending_messages_(0),
    paused_(false),
    shutting_down_(false),
    confirms_enabled_(false),
    empty_bidi_enabled_(false) {
  set_publisher_state(ClientState::kUnknown);
}

template<typename MessageType>
void Publisher<MessageType>::Initialize() {
  publisher_thread_ = std::thread([this] { this->PublisherThread(); });
  metrics_thread_ = std::thread([this] {
    while (this->shutting_down_ == false) {
      this->CommitMetrics();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
}

template<typename MessageType>
void Publisher<MessageType>::Shutdown() {
  shutting_down_ = true;

  {
    std::scoped_lock<std::mutex> queue_lock(queue_lock_);
    request_disconnect_ = true;
    queue_condition_.notify_all();
  }

  if (publisher_thread_.joinable()) {
    publisher_thread_.join();
  }

  if (metrics_thread_.joinable()) {
    metrics_thread_.join();
  }

  jobs_.Wait();
}

template<typename MessageType>
void Publisher<MessageType>::SetConnection(std::shared_ptr<Connection> connection) {
  Async([=] {
    connection_.reset();

    // Cancel the context to end any ongoing rpcs
    {
      std::scoped_lock<std::mutex, std::mutex> locks(queue_lock_, context_lock_);

      request_disconnect_ = true;
      queue_condition_.notify_all();

      if (context_) {
        context_->TryCancel();
      }
    }

    // Wait for the publisher thread to reset
    while (publisher_state() == ClientState::kActive) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    };

    // Now we can set the new connection and resume the publisher thread
    auth_rejected_ = false;
    request_disconnect_ = false;
    connection_ = connection;
    if (connection) {
      connection_listener_ = connection->GetListener();
    }
  });
}

template<typename MessageType>
PublishResult Publisher<MessageType>::PublishMessage(UnpackedMessage* message) {
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

template<typename MessageType>
void Publisher<MessageType>::WaitUntilPublishesComplete() {
  if (!publisher_thread_.joinable()) {
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

template<typename MessageType>
void Publisher<MessageType>::set_on_message_published(std::function<void(UnpackedMessage*)> callback) {
  std::scoped_lock<std::mutex> lock(callback_lock_);
  on_message_published_ = callback;
}

template<typename MessageType>
unsigned int Publisher<MessageType>::num_pending_messages() const {
  return pending_messages_;
}

template<typename MessageType>
void Publisher<MessageType>::PauseSending() {
  std::scoped_lock<std::mutex> lock(queue_lock_);
  paused_ = true;
  queue_condition_.notify_all();
}

template<typename MessageType>
void Publisher<MessageType>::ResumeSending() {
  std::scoped_lock<std::mutex> lock(queue_lock_);
  paused_ = false;
  queue_condition_.notify_all();
}

template<typename MessageType>
void Publisher<MessageType>::Ping() {
  if (!pings_enabled_) {
    return;
  }

  ping_in_progress_ = true;
}

template<typename MessageType>
Publisher<MessageType>::clock::duration Publisher<MessageType>::PingResult() {
  return ping_result_;
}

template<typename MessageType>
bool Publisher<MessageType>::PingInProgress() {
  return ping_in_progress_;
}

template<typename MessageType>
std::list<StreamStatus> Publisher<MessageType>::GetStatusLog() {
  return stream_status_log_.GetStatusLog();
}

template<typename MessageType>
void Publisher<MessageType>::PingThread() {
  while (!shutting_down_ && !request_disconnect_ && !writer_exited_) {
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

template<typename MessageType>
void Publisher<MessageType>::set_metric_registry(
  prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
  const std::map<std::string, std::string>& labels, AsyncMode async
) {
  auto lambda = [=] {
    std::unique_lock<std::mutex> lock(metrics_lock_);

    MetricsCreator metrics_creator;
    metrics_creator.set_registry(registry);
    metrics_creator.set_prefix(prefix);
    metrics_creator.set_labels(labels);

    if (registry == nullptr) {
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

      time_in_queue_ = CachedHistogram();
      time_sending_ = CachedHistogram();
      time_confirming_ = CachedHistogram();
      total_time_to_delivery_ = CachedHistogram();
      total_time_to_confirmation_ = CachedHistogram();
    } else {
      metric_registry_ = registry;

      static std::string component_key = "component";
      static std::string component_value = "publisher";

      metrics_creator.assign_label("pb_domain", metrics_domain());
      metrics_creator.assign_label("pb_message", metrics_message());

      // Publishing

      attempted_messages_published_ = metrics_creator.MakeCounter("attempted_messages_published");
      attempted_bytes_published_ = metrics_creator.MakeCounter("attempted_bytes_published");
      messages_published_success_ = metrics_creator.MakeCounter("messages_published_success");
      bytes_published_success_ = metrics_creator.MakeCounter("bytes_published_success");

      // Queue

      messages_rejected_ = metrics_creator.MakeCounter("messages_rejected");
      total_messages_seen_ = metrics_creator.MakeCounter("total_messages_seen");
      total_bytes_seen_ = metrics_creator.MakeCounter("total_bytes_seen");

      metrics_creator.assign_label(component_key, component_value);
      messages_queued_ = metrics_creator.MakeGauge("messages_queued");
      bytes_queued_ = metrics_creator.MakeGauge("bytes_queued");
      metrics_creator.remove_label(component_key);  // https://github.com/ccpgames/eve-monolith-grpc/issues/251

      queue_utilization_percent_ = metrics_creator.MakeGauge("queue_utilization_percent");

      // Latency

      std::vector<double> boundaries = {1, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000};

      metrics_creator.assign_label(component_key, component_value);
      time_in_queue_ = metrics_creator.MakeHistogram("time_in_queue_ms", boundaries);
      metrics_creator.remove_label(component_key);  // https://github.com/ccpgames/eve-monolith-grpc/issues/251

      time_sending_ = metrics_creator.MakeHistogram("time_sending_ms", boundaries);
      total_time_to_delivery_ = metrics_creator.MakeHistogram("total_time_to_delivery_ms", boundaries);

      if (confirms_enabled_) {
        time_confirming_ = metrics_creator.MakeHistogram("time_confirming_ms", boundaries);
        total_time_to_confirmation_ = metrics_creator.MakeHistogram("total_time_to_confirmation_ms", boundaries);
      }

      // Publisher state

      const std::string state_metric = "publisher_state";
      const std::string state_key = "state";

      metrics_creator.assign_label(state_key, "unknown");
      publisher_state_unknown_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "connecting");
      publisher_state_connecting_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "active");
      publisher_state_active_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "shut_down");
      publisher_state_shut_down_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.assign_label(state_key, "auth_rejected");
      publisher_state_auth_rejected_ = metrics_creator.MakeGauge(state_metric);

      metrics_creator.remove_label(state_key);

      lock.unlock();
      UpdatePublisherStateMetrics(publisher_state_);
    }
  };

  if (async == AsyncMode::kAsync) {
    Async(lambda);
  } else {
    lambda();
  }
}

template<typename MessageType>
void Publisher<MessageType>::set_max_message_size_bytes(size_t max_bytes) {
  max_message_size_bytes_ = max_bytes;
}

template<typename MessageType>
void Publisher<MessageType>::set_max_queue_size_bytes(size_t max_bytes) {
  max_queue_size_bytes_ = max_bytes;
  UpdateQueueMetrics();
}

template<typename MessageType>
size_t Publisher<MessageType>::max_queue_size_bytes() const {
  return max_queue_size_bytes_;
}

template<typename MessageType>
void Publisher<MessageType>::set_compression_algorithm(int algorithm) {
  if (algorithm >= grpc_compression_algorithm::GRPC_COMPRESS_ALGORITHMS_COUNT) {
    return;
  }

  compression_algorithm_ = algorithm;
}

template<typename MessageType>
int Publisher<MessageType>::compression_algorithm() const {
  return compression_algorithm_;
}

template<typename MessageType>
grpc_connectivity_state Publisher<MessageType>::channel_state() {
  grpc_connectivity_state result = grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN;
  WithConnection([&result](Connection& connection) { result = connection.channel_state(); });
  return result;
}

template<typename MessageType>
int Publisher<MessageType>::active_connection_id() {
  int result = -1;
  WithConnection([&result](Connection& connection) { result = connection.id(); });
  return result;
}

template<typename MessageType>
unsigned int Publisher<MessageType>::active_stream_id() const {
  return stream_id_;
}

template<typename MessageType>
void Publisher<MessageType>::set_publisher_state(ClientState state) {
  if (publisher_state_ == state) {
    return;
  }

  static std::map<ClientState, const char*> states = {
    {ClientState::kUnknown, "Unknown"},
    {ClientState::kConnecting, "Connecting"},
    {ClientState::kActive, "Active"},
    {ClientState::kShutDown, "Shutdown"},
    {ClientState::kAuthRejected, "AuthRejected"},
  };
  // printf("publisher state %s -> %s\n", states[publisher_state_],
  // states[state]);

  publisher_state_ = state;

  UpdatePublisherStateMetrics(state);
}

template<typename MessageType>
ClientState Publisher<MessageType>::publisher_state() const {
  return publisher_state_;
}

template<typename MessageType>
void Publisher<MessageType>::set_auth_stop_enabled(bool enabled) {
  auth_stop_enabled_ = enabled;
}

template<typename MessageType>
bool Publisher<MessageType>::auth_stop_enabled() const {
  return auth_stop_enabled_;
}

template<typename MessageType>
void Publisher<MessageType>::RecordFinishStatus(const grpc::Status& status) {
  last_finish_code_ = (int)status.error_code();
  stream_status_log_.RecordStatus(stream_id_, status);
}

template<typename MessageType>
void Publisher<MessageType>::WaitBeforeReconnect() {
  reconnect_backoff_.NoteStreamEnd();

  if (shutting_down_ || request_disconnect_) {
    return;
  }

  if (auth_stop_enabled_ && last_finish_code_ == (int)grpc::StatusCode::UNAUTHENTICATED) {
    // Retrying with the same credentials can never succeed; park until new
    // credentials arrive, the connection is replaced, or we shut down.
    auth_rejected_ = true;
    set_publisher_state(ClientState::kAuthRejected);
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
void Publisher<MessageType>::WaitForConnection() {
  std::shared_ptr<Connection> connection = connection_;
  if (connection && connection->ready()) {
    return;
  }

  set_publisher_state(ClientState::kConnecting);
  bool ready = false;
  while (!ready) {
    // Don't burn CPU while waiting for a connection
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    if (shutting_down_ || request_disconnect_) {
      break;
    }

    connection = connection_;
    if (!connection) {
      continue;
    }

    connection->UpdateChannelState();
    ready = connection->ready();
  }
}

template<typename MessageType>
bool Publisher<MessageType>::SetUpStream() {
  std::scoped_lock<std::mutex> context_lock(context_lock_);

  context_ = std::make_unique<grpc::ClientContext>();
  monolith_grpc::client::Metadata::ApplyToContext(*context_);

  if (compression_algorithm_ >= 0 && compression_algorithm_ < grpc_compression_algorithm::GRPC_COMPRESS_ALGORITHMS_COUNT) {
    context_->set_compression_algorithm((grpc_compression_algorithm)compression_algorithm_);
  }

  if (!PrepareStream(context_.get())) {
    return false;
  }

  stream_id_ = StreamCounter::AssignStreamId();

  last_confirmed_message_ = 0;
  next_message_to_send_ = 1;

  return true;
}

template<typename MessageType>
void Publisher<MessageType>::ProcessMessages() {
  last_finish_code_ = -1;
  reconnect_backoff_.NoteStreamStart();
  set_publisher_state(ClientState::kActive);

  // Set up readers and writers
  writer_exited_ = false;
  std::future<void> writer = std::async([=] { PublishMessages(); });

  reader_exited_ = false;
  std::future<void> reader;
  if (confirms_enabled_) {
    reader = std::async([=] { ReadConfirms(); });
  } else if (empty_bidi_enabled_) {
    reader = std::async([=] { ReadEmpties(); });
  }

  std::future<void> ping_thread;
  if (pings_enabled_) {
    ping_thread = std::async([=] { PingThread(); });
  }

  // Wait for processing to complete
  while (!writer_exited_) {
    WithConnection([=](Connection& connection) { connection.UpdateChannelState(); });

    if (confirms_enabled_ || empty_bidi_enabled_) {
      if (reader.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
        reader_exited_ = true;

        if (!writer_exited_) {
          // Wake up the writer thread so it can exit as well
          queue_condition_.notify_all();
        }
      }
    }

    if (writer.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
      writer_exited_ = true;
    }
  }

  // Clean up
  writer.get();
  if (confirms_enabled_ || empty_bidi_enabled_) {
    reader.get();
  }
  FinishStream();

  if (pings_enabled_) {
    ping_thread.get();
  }
}

template<typename MessageType>
void Publisher<MessageType>::PublisherThread() {
  auto connection_monitor = std::async(std::launch::async, [=] { ConnectionMonitor(); });
  while (!shutting_down_) {
    WaitForConnection();

    // WaitForConnection can exit on shutdown or disconnect or connection-ok
    std::shared_ptr<Connection> connection = connection_;
    if (!connection || !connection->ready()) {
      // request_disconnect_ can cause busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    // Get a reference to the channel
    // This is a shared_ptr, so the channel will remain valid as long as we hold
    // this reference
    channel_ = connection->channel();
    if (channel_ == nullptr) {
      continue;
    }

    if (!SetUpStream()) {
      WaitBeforeReconnect();
      continue;
    }

    // Each new stream starts at message id 1, so we need to re-number any
    // existing messages
    {
      std::scoped_lock<std::mutex> lock(queue_lock_);
      unsigned long long id = 1;
      for (auto m : queued_messages_) {
        m->id = id;
        id++;
      }
      next_assignable_id_ = id;
    }

    ProcessMessages();

    channel_ = nullptr;

    WaitBeforeReconnect();
  }

  // todo: cleanup remaining messages

  connection_monitor.get();
  set_publisher_state(ClientState::kShutDown);
}

template<typename MessageType>
void Publisher<MessageType>::PublishMessages() {
  while (true) {
    UnpackedMessage* unpacked = GetNextMessage();

    if (shutting_down_) {
      break;
    }

    if (request_disconnect_) {
      break;
    }

    if (reader_exited_) {
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
      reconnect_backoff_.NoteHealthy();
      messages_published_success_.Increment();
      bytes_published_success_.Increment((double)message_size);

      if (!confirms_enabled_ /* todo: empty_bidi_enabled_ here? */) {
        {
          std::scoped_lock<std::mutex> queue_lock(queue_lock_);

          queued_messages_.pop_front();
          pending_messages_ = (int)queued_messages_.size();
          queue_size_bytes_ = 0;
          for (UnpackedMessage* message : queued_messages_) {
            queue_size_bytes_ += message->message->ByteSizeLong();
          }
        }

        auto now = clock::now();
        unpacked->time_delivered = now;

        std::scoped_lock<std::mutex> callback_lock(callback_lock_);
        if (on_message_published_ != nullptr) {
          on_message_published_(unpacked);
        }

        queue_condition_.notify_all();

        UpdateQueueMetrics();
        UpdateTimingMetrics(unpacked);
      }
    } else {
      break;  // write failure means the stream is dead, so exit the loop and
              // restart the stream
    }
  }

  WritesDone();
}

template<typename MessageType>
void Publisher<MessageType>::ReadConfirms() {
  while (true) {
    if (shutting_down_) {
      break;
    }

    if (writer_exited_) {
      break;
    }

    Confirmation confirm = ReadConfirm();
    if (confirm.read_ok == false) {
      break;
    }

    MessageConfirmed(confirm.message_id);
  }
}

template<typename MessageType>
void Publisher<MessageType>::ReadEmpties() {
  while (true) {
    if (shutting_down_) {
      break;
    }

    if (writer_exited_) {
      break;
    }

    bool result = ReadEmpty();
    if (!result) {
      break;
    }
  }
}

template<typename MessageType>
typename Publisher<MessageType>::UnpackedMessage* Publisher<MessageType>::GetNextMessage() {
  std::unique_lock<std::mutex> queue_lock(queue_lock_);
  Publisher<MessageType>::UnpackedMessage* result = PeekNextMessage(queue_lock);

  while (result == nullptr && !shutting_down_ && !request_disconnect_ && !reader_exited_) {
    queue_condition_.wait(queue_lock);
    result = PeekNextMessage(queue_lock);
  }

  return result;
}

template<typename MessageType>
typename Publisher<MessageType>::UnpackedMessage* Publisher<MessageType>::PeekNextMessage(
  std::unique_lock<std::mutex>& queue_lock
) {
  if (paused_) {
    return nullptr;
  }

  if (queued_messages_.empty()) {
    return nullptr;
  }

  Publisher<MessageType>::UnpackedMessage* first_message = *queued_messages_.begin();
  long long offset = next_message_to_send_ - first_message->id;
  if (offset < 0 || offset >= (long long)queued_messages_.size() || shutting_down_) {
    return nullptr;
  }

  auto iter = queued_messages_.begin() + offset;
  auto result = *iter;

  next_message_to_send_++;

  return result;
}

template<typename MessageType>
void Publisher<MessageType>::MessageConfirmed(unsigned long long id) {
  if (id <= (unsigned long long)last_confirmed_message_) {
    return;
  }

  std::deque<Publisher<MessageType>::UnpackedMessage*> finished;

  {
    std::scoped_lock<std::mutex> queue_lock(queue_lock_);

    if (queued_messages_.empty()) {
      return;
    }

    auto iter = queued_messages_.begin();
    while (iter != queued_messages_.end() && (*iter) != nullptr && (*iter)->id <= id) {
      finished.push_back(*iter);
      ++iter;
    }

    queued_messages_.erase(queued_messages_.begin(), iter);

    pending_messages_ = (int)queued_messages_.size();
    queue_size_bytes_ = 0;
    for (UnpackedMessage* message : queued_messages_) {
      queue_size_bytes_ += message->message->ByteSizeLong();
    }
  }

  auto now = clock::now();
  for (auto m : finished) {
    m->time_confirmed = now;
  }

  std::scoped_lock<std::mutex> callback_lock(callback_lock_);
  if (on_message_published_ != nullptr) {
    for (auto m : finished) {
      on_message_published_(m);
    }
  }

  last_confirmed_message_ = id;

  queue_condition_.notify_all();

  UpdateQueueMetrics();
  for (auto message : finished) {
    UpdateTimingMetrics(message);
  }
}

template<typename MessageType>
void Publisher<MessageType>::Async(const std::function<void()>& lambda) {
  if (shutting_down_) {
    return;
  }

  jobs_.Run(lambda);
}

template<typename MessageType>
void Publisher<MessageType>::WithConnection(const std::function<void(Connection&)>& f) {
  std::shared_ptr<Connection> connection(connection_);
  if (connection) {
    f(*connection);
  }
}

template<typename MessageType>
void Publisher<MessageType>::ConnectionMonitor() {
  while (!shutting_down_) {
    if (connection_listener_ && !connection_listener_->closed()) {
      Connection::ConnectionMessage message;
      if (connection_listener_->Read(message)) {
        if (message == Connection::ConnectionMessage::kConnected) {
          auth_rejected_ = false;
          request_disconnect_ = false;
        } else if (message == Connection::ConnectionMessage::kDisconnected) {
          std::scoped_lock<std::mutex> queue_lock(queue_lock_);
          request_disconnect_ = true;
          queue_condition_.notify_all();
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

template<typename MessageType>
void Publisher<MessageType>::CommitMetrics() {
  std::scoped_lock<std::mutex> lock(metrics_lock_);

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

  time_in_queue_.Commit();
  time_sending_.Commit();
  time_confirming_.Commit();
  total_time_to_delivery_.Commit();
  total_time_to_confirmation_.Commit();

  publisher_state_unknown_.Commit();
  publisher_state_connecting_.Commit();
  publisher_state_active_.Commit();
  publisher_state_shut_down_.Commit();
  publisher_state_auth_rejected_.Commit();
}

template<typename MessageType>
void Publisher<MessageType>::UpdateQueueMetrics() {
  messages_queued_.Set(pending_messages_);
  bytes_queued_.Set((double)queue_size_bytes_);
  if (max_queue_size_bytes_ > 0) {
    queue_utilization_percent_.Set((double)queue_size_bytes_ / (double)max_queue_size_bytes_);
  } else {
    queue_utilization_percent_.Set(0.0);
  }
}

template<typename MessageType>
void Publisher<MessageType>::UpdateTimingMetrics(UnpackedMessage* message) {
  time_in_queue_.Observe((double
  )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_retrieved - message->time_received)
                           .count());
  time_sending_.Observe((double
  )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_delivered - message->time_retrieved)
                          .count());

  if (confirms_enabled_) {
    time_confirming_.Observe((double
    )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_confirmed - message->time_delivered)
                               .count());
    total_time_to_delivery_.Observe((double
    )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_delivered - message->time_received)
                                      .count());
    total_time_to_confirmation_.Observe((double
    )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_confirmed - message->time_received)
                                          .count());
  } else {
    total_time_to_delivery_.Observe((double
    )std::chrono::duration_cast<std::chrono::milliseconds>(message->time_delivered - message->time_received)
                                      .count());
  }
}

template<typename MessageType>
void Publisher<MessageType>::UpdatePublisherStateMetrics(ClientState state) {
  std::scoped_lock<std::mutex> lock(metrics_lock_);
  if (state == ClientState::kUnknown) {
    publisher_state_unknown_.Set((double)1);
  } else {
    publisher_state_unknown_.Set((double)0);
  }

  if (state == ClientState::kConnecting) {
    publisher_state_connecting_.Set((double)1);
  } else {
    publisher_state_connecting_.Set((double)0);
  }

  if (state == ClientState::kActive) {
    publisher_state_active_.Set((double)1);
  } else {
    publisher_state_active_.Set((double)0);
  }

  if (state == ClientState::kShutDown) {
    publisher_state_shut_down_.Set((double)1);
  } else {
    publisher_state_shut_down_.Set((double)0);
  }

  if (state == ClientState::kAuthRejected) {
    publisher_state_auth_rejected_.Set((double)1);
  } else {
    publisher_state_auth_rejected_.Set((double)0);
  }
}
