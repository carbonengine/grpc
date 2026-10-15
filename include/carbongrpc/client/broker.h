// Copyright © 2025 CCP ehf.
#ifndef BROKER_H
#define BROKER_H

// std
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <queue>
#include <vector>

// grpc
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

// protobuf
#include <google/protobuf/message.h>

// monolith_grpc components
#include "carbongrpc/client/cached_metrics.h"
#include "carbongrpc/client/connection.h"
#include "carbongrpc/client/constants.h"
#include "carbongrpc/client/jobs.h"
#include "carbongrpc/client/reconnect_backoff.h"
#include "carbongrpc/client/stream_status_log.h"

// prometheus
namespace prometheus_module {
class MetricRegistryInterface;
class CounterInterface;
class GaugeInterface;
class HistogramInterface;
}  // namespace prometheus_module

namespace monolith_grpc {
namespace client {

template<typename RequestType, typename ResponseType, typename ServiceType>
class Broker {
public:

  Broker();
  virtual ~Broker() = default;

  virtual void Initialize();  ///< call immediately after the constructor
  virtual void Shutdown();    ///< call immediately before the destructor

  void SetConnection(std::shared_ptr<Connection> connection);

  using clock = std::chrono::steady_clock;
  void Ping();
  [[nodiscard]] clock::duration PingResult();
  [[nodiscard]] bool PingInProgress();

  [[nodiscard]] std::list<StreamStatus> GetStatusLog();

  // Sending

  struct UnpackedMessage {
    explicit UnpackedMessage(
      RequestType* in_message, void* in_py_message = nullptr, google::protobuf::Message* in_payload = nullptr,
      void* in_py_payload = nullptr, unsigned long long id = 0
    );
    RequestType* message;
    google::protobuf::Message* payload;
    void* py_message;
    void* py_payload;

    unsigned long long id;

    clock::time_point time_received;
    clock::time_point time_retrieved;
    clock::time_point time_delivered;
  };

  [[nodiscard]] PublishResult SendRequest(UnpackedMessage* message);
  void WaitUntilPublishesComplete();
  void set_on_message_published(std::function<void(UnpackedMessage*)> callback);
  void set_max_message_size_bytes(size_t max_bytes);  ///< Values <=0 mean "unlimited"
  void set_max_queue_size_bytes(size_t max_bytes);    ///< Values <=0 mean "unlimited"
  [[nodiscard]] unsigned int num_pending_messages() const;

  void PauseSending();
  void ResumeSending();

  // Receiving

  struct MessageTiming {
    MessageTiming() {
      auto now = clock::now();
      pulled = now;
      queued = now;
      retrieved = now;
    }

    clock::time_point pulled;
    clock::time_point queued;
    clock::time_point retrieved;
  };

  struct ReceivedMessage {
    bool valid;
    unsigned long long id;
    std::string data;
    MessageTiming times;

    ReceivedMessage()
      : valid(false),
        id(0) {
    }
  };
  [[nodiscard]] ReceivedMessage GetResponse();
  [[nodiscard]] unsigned int num_messages_ready() const;

  virtual void set_metric_registry(
    prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
    const std::map<std::string, std::string>& labels, AsyncMode async
  );

  [[nodiscard]] grpc_connectivity_state channel_state();
  [[nodiscard]] int active_connection_id();
  [[nodiscard]] unsigned int active_stream_id() const;

  [[nodiscard]] ClientState broker_state() const;

  /// When enabled, a stream ending with UNAUTHENTICATED parks the broker in
  /// ClientState::kAuthRejected instead of reconnecting: retrying with the
  /// same credentials can never succeed. The broker re-arms when new
  /// credentials arrive (Metadata::set_auth_token) or on SetConnection.
  /// Default off to preserve existing behavior for current consumers.
  void set_auth_stop_enabled(bool enabled);
  [[nodiscard]] bool auth_stop_enabled() const;

protected:

  void set_broker_state(ClientState state);

  void Async(const std::function<void()>& lambda);
  Jobs jobs_;

  std::shared_ptr<Connection> connection_;
  void WithConnection(const std::function<void(Connection&)>& f);
  std::shared_ptr<monolith_grpc::client::Channel<Connection::ConnectionMessage>> connection_listener_;
  std::atomic<bool> request_disconnect_;
  std::mutex disconnect_lock_;
  void ConnectionMonitor();

  // Broker thread
  void BrokerThread();
  std::thread broker_thread_;
  std::unique_ptr<grpc::ClientContext> context_;
  std::mutex context_lock_;
  std::shared_ptr<grpc::Channel> channel_;
  ClientState broker_state_{ClientState::kUnknown};
  std::atomic<bool> shutting_down_;
  std::queue<std::function<bool()>> operations_;

  // Sending

  void PublishMessages();
  std::deque<UnpackedMessage*> queued_messages_;
  std::mutex queue_lock_;
  std::condition_variable queue_condition_;
  std::atomic<int> pending_messages_;
  std::atomic<bool> paused_;
  std::function<void(UnpackedMessage*)> on_message_published_;
  std::mutex callback_lock_;

  [[nodiscard]] bool WriteMessageToStream(const RequestType& message);
  void WritesDone();

  [[nodiscard]] UnpackedMessage* GetNextMessage();
  [[nodiscard]] UnpackedMessage* PeekNextMessage(std::unique_lock<std::mutex>& queue_lock);
  unsigned long long next_assignable_id_;
  std::atomic<bool> writer_exit_requested_{false};

  size_t max_message_size_bytes_;

  size_t max_queue_size_bytes_;
  size_t queue_size_bytes_;

  // Receiving

  // PullMessages, ConsumeMessages
  std::condition_variable request_condition_;
  std::mutex request_lock_;

  // Message queue
  std::queue<ReceivedMessage> message_queue_;
  std::mutex message_queue_lock_;
  std::atomic<unsigned int> num_messages_ready_;
  unsigned int num_bytes_ready_;

  // Listener
  std::atomic<bool> reader_exit_requested_{false};
  void ReadMessages();

  // Reconnect policy
  ReconnectBackoff reconnect_backoff_;
  std::atomic<bool> auth_stop_enabled_{false};
  std::atomic<bool> auth_rejected_{false};
  std::atomic<int> last_finish_code_{-1};
  void RecordFinishStatus(const grpc::Status& status);
  /// Parks until new credentials/connection/shutdown, applies backoff
  /// otherwise. Call between reconnect attempts.
  void WaitBeforeReconnect();

  // Stream type interface
  [[nodiscard]] bool PrepareStream(grpc::ClientContext* context);
  void FinishStream();
  void ReadMessagesInternal();
  std::unique_ptr<typename ServiceType::Stub> stub_;
  std::unique_ptr<grpc::ClientReaderWriter<RequestType, ResponseType>> stream_;
  std::mutex stream_write_lock_;
  std::mutex stream_read_lock_;

  unsigned long long next_id_{0};
  std::map<std::string, unsigned long long> id_map_;
  std::mutex id_lock_;
  [[nodiscard]] unsigned long long IdFromString(const std::string& str);

  // Ping
  bool pings_enabled_;
  clock::duration ping_result_;
  std::atomic<bool> ping_in_progress_;
  void PingThread();
  [[nodiscard]] virtual bool SendPing() = 0;

  // Stream status
  unsigned int stream_id_;
  StreamStatusLog stream_status_log_;

  // Metrics
  [[nodiscard]] virtual std::string metrics_message() = 0;
  [[nodiscard]] virtual std::string metrics_domain() = 0;

  prometheus_module::MetricRegistryInterface* metric_registry_;
  std::string metric_prefix_;
  std::map<std::string, std::string> metric_labels_;
  std::mutex metrics_lock_;
  std::thread metrics_thread_;
  void CommitMetrics();

  // Publishing metrics
  CachedCounter attempted_messages_published_{nullptr};
  CachedCounter attempted_bytes_published_{nullptr};
  CachedCounter messages_published_success_{nullptr};
  CachedCounter bytes_published_success_{nullptr};

  CachedCounter messages_rejected_{nullptr};
  CachedCounter total_messages_seen_{nullptr};
  CachedCounter total_bytes_seen_{nullptr};
  CachedGauge messages_queued_{nullptr};
  CachedGauge bytes_queued_{nullptr};
  CachedGauge queue_utilization_percent_{nullptr};
  void UpdateQueueMetrics();

  CachedHistogram time_in_send_queue_{nullptr};
  CachedHistogram time_sending_{nullptr};
  CachedHistogram total_time_to_delivery_{nullptr};
  void UpdateTimingMetrics(UnpackedMessage* message);

  // Consuming metrics

  CachedCounter total_messages_pulled_{nullptr};
  CachedCounter total_bytes_pulled_{nullptr};
  CachedCounter total_messages_consumed_{nullptr};

  CachedGauge current_messages_queued_{nullptr};
  CachedGauge current_bytes_queued_{nullptr};

  std::map<unsigned long long, MessageTiming> message_times_;
  std::mutex message_times_lock_;
  void UpdateTimingMetrics(unsigned long long id);

  CachedHistogram time_in_receive_queue_{nullptr};
  CachedHistogram total_time_to_completion_{nullptr};

  // Broker state metrics

  CachedGauge broker_state_unknown_{nullptr};
  CachedGauge broker_state_connecting_{nullptr};
  CachedGauge broker_state_active_{nullptr};
  CachedGauge broker_state_shut_down_{nullptr};
  CachedGauge broker_state_auth_rejected_{nullptr};
  void UpdateBrokerStateMetrics(ClientState state);
};

}  // namespace client
}  // namespace monolith_grpc

#endif
