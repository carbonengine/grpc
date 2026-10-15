// Copyright © 2025 CCP ehf.
#ifndef CONSUMER_H
#define CONSUMER_H

// std
#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

// grpc
#include <grpcpp/grpcpp.h>

// monolith_grpc components
#include "carbongrpc/client/cached_metrics.h"
#include "carbongrpc/client/connection.h"
#include "carbongrpc/client/constants.h"
#include "carbongrpc/client/jobs.h"
#include "carbongrpc/client/reconnect_backoff.h"
#include "carbongrpc/client/stream_status_log.h"

// prometheus
namespace prometheus_module {
class GaugeInterface;
}

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

namespace monolith_grpc {
namespace client {

template<typename MessageType>
class Consumer {
public:

  Consumer();
  virtual ~Consumer() = default;

  void Initialize();  ///< call immediately after the constructor
  void Shutdown();    ///< call immediately before the destructor

  void SetConnection(std::shared_ptr<Connection> connection);

  using clock = std::chrono::steady_clock;

  struct MessageTiming {
    MessageTiming() {
      auto now = clock::now();
      pulled = now;
      queued = now;
      retrieved = now;
      processed = now;
    }

    clock::time_point pulled;
    clock::time_point queued;
    clock::time_point retrieved;
    clock::time_point processed;

    std::chrono::system_clock::time_point system_time_pulled;
  };

  struct ReceivedMessage {
    bool valid;
    unsigned long long id;
    unsigned int stream_id;
    std::string data;
    MessageTiming times;

    ReceivedMessage()
      : valid(false),
        id(0),
        stream_id(0) {
    }
  };
  [[nodiscard]] virtual bool Subscribe(::google::protobuf::Message* subscription_info) = 0;
  [[nodiscard]] virtual bool subscribed() const;
  [[nodiscard]] ReceivedMessage GetNextMessage();
  [[nodiscard]] PublishResult AcknowledgeMessage(
    unsigned long long id, unsigned int stream_id, bool positive, bool requeue, ::google::protobuf::Message* response,
    ::google::protobuf::Message* response_payload
  );
  [[nodiscard]] unsigned int num_messages_ready() const;
  void set_max_message_size_bytes(size_t max_bytes);  ///< Values <=0 mean "unlimited"

  [[nodiscard]] std::list<StreamStatus> GetStatusLog();

  virtual void set_metric_registry(
    prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
    const std::map<std::string, std::string>& labels, AsyncMode async
  );

  [[nodiscard]] grpc_connectivity_state channel_state();
  [[nodiscard]] int active_connection_id();
  [[nodiscard]] unsigned int active_stream_id() const;

  [[nodiscard]] ClientState consumer_state() const;

  /// When enabled, a stream ending with UNAUTHENTICATED parks the consumer in
  /// ClientState::kAuthRejected instead of reconnecting; re-arms on new
  /// credentials (Metadata::set_auth_token) or SetConnection. Default off.
  void set_auth_stop_enabled(bool enabled);
  [[nodiscard]] bool auth_stop_enabled() const;

protected:

  void set_consumer_state(ClientState state);

  void Async(const std::function<void()>& lambda);
  Jobs jobs_;

  std::shared_ptr<Connection> connection_;
  void WithConnection(const std::function<void(Connection&)>& f);
  std::shared_ptr<monolith_grpc::client::Channel<Connection::ConnectionMessage>> connection_listener_;
  std::atomic<bool> request_disconnect_;
  std::mutex disconnect_lock_;
  void ConnectionMonitor();

  // Consumer thread
  void ConsumerThread();
  std::thread consumer_thread_;
  std::unique_ptr<grpc::ClientContext> context_;
  std::mutex context_lock_;
  ClientState consumer_state_{ClientState::kUnknown};
  std::atomic<bool> shutting_down_{false};
  std::shared_ptr<grpc::Channel> channel_;
  void AddOperation(std::function<bool()> operation);
  std::queue<std::function<bool()>> operations_;
  std::atomic<bool> is_subscribed_{false};

  // PullMessages, ConsumeMessages
  std::condition_variable request_condition_;
  std::mutex request_lock_;

  // Message queue
  std::queue<ReceivedMessage> message_queue_;
  std::mutex message_queue_lock_;
  std::atomic<unsigned int> num_messages_ready_{0};
  unsigned int num_bytes_ready_{0};
  std::condition_variable message_queue_condition_;

  // Listener
  std::atomic<bool> reader_exit_requested_{};
  void ReadMessages();

  // Acknowledgement
  struct Acknowledgement {
    unsigned long long id;
    unsigned int stream_id;
    bool positive;
    bool requeue;
    ::google::protobuf::Message* response;
    ::google::protobuf::Message* response_payload;

    Acknowledgement() {
      id = 0;
      stream_id = 0;
      positive = false;
      requeue = false;
      response = nullptr;
      response_payload = nullptr;
    }
  };
  void SendAcknowledgements();
  bool acks_enabled_;
  std::mutex acks_lock_;
  std::atomic<bool> acks_exit_requested_{};
  std::vector<Acknowledgement> completed_messages_;
  size_t max_message_size_bytes_;

  // Stream status
  unsigned int stream_id_;
  StreamStatusLog stream_status_log_;

  // Reconnect policy
  ReconnectBackoff reconnect_backoff_;
  std::atomic<bool> auth_stop_enabled_{false};
  std::atomic<bool> auth_rejected_{false};
  std::atomic<int> last_finish_code_{-1};
  /// Child FinishStream() implementations must report the terminal stream
  /// status through this instead of writing to stream_status_log_ directly.
  void RecordFinishStatus(const grpc::Status& status);
  void WaitBeforeReconnect();

  // Child class interface
  [[nodiscard]] virtual std::string metrics_message() = 0;
  [[nodiscard]] virtual std::string metrics_domain() = 0;
  [[nodiscard]] virtual bool PrepareStream(grpc::ClientContext* context) = 0;
  virtual void FinishStream() = 0;
  virtual void ReadMessagesInternal() = 0;
  [[nodiscard]] virtual bool Acknowledge(std::vector<Acknowledgement>& acks) = 0;

  // Metrics
  prometheus_module::MetricRegistryInterface* metric_registry_;
  std::string metric_prefix_;
  std::map<std::string, std::string> metric_labels_;
  std::mutex metrics_lock_;
  std::thread metrics_thread_;
  void CommitMetrics();

  CachedCounter total_messages_pulled_;
  CachedCounter total_bytes_pulled_;
  CachedCounter total_messages_consumed_;
  CachedCounter total_messages_acknowledged_;
  CachedCounter total_messages_dropped_;
  CachedCounter total_acks_dropped_;

  CachedGauge current_messages_queued_;
  CachedGauge current_bytes_queued_;
  CachedGauge current_messages_processing_;

  std::map<unsigned long long, MessageTiming> message_times_;
  std::mutex message_times_lock_;
  void UpdateTimingMetrics(unsigned long long id);

  CachedHistogram time_in_queue_{nullptr};
  CachedHistogram time_processing_{nullptr};
  CachedHistogram time_acknowledging_{nullptr};
  CachedHistogram total_time_to_completion_{nullptr};

  CachedGauge consumer_state_unknown_{nullptr};
  CachedGauge consumer_state_connecting_{nullptr};
  CachedGauge consumer_state_active_{nullptr};
  CachedGauge consumer_state_shut_down_{nullptr};
  CachedGauge consumer_state_auth_rejected_{nullptr};
  void UpdateConsumerStateMetrics(ClientState state);
};

}  // namespace client
}  // namespace monolith_grpc

#endif
