// Copyright © 2025 CCP ehf.
#ifndef PUBLISHER_H
#define PUBLISHER_H

// std
#include <atomic>
#include <chrono>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// grpc
#include <grpcpp/grpcpp.h>

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

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

namespace monolith_grpc {
namespace client {

template<typename MessageType>
class Publisher {
public:

  Publisher();
  virtual ~Publisher() = default;

  virtual void Initialize();  ///< call immediately after the constructor
  virtual void Shutdown();    ///< call immediately before the destructor

  void SetConnection(std::shared_ptr<Connection> connection);

  using clock = std::chrono::steady_clock;
  struct UnpackedMessage {
    explicit UnpackedMessage(
      MessageType* in_message, void* in_py_message = nullptr, google::protobuf::Message* in_payload = nullptr,
      void* in_py_payload = nullptr, unsigned long long id = 0
    );
    MessageType* message;
    google::protobuf::Message* payload;
    void* py_message;
    void* py_payload;

    unsigned long long id;

    clock::time_point time_received;
    clock::time_point time_retrieved;
    clock::time_point time_delivered;
    clock::time_point time_confirmed;
  };

  [[nodiscard]] PublishResult PublishMessage(UnpackedMessage* message);
  void WaitUntilPublishesComplete();
  void set_on_message_published(std::function<void(UnpackedMessage*)> callback);
  [[nodiscard]] unsigned int num_pending_messages() const;

  void PauseSending();
  void ResumeSending();

  void Ping();
  [[nodiscard]] clock::duration PingResult();
  [[nodiscard]] bool PingInProgress();

  [[nodiscard]] std::list<StreamStatus> GetStatusLog();

  virtual void set_metric_registry(
    prometheus_module::MetricRegistryInterface* registry, const std::string& prefix,
    const std::map<std::string, std::string>& labels, AsyncMode async
  );

  void set_max_message_size_bytes(size_t max_bytes);  ///< Values <=0 mean "unlimited"

  void set_max_queue_size_bytes(size_t max_bytes);  ///< Values <=0 mean "unlimited"
  [[nodiscard]] size_t max_queue_size_bytes() const;

  void set_compression_algorithm(int algorithm);
  [[nodiscard]] int compression_algorithm() const;

  [[nodiscard]] grpc_connectivity_state channel_state();
  [[nodiscard]] int active_connection_id();
  [[nodiscard]] unsigned int active_stream_id() const;

  void set_publisher_state(ClientState state);
  [[nodiscard]] ClientState publisher_state() const;

  /// When enabled, a stream ending with UNAUTHENTICATED parks the publisher in
  /// ClientState::kAuthRejected instead of reconnecting; re-arms on new
  /// credentials (Metadata::set_auth_token) or SetConnection. Default off.
  void set_auth_stop_enabled(bool enabled);
  [[nodiscard]] bool auth_stop_enabled() const;

protected:

  // Async
  void Async(const std::function<void()>& lambda);
  Jobs jobs_;

  // Connection
  std::shared_ptr<Connection> connection_;
  void WithConnection(const std::function<void(Connection&)>& f);
  std::shared_ptr<monolith_grpc::client::Channel<Connection::ConnectionMessage>> connection_listener_;
  std::shared_ptr<grpc::Channel> channel_;
  std::atomic<bool> request_disconnect_;
  int compression_algorithm_;
  void ConnectionMonitor();

  // Publisher thread and queue
  void PublisherThread();
  void PublishMessages();
  void ReadConfirms();
  void ReadEmpties();
  std::unique_ptr<grpc::ClientContext> context_;
  std::mutex context_lock_;
  std::thread publisher_thread_;
  ClientState publisher_state_;
  std::deque<UnpackedMessage*> queued_messages_;
  std::mutex queue_lock_;
  std::condition_variable queue_condition_;
  std::atomic<bool> shutting_down_;
  std::atomic<int> pending_messages_;
  std::atomic<bool> paused_;
  std::function<void(UnpackedMessage*)> on_message_published_;
  std::mutex callback_lock_;

  void WaitForConnection();
  [[nodiscard]] bool SetUpStream();
  void ProcessMessages();

  // Message limits
  size_t max_message_size_bytes_;

  // Queue limits
  size_t max_queue_size_bytes_;
  size_t queue_size_bytes_;

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

  virtual std::string metrics_message() = 0;
  virtual std::string metrics_domain() = 0;
  [[nodiscard]] virtual bool PrepareStream(grpc::ClientContext* context) = 0;
  virtual void FinishStream() = 0;
  [[nodiscard]] virtual bool WriteMessageToStream(const MessageType& message) = 0;
  virtual void WritesDone() = 0;
  [[nodiscard]] virtual bool SendPing() = 0;

  struct Confirmation {
    unsigned long long message_id;
    bool read_ok;
  };
  [[nodiscard]] virtual Confirmation ReadConfirm() = 0;
  bool confirms_enabled_;
  virtual bool ReadEmpty() = 0;
  bool empty_bidi_enabled_;

  // Message queue
  [[nodiscard]] UnpackedMessage* GetNextMessage();
  [[nodiscard]] UnpackedMessage* PeekNextMessage(std::unique_lock<std::mutex>& queue_lock);
  void MessageConfirmed(unsigned long long id);
  unsigned long long last_confirmed_message_;
  unsigned long long next_message_to_send_;
  unsigned long long next_assignable_id_;
  bool reader_exited_;
  bool writer_exited_;

  // Ping
  bool pings_enabled_;
  clock::duration ping_result_;
  std::atomic<bool> ping_in_progress_;
  void PingThread();

  // Metrics
  prometheus_module::MetricRegistryInterface* metric_registry_;
  std::string metric_prefix_;
  std::map<std::string, std::string> metric_labels_;
  std::mutex metrics_lock_;
  std::thread metrics_thread_;
  void CommitMetrics();

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

  CachedHistogram time_in_queue_{nullptr};
  CachedHistogram time_sending_{nullptr};
  CachedHistogram time_confirming_{nullptr};
  CachedHistogram total_time_to_delivery_{nullptr};
  CachedHistogram total_time_to_confirmation_{nullptr};
  void UpdateTimingMetrics(UnpackedMessage* message);

  CachedGauge publisher_state_unknown_{nullptr};
  CachedGauge publisher_state_connecting_{nullptr};
  CachedGauge publisher_state_active_{nullptr};
  CachedGauge publisher_state_shut_down_{nullptr};
  CachedGauge publisher_state_auth_rejected_{nullptr};
  void UpdatePublisherStateMetrics(ClientState state);
};

}  // namespace client
}  // namespace monolith_grpc

#endif
