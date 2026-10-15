#ifndef RECONNECT_BACKOFF_H
#define RECONNECT_BACKOFF_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <random>
#include <thread>

namespace monolith_grpc {
namespace client {

/// Capped exponential backoff with jitter for stream reconnect attempts.
///
/// Streams that die immediately (gateway down, auth rejection, network flap)
/// must not be retried at full speed: a fleet of clients doing so stampedes a
/// recovering gateway and floods telemetry. The delay doubles per failed
/// attempt up to a cap and resets once a stream proves healthy (lived long
/// enough or received a message).
class ReconnectBackoff {
public:

  using clock = std::chrono::steady_clock;

  explicit ReconnectBackoff(
    clock::duration base = std::chrono::milliseconds(250), clock::duration max = std::chrono::seconds(30),
    double multiplier = 2.0, double jitter_ratio = 0.2,
    clock::duration healthy_stream_duration = std::chrono::seconds(60)
  )
    : base_(base),
      max_(max),
      multiplier_(multiplier),
      jitter_ratio_(jitter_ratio),
      healthy_stream_duration_(healthy_stream_duration),
      current_(base),
      rng_(std::random_device{}()) {
  }

  /// The delay to sleep before the next reconnect attempt; advances the
  /// internal state so consecutive failures wait progressively longer.
  [[nodiscard]] clock::duration NextDelay() {
    auto delay = current_;

    auto next = std::chrono::duration_cast<clock::duration>(current_ * multiplier_);
    current_ = std::min(next, max_);

    if (jitter_ratio_ > 0.0) {
      std::uniform_real_distribution<double> dist(1.0 - jitter_ratio_, 1.0 + jitter_ratio_);
      delay = std::chrono::duration_cast<clock::duration>(delay * dist(rng_));
    }

    return std::min(delay, max_);
  }

  /// Call when a stream becomes active.
  void NoteStreamStart() {
    stream_started_ = clock::now();
    saw_message_ = false;
  }

  /// Call when any message flows on the stream; a stream that carried
  /// traffic resets the backoff regardless of how long it lived.
  void NoteHealthy() {
    saw_message_ = true;
  }

  /// Call when a stream ends; resets the backoff if the stream was healthy.
  void NoteStreamEnd() {
    if (saw_message_ || clock::now() - stream_started_ >= healthy_stream_duration_) {
      Reset();
    }
  }

  void Reset() {
    current_ = base_;
  }

  /// Sleep for the given duration in short slices, aborting early when the
  /// predicate becomes true (shutdown, reconnect request, new credentials).
  template<typename Predicate>
  void SleepFor(clock::duration duration, Predicate abort) const {
    constexpr auto kSlice = std::chrono::milliseconds(5);
    const auto deadline = clock::now() + duration;
    while (clock::now() < deadline) {
      if (abort()) {
        return;
      }
      std::this_thread::sleep_for(kSlice);
    }
  }

private:

  clock::duration base_;
  clock::duration max_;
  double multiplier_;
  double jitter_ratio_;
  clock::duration healthy_stream_duration_;

  clock::duration current_;
  clock::time_point stream_started_{};
  std::atomic<bool> saw_message_{false};
  std::mt19937 rng_;
};

}  // namespace client
}  // namespace monolith_grpc

#endif
