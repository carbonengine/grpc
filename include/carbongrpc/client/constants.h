// Copyright © 2025 CCP ehf.
#ifndef CONSTANTS_H
#define CONSTANTS_H

namespace monolith_grpc {

enum class AsyncMode {
  kAsync,
  kSynchronous
};

enum class BlockingMode {
  kBlocking,
  kNonBlocking
};

enum class PublishResult {
  kSuccess,
  kFailure,
  kQueueFull,
  kTimeLimitExceeded,
  kMessageSizeLimitExceeded
};

enum class ClientState {
  kUnknown,
  kConnecting,
  kActive,
  kShutDown,
  kAuthRejected,  ///< parked after UNAUTHENTICATED; re-arms on new credentials
};

}  // namespace monolith_grpc

#endif
