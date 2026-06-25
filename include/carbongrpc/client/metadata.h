// Copyright © 2025 CCP ehf.
#ifndef METADATA_H
#define METADATA_H

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstdint>
#include <string>

namespace monolith_grpc::client {
class Metadata {
public:

  static void ApplyToContext(::grpc::ClientContext& context);

  [[nodiscard]] static std::string application_instance_uuid();
  static void set_application_instance_uuid(const std::string& uuid);

  [[nodiscard]] static std::string auth_token();
  static void set_auth_token(const std::string& token);

  /// Incremented every time set_auth_token is called. Clients parked after an
  /// UNAUTHENTICATED rejection watch this to know fresh credentials arrived.
  [[nodiscard]] static std::uint64_t auth_generation();

private:

  static std::string application_instance_uuid_;
  static std::string auth_token_;
  static std::atomic<std::uint64_t> auth_generation_;
};
}  // namespace monolith_grpc

#endif
