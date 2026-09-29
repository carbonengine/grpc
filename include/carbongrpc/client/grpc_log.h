// Copyright © 2025 CCP ehf.
#pragma once

#include <list>
#include <mutex>
#include <string>

#include "grpc/support/log.h"

namespace monolith_grpc::client {


struct GrpcLogEntry {
  std::string file;
  int line;
  gpr_log_severity severity;
  std::string message;

  GrpcLogEntry(std::string file, int line, gpr_log_severity severity, std::string message);
};

class GrpcLog {
public:

  static void Initialize();

  static void SetLogLevel(gpr_log_severity level);

  [[nodiscard]] static std::list<GrpcLogEntry> GetLogEntries();

private:

  static std::once_flag init_flag_;
};

}  // namespace monolith_grpc::client
