// Copyright © 2025 CCP ehf.
#include "carbongrpc/client/stream_counter.h"

std::atomic_uint StreamCounter::stream_id_ = {0};
std::once_flag stream_id_initialized;

// cppcheck-suppress unusedFunction
unsigned int StreamCounter::AssignStreamId() {
  std::call_once(stream_id_initialized, []() { StreamCounter::stream_id_ = 1; });

  auto result = stream_id_.fetch_add(1);
  return result;
}
