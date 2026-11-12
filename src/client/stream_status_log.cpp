// Copyright © 2025 CCP ehf.
#include "carbongrpc/client/stream_status_log.h"

using namespace monolith_grpc::client;

StreamStatus::StreamStatus(unsigned int stream_id, ::grpc::Status  status)
  : stream_id(stream_id),
    status(std::move(status)) {
}

StreamStatusLog::StreamStatusLog()
  : max_log_records_(10000) {
}

void StreamStatusLog::RecordStatus(const StreamStatus& status) {
  std::scoped_lock<std::mutex> lock(lock_);
  log_.push_back(status);

  if (max_log_records_ > 0) {
    while (log_.size() > max_log_records_) {
      log_.pop_front();
    }
  }
}

void StreamStatusLog::RecordStatus(unsigned int stream_id, const ::grpc::Status& status) {
  RecordStatus(StreamStatus(stream_id, status));
}

std::list<StreamStatus> StreamStatusLog::GetStatusLog() {
  std::scoped_lock<std::mutex> lock(lock_);

  std::list<StreamStatus> result = std::move(log_);
  log_ = std::list<StreamStatus>();

  return result;
}
