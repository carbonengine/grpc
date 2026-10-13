// Copyright © 2025 CCP ehf.
#include <list>
#include <mutex>
#include <string>
#include <memory>

#include <absl/log/log_sink_registry.h>
#include <absl/log/globals.h>
#include <CCPLog.h>

#include "carbongrpc/client/grpc_log.h"

namespace monolith_grpc::client {

static CcpLogChannel_t s_chGRPC { 1, "carbon-grpc", "grpc", 0 };

class LogSink : public absl::LogSink
{
public:
	void Send(const absl::LogEntry& entry) override
	{
		auto severityEnumType = entry.log_severity();

		auto sourceFile = std::string(entry.source_filename());
		int sourceLine = entry.source_line();
		auto message = std::string(entry.text_message());

		switch (severityEnumType)
		{
		case absl::LogSeverity::kInfo:
			CCP_LOG_CH(s_chGRPC, "%s (%s:%d)", message.c_str(), sourceFile.c_str(), sourceLine );
			break;
		case absl::LogSeverity::kWarning:
			CCP_LOGWARN_CH(s_chGRPC, "%s (%s:%d)", message.c_str(), sourceFile.c_str(), sourceLine );
			break;
		case absl::LogSeverity::kFatal:
		case absl::LogSeverity::kError:
			CCP_LOGERR_CH(s_chGRPC, "%s (%s:%d)", message.c_str(), sourceFile.c_str(), sourceLine );
			break;
		}

		// the rest of this function only exists to support deprecated behaviour that exposes logs to python

		int severity  {static_cast<std::underlying_type_t<decltype(severityEnumType)>>(severityEnumType)};
		if (severity < m_verbosity)
		{
			return;
		}
		std::scoped_lock lock(m_lock);
		m_log.emplace_back(GrpcLogEntry(sourceFile, sourceLine, (gpr_log_severity)severity, message));
		while (m_log.size() > 10000) {
			m_log.pop_front();
		}
	}

	void SetVerbosity(int verbosity)
	{
		m_verbosity = verbosity;
	}

	std::list<GrpcLogEntry> GetLogEntries()
	{
		std::scoped_lock lock(m_lock);

		std::list<GrpcLogEntry> result = std::move(m_log);
		m_log = std::list<GrpcLogEntry>();

		return result;
	}

private:
	std::mutex m_lock;
	std::list<GrpcLogEntry> m_log;
	int m_verbosity{GPR_LOG_SEVERITY_ERROR};
};

GrpcLogEntry::GrpcLogEntry(std::string file, int line, gpr_log_severity severity, std::string message)
  : file(std::move(file)),
    line(line),
    severity(severity),
    message(std::move(message)) {
}

std::once_flag GrpcLog::init_flag_;
static std::unique_ptr<LogSink> log_sink;

void GrpcLog::Initialize() {
  std::call_once(init_flag_, []() {
  	log_sink = std::make_unique<LogSink>();
  	absl::AddLogSink( log_sink.get() );
  	absl::SetStderrThreshold(absl::LogSeverity::kFatal);
  });
}

// cppcheck-suppress unusedFunction
[[deprecated ("All grpc logging goes to CCP_LOG through an abseil log sink")]]
void GrpcLog::SetLogLevel(gpr_log_severity level) {
  Initialize();
  log_sink->SetVerbosity( level );
}

// cppcheck-suppress unusedFunction
[[deprecated ("All grpc logging goes to CCP_LOG through an abseil log sink")]]
std::list<GrpcLogEntry> GrpcLog::GetLogEntries() {
  Initialize();
  return log_sink->GetLogEntries();
}

}  // namespace monolith_grpc::client
