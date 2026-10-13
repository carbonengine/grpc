// Copyright © 2026 Fenris Creations ehf.

#include <gtest/gtest.h>
#include <carbongrpc/client/grpc_log.h>
#include "absl/log/log.h"
#include "absl/log/initialize.h"

#include <CCPLog.h>

#include <iostream>

class TestEnv : public ::testing::Test
{
public:
	static void SetUpTestSuite()
	{
		absl::InitializeLog();
		monolith_grpc::client::GrpcLog::Initialize();
		Test::SetUpTestSuite();
	}

	void SetUp() override
	{
		s_error_count_tracker = CCP::GetLogCounter(CCP::LOGTYPE_ERR);
		s_warn_count_tracker = CCP::GetLogCounter(CCP::LOGTYPE_WARN);
		s_info_count_tracker = CCP::GetLogCounter(CCP::LOGTYPE_INFO);
		const auto _ = monolith_grpc::client::GrpcLog::GetLogEntries();
	}

	static uint32_t s_info_count_tracker;
	static uint32_t s_warn_count_tracker;
	static uint32_t s_error_count_tracker;

	static int GetErrorCount()
	{
		return CCP::GetLogCounter(CCP::LOGTYPE_ERR) - s_error_count_tracker;
	}

	static int GetWarnCount()
	{
		return CCP::GetLogCounter(CCP::LOGTYPE_WARN) - s_warn_count_tracker;
	}

	static int GetInfoCount()
	{
		return CCP::GetLogCounter(CCP::LOGTYPE_INFO) - s_info_count_tracker;
	}
};


uint32_t TestEnv::s_info_count_tracker = 0;
uint32_t TestEnv::s_warn_count_tracker = 0;
uint32_t TestEnv::s_error_count_tracker = 0;

TEST_F(TestEnv, CcpLog_Sink)
{
	LOG(INFO) << "INFO" << std::endl;
	ASSERT_EQ( 1, GetInfoCount());
	LOG(WARNING) << "WARNING" << std::endl;
	ASSERT_EQ( 1, GetWarnCount());
	LOG(ERROR) << "ERROR" << std::endl;
	ASSERT_EQ( 1, GetErrorCount() );
}

TEST_F (TestEnv, SetLogLevelDebugOld)
{
	monolith_grpc::client::GrpcLog::SetLogLevel( GPR_LOG_SEVERITY_DEBUG );
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(INFO) << "INFO" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
	LOG(WARNING) << "WARNING" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
	LOG(ERROR) << "ERROR" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
}

TEST_F (TestEnv, SetLogLevelWarnOld)
{
	monolith_grpc::client::GrpcLog::SetLogLevel( GPR_LOG_SEVERITY_INFO );
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(INFO) << "INFO" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(WARNING) << "WARNING" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
	LOG(ERROR) << "ERROR" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
}

TEST_F (TestEnv, SetLogLevelErrOld)
{
	monolith_grpc::client::GrpcLog::SetLogLevel( GPR_LOG_SEVERITY_ERROR );
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(INFO) << "INFO" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(WARNING) << "WARNING" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 0 );
	LOG(ERROR) << "ERROR" << std::endl;
	ASSERT_EQ( monolith_grpc::client::GrpcLog::GetLogEntries().size(), 1 );
}