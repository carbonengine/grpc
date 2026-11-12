// Copyright © 2025 CCP ehf.
#include "carbongrpc/client/resolver.h"
using namespace monolith_grpc::client;

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#ifdef _POSIX_VERSION
	#include <arpa/inet.h>
	#include <netdb.h>
#endif

std::string Resolver::servers_;

// void GetServers(ares_channel channel) {
//	ares_addr_node* servers = nullptr;
//	int status = ares_get_servers(channel, &servers);
//	if (status == ARES_SUCCESS) {
//		printf("ares using dns servers:\n");
//		for (auto server = servers; server != nullptr; server =
// server->next) { 			char str[INET6_ADDRSTRLEN] = { 0 };
// inet_ntop(server->family, &server->addr, str, INET_ADDRSTRLEN);
// printf(" %s\n", str);
//		}
//	}
//	else {
//		printf("ares_get_servers failed. status=%d\n", status);
//	}
//	ares_free_data(servers);
// }

Resolver::Result::Result(ares_channel channel, const char* name, QueryType query_type)
  : channel_(channel),
    need_destroy_channel_(true),
    query_type_(query_type),
    request_name_(name),
    error_(-1) {
  if (query_type == QueryType::kHostname) {
    status_ = Status::kInProgress;
    ares_gethostbyname(channel_, name, AF_INET, ares_cb_host, static_cast<void*>(this));
  } else if (query_type == QueryType::kSRV) {
    status_ = Status::kInProgress;
    ares_query(channel_, name, /*dnsclass=ns_c_in*/ 1, /*type=ns_t_srv*/ 33, ares_cb_query, static_cast<void*>(this));
  } else {
    status_ = Status::kFailure;
  }
}

Resolver::Result::~Result() {
  if (need_destroy_channel_) {
    ares_destroy(channel_);
  }
}

Resolver::Status Resolver::Result::status() {
  Update();
  return status_;
}

// cppcheck-suppress unusedFunction
int Resolver::Result::error() {
  Update();
  return error_;
}

// cppcheck-suppress unusedFunction
std::string Resolver::Result::request_name() {
  Update();
  return request_name_;
}

// cppcheck-suppress unusedFunction
std::string Resolver::Result::official_name() {
  Update();
  return official_name_;
}

// cppcheck-suppress unusedFunction
std::vector<std::string> Resolver::Result::addresses() {
  Update();
  return addresses_;
}

// cppcheck-suppress unusedFunction
std::vector<std::string> Resolver::Result::aliases() {
  Update();
  return aliases_;
}

// cppcheck-suppress unusedFunction
std::vector<Resolver::Result::Srv> Resolver::Result::srv_result() {
  Update();
  return srv_results_;
}

void Resolver::Result::Update() {
  if (status_ != Status::kInProgress) {
    return;
  }

  int nfds;
  fd_set read_fds;
  fd_set write_fds;
  timeval* tvp;
  timeval tv{};

  FD_ZERO(&read_fds);
  FD_ZERO(&write_fds);

  nfds = ares_fds(channel_, &read_fds, &write_fds);
  if (nfds == 0) {
    status_ = Status::kSuccess;
    return;
  }

  tvp = ares_timeout(channel_, nullptr, &tv);
  select(nfds, &read_fds, &write_fds, nullptr, tvp);
  ares_process(channel_, &read_fds, &write_fds);
}

void Resolver::Result::ares_cb_host(void* arg, int status, int timeouts, struct hostent* hostent) {
  auto* owner = (Resolver::Result*)arg;
  owner->error_ = status;

  if (status == ARES_SUCCESS) {
    char buf[INET6_ADDRSTRLEN];
    owner->official_name_ = hostent->h_name;
    for (int i = 0; hostent->h_aliases[i]; ++i) {
      owner->aliases_.emplace_back(hostent->h_aliases[i]);
    }
    int proto = hostent->h_addrtype;
    for (int i = 0; hostent->h_addr_list[i]; ++i) {
      inet_ntop(proto, hostent->h_addr_list[i], buf, INET6_ADDRSTRLEN);
      owner->addresses_.emplace_back(buf);
    }

    owner->status_ = Status::kSuccess;
  } else {
    printf("failed. status = %d\n", status);
    owner->status_ = Status::kFailure;
  }
}

void Resolver::Result::ares_cb_query(void* arg, int status, int timeouts, unsigned char* abuf, int alen) {
  auto* owner = (Resolver::Result*)arg;
  owner->error_ = status;

  if (status == ARES_SUCCESS) {
    owner->srv_results_.clear();

    ares_srv_reply* reply;
    ares_parse_srv_reply(abuf, alen, &reply);

    while (reply != nullptr) {
      Srv srv;
      srv.host = reply->host;
      srv.port = reply->port;
      srv.priority = reply->priority;
      srv.weight = reply->weight;
      owner->srv_results_.push_back(srv);

      reply = reply->next;
    }

    ares_free_data(reply);

    owner->status_ = Status::kSuccess;
  } else {
    printf("failed. status = %d\n", status);
    owner->status_ = Status::kFailure;
  }
}

void Resolver::Initialize() {
  // grpc does call init but it's safe to call multiple times, and this ensures
  // we aren't waiting for grpc to init
  ares_library_init(ARES_LIB_INIT_ALL);

  // Let grpc do cleanup because it might outlive the resolver, so we don't need
  // any destructor handling
}

// cppcheck-suppress unusedFunction
std::unique_ptr<Resolver::Result> Resolver::Resolve(const char* name, QueryType query_type) {
  ares_channel channel;

  ares_options opts{};
  opts.flags = ARES_FLAG_NOCHECKRESP;

  int ret = ares_init_options(&channel, &opts, ARES_OPT_FLAGS);
  if (ret != ARES_SUCCESS) {
    return nullptr;
  }

  if (!servers_.empty()) {
    int res = -1;
    while (res != ARES_SUCCESS) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      res = ares_set_servers_ports_csv(channel, servers_.c_str());
    }
  }

  std::unique_ptr<Result> result = std::make_unique<Result>(channel, name, query_type);
  return result;
}

// cppcheck-suppress unusedFunction
void Resolver::SetServers(const char* servers) {
  servers_ = servers;
}
