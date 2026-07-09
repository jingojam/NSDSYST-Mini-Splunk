#ifndef WORKER_NODE_H
#define WORKER_NODE_H

/*standard C++ headers*/
#include <iostream>
#include <string>

/*regex*/
#include <regex>

/*database header*/
#include <sqlite3.h> // SQLite 3

/*thread safety headers*/
#include <thread>
#include <mutex>
#include <shared_mutex> // for unique_lock (write lock) and shared_lock (read lock)

/*gRPC headers*/
#include <grpcpp/grpcpp.h>
#include "mini_splunk_protobuf.grpc.pb.h"
#include "mini_splunk_protobuf.pb.h"

static std::shared_mutex worker_mutex;
static const auto syslog_pattern = std::regex(R"^([a-zA-Z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s([\w\-._]+)\s([\w-]+(?:\[\d+\])?):\s+(.*)$"); 

class WorkerNode final : public MiniSplunk::Service{
	private:
		std::string node_name;
		std::string db_filename;
		sqlite3* db;
		
	public:
		WorkerNode(std::string node_name);
		~WorkerNode();
		
		// Service Methods
		
		// Write Operations (write-locked)
		grpc::Status Ingest(grpc::ServerContext* context, grpc::ServerReader<LogString>* reader, RequestStatus* response);
		grpc::Status Purge(grpc::ServerContext* context, PurgeRequest*, RequestStatus* response);
		
		// Read Operations (shared read-locked)
		grpc::Status SearchDate(grpc::ServerContext* context, const QueryRequest* request, LogResults* response);
};

#endif