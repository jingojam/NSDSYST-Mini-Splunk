#include <iostream>
#include "WorkerNode.h"

// [1] Node Name, [2] IP:Port
int main(int argc, char* argv[]){
	std::string worker_address = argv[2];
	WorkerNode worker(argv[1]);
	
	grpc::ServerBuilder builder; // factory grpc server instantiator
	builder.AddListeningPort(worker_address, grpc::InsecureServerCredentials()); // worker listens on the address specified in the arguments
	builder.RegisterService(&worker); // register the server object as service
	std::unique_ptr<grpc::Server> server(builder.BuildAndStart()); // start the server
	
	std::cout << "Worker " << argv[1] << " Started on " << argv[2] << "\n";
	server->Wait();
	
	return 0;
}