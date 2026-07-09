protoc -I=. --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` mini_splunk_protobuf.proto
g++ main.cpp mini_splunk_protobuf.pb.cc mini_splunk_protobuf.grpc.pb.cc WorkerNode.cpp -o worker.exe     -lgrpc++     -lprotobuf     -lsqlite3     -lgrpc
