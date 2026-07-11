import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
import os

def main():
    channel = grpc.insecure_channel("central_gateway:50050")
    stub = mini_splunk_protobuf_pb2.MiniSplunkStub(channel)

    #client stuff...

if __name__ == "__main__":
    main()