import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from elasticsearch import ElasticSearch
import os
import sys
import time
from concurrent import futures

# Central server node class serves as intermediary for facilitating client requests and distributing tasks to workers
# inherits the Servicer class for interface methods, and Stub class for client requests
class CentralNode(mini_splunk_protobuf_pb2_grpc.MiniSplunkServicer):
    def __init__(self, worker_addresses):
        self.worker_nodes = {}
 
        for address in worker_addresses:
            # create channels to every worker node
            channel = grpc.insecure_channel(address)

            self.worker_nodes[address] = {
                "channel": channel,
                "stub": mini_splunk_protobuf_pb2_grpc.MiniSplunkStub(channel),
            }
        
        #connect via the elastic search cluster nodes
		self.elastic_client = Elasticsearch(
			hosts=[
				"http://elastic_node_0:9200",
				"http://elastic_node_1:9200",
				"http://elastic_node_2:9200",
			]
		)

    def Ingest(self, request_iterator, context):
        pass

    def SearchDate(self, request, context):
        pass

def main():
    address = "0.0.0.0:50050"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # addresses of the worker nodes
    worker_addresses = [
        "worker_node_0:50051", # using the docker service name automatically resolves to the node's IP
        "worker_node_1:50051",
        "worker_node_2:50051",
        "worker_node_3:50051",
        "worker_node_4:50051",
    ]

    node = CentralNode(worker_addresses)
    mini_splunk_protobuf_pb2_grpc.add_MiniSplunkServicer_to_server(node, server)
    server.add_insecure_port(address)
    server.start()

    try:
        print(f"Central Node Started on {address}")
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        print()
        return

    except grpc.RpcError as e:
        return

if __name__ == "__main__":
    main()