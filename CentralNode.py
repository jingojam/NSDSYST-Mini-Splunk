import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from elasticsearch import Elasticsearch
import os
import sys
import time
import itertools
from concurrent import futures

# Central server node class serves as intermediary for facilitating client requests and distributing tasks to workers
# inherits the Servicer class for interface methods, and Stub class for client requests
class CentralNode(mini_splunk_protobuf_pb2_grpc.MiniSplunkServicer):
    def __init__(self, worker_addresses):
        self.node_name = os.getenv("NODE_NAME")
        self.worker_node_count = len(worker_addresses)
        self.worker_nodes = {}
        self.worker_node_addresses = worker_addresses
        self.current_worker_node = 0
        
        for address in worker_addresses:
            # create channels to every worker node
            channel = grpc.insecure_channel(address)
            self.worker_nodes[address] = {
                "channel": channel,
                "stub": mini_splunk_protobuf_pb2_grpc.MiniSplunkStub(channel),
            }
            time.sleep(20)
            pong = self.worker_nodes[address]["stub"].SendPing(mini_splunk_protobuf_pb2.Ping(sender="CENTRAL_GATEWAY"))
            if pong.receiver and pong.sender.sender == "CENTRAL_GATEWAY":
                print(f"[STATUS] Worker Node Active on {address}", flush=True)
            else:
                print(f"[STATUS] Unable to Reach Worker Node on {address}", flush=True)
                
        #connect via the elastic search cluster nodes
        self.elastic_client = Elasticsearch(
            hosts=[
                "http://elastic_node_0:9200",
                "http://elastic_node_1:9200",
                "http://elastic_node_2:9200",
            ]
        )

    # updates current worker node to next node
    def Next(self):
        # update the last worker node to next (cycles back to 0 --the first worker node)
        self.current_worker_node = (self.current_worker_node + 1) % self.worker_node_count

    """Service for File Ingests. Accepts a stream (flow) of `LogString` messages. """
    def Ingest(self, request_iterator, context):
        # function to yield request messages of (`mini_splunk_protobuf_pb2.LogString()`) from an iterator
        def Requests(iterator):
            for request in iterator:
                yield request
        # send the requests to the worker node for ingestion
        res = self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].Ingest(Requests(request_iterator))
        # move to the next worker node
        self.Next()
        return res

    """Service for Purging Logs. Accepts a `PurgeRequest` message to signal Server. """
    def Purge(self, request, context):
        pass

    """Service for filtering logs based on Date criterion. Returns a stream of 0-n `LogString` messages. """
    def SearchDate(self, request, context):
        pass

    """Service for filtering logs based on Hostname criterion. Returns a stream of 0-n `LogString` messages. """
    def SearchHost(self, request, context):
        pass

    """Service for filtering logs based on Process criterion. Returns a stream of 0-n `LogString` messages. """
    def SearchDaemon(self, request, context):
        pass

    """Service for filtering logs based on Severity criterion. Returns a stream of 0-n `LogString` messages. """
    def SearchSeverity(self, request, context):
        pass

    """Service for filtering logs based on Keyword/s criterion. Returns a stream of 0-n `LogString` messages. """
    def SearchKeyword(self, request, context):
        pass

    """Service for filtering logs based on Keyword/s criterion and accumulating matches. Returns `LogCount` match amount message. """
    def CountKeyword(self, request, context):
        pass

    def SendPing(self, request, context):
        return mini_splunk_protobuf_pb2.Pong(
            receiver=self.node_name,
            sender=request
        )

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