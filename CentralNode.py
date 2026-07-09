import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from cassandra.cluster import Cluster
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

        # main cassandra cluster, with the cassandra seed node as contact point
        cassandra_cluster = Cluster(
            ["cassandra_seed"],
            port=9402
        )

        # connect to the cluster
        session = cassandra_cluster.connect()

        # create keyspace
        session.execute(
            """
                CREATE KEYSPACE IF NOT EXISTS syslog_keyspace
                WITH REPLICATION = {
                    'class' : 'SimpleStrategy',
                    'replication_factor' : 3
                };
            """
        )

        # initialize table with date and timestamp as compound partition key
        session.execute(
            """
                CREATE IF NOT EXISTS syslog_keyspace.syslogs(
                    date text,
                    timestamp text,
                    hostname text,
                    daemon text,
                    severity text,
                    message text,
                    PRIMARY KEY (date, timestamp)
                );
            """
        )

        # terminate connection
        cassandra_cluster_nodes.shutdown()

    def Ingest(self, request_iterator, context):
        pass

    def SearchDate(self, request, context):
        return self.nodes[0]["stub"].SearchDate(request)

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