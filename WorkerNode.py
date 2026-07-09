import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from cassandra.cluster import Cluster
import os
import sys
import time
import re
from concurrent import futures

syslog_pattern = re.compile(
    r"^([a-zA-Z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s([\w\-._]+)\s([\w-]+(?:\[\d+\])?):\s+(.*)$"
)

# Main worker node class for message parsing, structuring, and database operations
class WorkerNode(mini_splunk_protobuf_pb2_grpc.MiniSplunkServicer):
	def __init__(self, node_name):
		pass

	def Ingest(self, request_iterator, context):
		pass

	def Purge(self, request, context):
		pass

	def SearchDate(self, request, context):
		pass


def main():
	node_name = "Node"
	address = "0.0.0.0:50051"

	# each worker node uses 10 internal worker threads
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	node = WorkerNode(node_name)
	mini_splunk_protobuf_pb2_grpc.add_MiniSplunkServicer_to_server(node, server)
	server.add_insecure_port(address)
	server.start()

	try:
		print(f"Node Started on {address}")
		while True:
			time.sleep(1000)
	except KeyboardInterrupt:
		print()
		return
	except grpc.RpcError as e:
		return

if __name__ == "__main__":
	main()