import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import os
import sys
import time
import re
from concurrent import futures

# severity levels
severity_level = [
	"EMERGENCY",    # index 0: emergency (level 0)
	"ALERT",
	"CRITICAL",
	"ERROR",
	"WARNING",
	"NOTICE",
	"INFORMATIONAL",
	"DEBUG"
]

# severity level mappings to strings
inferred_severity = {
	# emergency
	"panic": 0, "emerg": 0, 

	# alert
	"alert": 1, "immediate": 1,

	# critical
	"crit": 2, "critical": 2, "fatal": 2,
    
	# error
	"error": 3, "fail": 3, "failed": 3, "failure": 3, "err": 3,
    
	# warning
	"warning": 4, "warn": 4, "invalid": 4, "denied": 4, "refused": 4, 
	"unknown": 4, "timeout": 4, 
    
    # notice
	"notice": 5, "significant": 5, 
    
	# informational
	"info": 6, "accepted": 6, "opened": 6, "closed": 6, 

	# debug
	"debug": 7, "verbose": 7
}

keys = list(inferred_severity.keys())

"""
	regex pattern for syslog (RFC 3164 BSD Syslog format)
		* all groups (index [0]):
			-> entire matched string 

		* group 1 (index [1]) priority (optional):
			-> (\<\d+\>\s+)? -> matches 1+ numbers enclosed by < > angle brackets, 1+ space (optional in case logs do not have priority level)

		* group 2 (index [2]) MONTH DAY:
			-> ^([a-zA-Z]{3}\s+\d{1,2}) -> matches 3 alphabetical characters (month), 1+ space, 2 digits (day)
		
		* group 3 (index [3]) Timestamp:
			-> (\d{2}:\d{2}:\d{2}) -> matches 2 digits (hour) : 2 digits (minute) : 2 digits (second)
		
		* group 4 (index [4]) Hostname:
			-> ([\w\-._]+) -> matches 1+ alphanumeric characters, including dash (-), dot (.), and underscore (_)

		* group 5 (index [5]) Daemon (and optional PID):
			-> ([\w-]+(?:\[\d+\])?) -> matches 1+ alphanumeric characters, and optional 1+ integer PID enclosed by [ ]

		* group 6 (index [6]) Message:
			-> (.*)$ -> matches wildcard for multiple characters
"""
syslog_pattern = re.compile(
    r"^([a-zA-Z]{3}\s+\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s([\w\-._]+)\s([\w-]+(?:\[\d+\])?):\s+(.*)$"
)

"""Main worker node class for message parsing, structuring, and database operations
	inherits the `MiniSplunkServicer` servicer class interface
"""
class WorkerNode(mini_splunk_protobuf_pb2_grpc.MiniSplunkServicer):
	"""class Default constructor
	"""
	def __init__(self):
		# connect to elasticsearch cluster
		self.elastic_client = Elasticsearch(
			hosts=[
				"http://elastic_node_0:9200",
				"http://elastic_node_1:9200",
				"http://elastic_node_2:9200",
			]
		)

	def CreateIndex(index_name):
		if not self.elastic_client.indices.exists(index=index_name):
			self.elastic_client.indices.create(
				index=index_name
				body={
					"mappings": {
						"properties": {
							"date": {"type", "keyword"},
							"timestamp": {"type", "keyword"},
							"hostname": {"type", "keyword"},
							"daemon": {"type", "keyword"},
							"severity": {"type", "keyword"},
							"message": {"type", "keyword"},
						}
					}
				}
			)

	def InferSeverity(self, message):
		severity = 7
		
		for key in keys:
			if key in message:
				if inferred_severity[key] < severity:
					severity = inferred_severity[key] 
		
		return severity_level[severity]

	"""Service for File Ingests. Accepts a stream (flow) of `LogString` messages.
    """
	def Ingest(self, request_iterator, context):
		def obtain_bulk(iterator):
			for request in iterator: # for every request streamed by the client
				# precheck if index exists (since indices are per-client)
				self.CreateIndex(f"{request.client}_index")

				# match the log messages via regex
				matches = pattern.finditer(request.message)

				#then for every match group (see global `syslog_pattern`) return/yield that for the helpers bulk method
				for match in matches:
					message = match.group(5)

					yield{
						"_index": request.client + "_index",
						"date": match.group(1),
						"timestamp": match.group(2),
						"hostname": match.group(3),
						"daemon": match.group(4),
						"severity": self.InferSeverity(message)
						"message": message
					}

		# ingest the logs in bulk to send N+ logs in one network message
		helpers.bulk(self.elastic_client, obtain_bulk(request_iterator))

		return mini_splunk_protobuf_pb2.RequestStatus(status=True)

	"""Service for Purging Logs. Accepts a `PurgeRequest` message to signal Server.
    """
	def Purge(self, request, context):
		pass

	"""Service for filtering logs based on Date criterion. Returns a stream of 0-n `LogString` messages.
	"""
	def SearchDate(self, request, context):
		res = self.elastic_client.search(
			index="*_index",
			size=100,
			query={
				"match": {
					"date": request.argument
				}
			},
			sort=[
				{"date": {"order": "asc"}},
				{"timestamp": {"order": "asc"}}
			]
		)

		while True:
			hits = res["hits"]["hits"]

			if not hits:
				break
			
			for hit in hits:
				log_fields = hit["_source"]
				log_string = f"{log_fields['date']} {log_fields['timestamp']} {log_fields['hostname']} {log_fields['process']}: {log_fields['message']}"
				yield mini_splunk_protobuf_pb2.LogString(
					client=request.client,
					message=log_string
				)

			res = self.elastic_client.search(
				index="*_index",
				size=100,
				query={
					"match": {
						"date": request.argument
					}
				},

				search_after=hits[-1]["sort"],

				sort=[
					{"date": {"order": "asc"}},
					{"timestamp": {"order": "asc"}}
				]
			)
		

	"""Service for filtering logs based on Hostname criterion. Returns a stream of 0-n `LogString` messages.
    """
	def SearchHost(self, request, context):
		pass

	"""Service for filtering logs based on Process criterion. Returns a stream of 0-n `LogString` messages.
    """
	def SearchDaemon(self, request, context):
		pass

	"""Service for filtering logs based on Severity criterion. Returns a stream of 0-n `LogString` messages.
    """
	def SearchSeverity(self, request, context):
		pass

	"""Service for filtering logs based on Keyword/s criterion. Returns a stream of 0-n `LogString` messages.
    """
	def SearchKeyword(self, request, context):
		pass

	"""Service for filtering logs based on Keyword/s criterion and accumulating matches. Returns `LogCount` match amount message.
    """
	def CountKeyword(self, request, context):
		pass

def main():
	# wildcard address automatically resolved by the container instance
	address = "0.0.0.0:50051"

	# each worker node uses 10 internal threads
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

	# instantiate a worker node object
	node = WorkerNode()

	# add the servicer (worker node object) to the gRPC server
	mini_splunk_protobuf_pb2_grpc.add_MiniSplunkServicer_to_server(node, server)
	server.add_insecure_port(address)
	server.start()

	try:
		print(f"Node Started on {address}")
		while True:
			# sleep to avoid wasting cpu cycles
			time.sleep(1000)
	except KeyboardInterrupt:
		print()
		return
	except grpc.RpcError as e:
		return

if __name__ == "__main__":
	main()