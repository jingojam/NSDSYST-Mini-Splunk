import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from cassandra.cluster import Cluster

import os
import sys
import time
import re
from concurrent import futures

"""
	regex pattern for syslog (RFC 3164 BSD Syslog format)
		* all groups (index [0]):
			-> entire matched string

		* group 1 (index [1]) MONTH DAY:
			-> ^([a-zA-Z]{3}\s+\d{1,2}) -> matches 3 alphabetical characters (month), 1+ space, 2 digits (day)
		
		* group 2 (index [2]) Timestamp:
			-> (\d{2}:\d{2}:\d{2}) -> matches 2 digits (hour) : 2 digits (minute) : 2 digits (second)
		
		* group 3 (index [3]) Hostname:
			-> ([\w\-._]+) -> matches 1+ alphanumeric characters, including dash (-), dot (.), and underscore (_)

		* group 4 (index [4]) Daemon (and optional PID):
			-> ([\w-]+(?:\[\d+\])?) -> matches 1+ alphanumeric characters, and optional 1+ integer PID enclosed by [ ]

		* group 5 (index [5]) Message:
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
		# main cassandra cluster, with the cassandra seed node as contact point
		self.cassandra_cluster = Cluster(
			["cassandra_seed"],
			port=9402
		)

		# connect to the cluster
		self.cassandra_session = cassandra_cluster.connect()

    """Service for File Ingests. Accepts a stream (flow) of `LogString` messages.
    """
	def Ingest(self, request_iterator, context):
		pass

    """Service for Purging Logs. Accepts a `PurgeRequest` message to signal Server.
    """
	def Purge(self, request, context):
		pass

    """Service for filtering logs based on Date criterion. Returns `LogResults` containing 0-n `LogString` messages.
    """
	def SearchDate(self, request, context):
		# create a prepared statement for querying cassandra db with (date) column as criterion based on input
		prepared_statement = self.session.prepare(
			"SELECT * FROM syslog_keyspace.syslogs WHERE date = ?"
		)
		
		# automatically bind the request argument with the prepared statement and then execute the db query
		rows = self.session.execute(prepared_statement, [request.argument])

		entries = []

		# for every row (syslog line) in the returned rows, create a `LogString` from protobuf with the row line as message
		#  since protobuf specifies this service returns `LogResults` which contains repeated `LogString` instances
		for row in rows:
			entries.append(
				mini_splunk_protobuf_pb2.LogString(
					message=row
				)
			)

		# return a `LogResults` response with the entries as argument
		return mini_splunk_protobuf_pb2.LogResults(
			log_entries=entries
		)

    """Service for filtering logs based on Hostname criterion. Returns `LogResults` containing 0-n `LogString` messages.
    """
   	def SearchHost(self, request, context):
		# create a prepared statement for querying cassandra db with (hostname) column as criterion based on input
		prepared_statement = self.session.prepare(
			"SELECT * FROM syslog_keyspace.syslogs WHERE hostname = ?"
		)
		
		# automatically bind the request argument with the prepared statement and then execute the db query
		rows = self.session.execute(prepared_statement, [request.argument])

		entries = []

		# for every row (syslog line) in the returned rows, create a `LogString` from protobuf with the row line as message
		#  since protobuf specifies this service returns `LogResults` which contains repeated `LogString` instances
		for row in rows:
			entries.append(
				mini_splunk_protobuf_pb2.LogString(
					message=row
				)
			)

		# return a `LogResults` response with the entries as argument
		return mini_splunk_protobuf_pb2.LogResults(
			log_entries=entries
		)

    """Service for filtering logs based on Process criterion. Returns `LogResults` containing 0-n `LogString` messages.
    """
    def SearchDaemon(self, request, context):
		# create a prepared statement for querying cassandra db with (daemon) column as criterion based on input
		prepared_statement = self.session.prepare(
			"SELECT * FROM syslog_keyspace.syslogs WHERE daemon = ?"
		)
		
		# automatically bind the request argument with the prepared statement and then execute the db query
		rows = self.session.execute(prepared_statement, [request.argument])

		entries = []

		# for every row (syslog line) in the returned rows, create a `LogString` from protobuf with the row line as message
		#  since protobuf specifies this service returns `LogResults` which contains repeated `LogString` instances
		for row in rows:
			entries.append(
				mini_splunk_protobuf_pb2.LogString(
					message=row
				)
			)

		# return a `LogResults` response with the entries as argument
		return mini_splunk_protobuf_pb2.LogResults(
			log_entries=entries
		)

    """Service for filtering logs based on Severity criterion. Returns `LogResults` containing 0-n `LogString` messages.
    """
    def SearchSeverity(self, request, context):
		# create a prepared statement for querying cassandra db with (severity) column as criterion based on input
		prepared_statement = self.session.prepare(
			"SELECT * FROM syslog_keyspace.syslogs WHERE severity = ?"
		)
		
		# automatically bind the request argument with the prepared statement and then execute the db query
		rows = self.session.execute(prepared_statement, [request.argument])

		entries = []

		# for every row (syslog line) in the returned rows, create a `LogString` from protobuf with the row line as message
		#  since protobuf specifies this service returns `LogResults` which contains repeated `LogString` instances
		for row in rows:
			entries.append(
				mini_splunk_protobuf_pb2.LogString(
					message=row
				)
			)

		# return a `LogResults` response with the entries as argument
		return mini_splunk_protobuf_pb2.LogResults(
			log_entries=entries
		)

    """Service for filtering logs based on Keyword/s criterion. Returns `LogResults` containing 0-n `LogString` messages.
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