import grpc.aio
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

import asyncio
import os
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
        self.max_batch = 1000
        self.node_name = os.getenv("NODE_NAME")

        # connect to elasticsearch cluster
        self.elastic_client = AsyncElasticsearch(
            hosts=[
                "http://elastic_node_0:9200",
                "http://elastic_node_1:9200",
                "http://elastic_node_2:9200",
            ]
        )
        
    async def ReachCluster(self):
        print(f"[STATUS] Worker Node {self.node_name} Connecting To ElasticSearch Cluster...")
        
        status = False
        
        for i in range(50):
            status = await self.elastic_client.ping()

            if status:
                break

            await asyncio.sleep(10)
            
        if not status:
            print(f"[STATUS] Worker Node {self.node_name} Cannot Reach Elasticsearch Cluster.")
        else:
            print(f"[STATUS] Worker Node {self.node_name} Connected to Elasticsearch Cluster.")

    """Internal (unexposed) method to create an index if it doesn't already exist
    """
    async def CreateIndex(self, index_name):
        # check if there is an index
        if not await self.elastic_client.indices.exists(index=index_name):
            # create index
            await self.elastic_client.indices.create(
                index=index_name,
                body={
                    "mappings": {
                        "properties": { # main fields (syslog)
                            "id": {"type": "integer"},
                            "date": {"type": "keyword"},
                            "timestamp": {"type": "keyword"},
                            "hostname": {"type": "keyword"},
                            "daemon": {"type": "keyword"},
                            "severity": {"type": "keyword"},
                            "message": {"type": "keyword"},
                        }
                    }
                }
            )

    """Internal method for inferring the severity of a log
       by mapping common words to severity levels
    """
    # THIS HAS TO BE OPTIMIZED
    def InferSeverity(self, message):
        #initial severity is lowest=7 (debug)
        severity = 7
        
        # for every key (word) mapped
        for key in keys:
            # check if the message has that keyword
            if key in message:
                # update severity level if it's higher (lower index)
                if inferred_severity[key] < severity:
                    severity = inferred_severity[key] 
        
        return severity_level[severity]

    """Service for File Ingests. Accepts a stream (flow) of `LogString` messages.
    """
    async def Ingest(self, request, context):
        print(f"[{self.node_name}] Batch Ingest Received.")
        # precheck if index exists (since indices are per-client)
        await self.CreateIndex(f"{request.client}_index")
        
        def obtain_bulk():
            for message in request.messages: # for every request streamed by the client
                # match the log messages via regex
                matches = syslog_pattern.finditer(message)
                id = 0
                
                #then for every match group (see global `syslog_pattern`) return/yield that for the helpers bulk method
                for match in matches:
                    message_field = match.group(5)

                    yield {
                        "_index": request.client + "_index",
                        "id": id,
                        "date": match.group(1),
                        "timestamp": match.group(2),
                        "hostname": match.group(3),
                        "daemon": match.group(4),
                        "severity": self.InferSeverity(message_field), #infer the severity (standard syslog files doesn't have severity)
                        "message": message_field
                    }
                    id += 1

        # ingest the logs in bulk in one network message
        await async_bulk(self.elastic_client, obtain_bulk())

        return mini_splunk_protobuf_pb2.IngestStatus(batch_id=request.batch_id)

    """Service for Purging Logs. Accepts a `PurgeRequest` message to signal Server.
    """
    async def Purge(self, request, context):
        pass 

    """Internal method for querying elasticsearch cluster via PIT (point in time) for search windows/paginated results
        based on search criterion (key; e.g., "date", "hostname", etc.)
    """
    async def Query(self, request, criterion):
        search_after = None
        
        #according to the docs, using pit is ideal for querying/deep pagination as logs stored grow larger
        res = await self.elastic_client.open_point_in_time(
            index="*_index", # `*` acts as a wildcard so the elastic search cluster would perform scatter-gather query on all indices and aggregate it back to the worker
            keep_alive="1m", #expiration timer for the pit instance
        )

        # get the point in time id for subsequent searches
        pit_id = res["id"]

        try:
            #re send the query over and over again for each page
            while True:
                search_arguments = {
                    "size": self.max_batch, # amount of hits max
                    "query": {
                        "term": { #matches all logs based on specified argument as criterion in field (e.g., "sshd" in `daemon` field)
                            criterion: request.argument.strip()
                        }
                    },
                    "pit": { # point in time with the previous pit id (same timer config)
                        "id": pit_id,
                        "keep_alive": "1m" 
                    },
                    "sort": [
                        {"id": {"order": "asc"}}, # sort results based on id field, it is incremented per ingest time so order is preserved
                        {"_doc": "asc"} # tie breaker for sort is the internal _doc index created by elasticsearch
                    ]
                }
                
                # only if the query result has a search_after field, then use that as an extra argument for windowing
                if search_after:
                    search_arguments["search_after"] = search_after
                
                #execute the search with the parameters defined above
                response = await self.elastic_client.search(**search_arguments)

                #get results and the pit id returned
                hits = response["hits"]["hits"] # actual log results
                pit_id = response["pit_id"]

                # if there are no results then send, then it's done
                if not hits:
                    break

                log_batch = []
                # for every matched log result
                for hit in hits:
                    # get the log fields
                    log_fields = hit["_source"]
                    log_batch.append(f"{log_fields['date']} {log_fields['timestamp']} {log_fields['hostname']} {log_fields['daemon']}: {log_fields['message']}")
                            
                        
                # if any remaining log batch, send it (fallback return if there are hits but less than defined `max_batch` class attribute)
                if log_batch:
                    yield mini_splunk_protobuf_pb2.LogBatch(
                        client=request.client,
                        batch_id=0,
                        messages=log_batch
                    )

                # then get the search_after id from the last hit (-1 index) result
                search_after = hits[-1]["sort"]
        finally:
            await self.elastic_client.close_point_in_time(body={"id": pit_id})

    """Service for filtering logs based on Date criterion. Returns a stream of 0-n `LogString` messages.
    """
    async def SearchDate(self, request, context):
        async for log_batch in self.Query(request, "date"):
            yield log_batch
        
    """Service for filtering logs based on Hostname criterion. Returns a stream of 0-n `LogString` messages.
    """
    async def SearchHost(self, request, context):
        async for log_batch in self.Query(request, "hostname"):
            yield log_batch

    """Service for filtering logs based on Process criterion. Returns a stream of 0-n `LogString` messages.
    """
    async def SearchDaemon(self, request, context):
        async for log_batch in self.Query(request, "daemon"):
            yield log_batch

    """Service for filtering logs based on Severity criterion. Returns a stream of 0-n `LogString` messages.
    """
    async def SearchSeverity(self, request, context):
        async for log_batch in self.Query(request, "severity"):
            yield log_batch

    """Service for filtering logs based on Keyword/s criterion. Returns a stream of 0-n `LogString` messages.
    """
    async def SearchKeyword(self, request, context):
         pass

    """Service for filtering logs based on Keyword/s criterion and accumulating matches. Returns `LogCount` match amount message.
    """
    async def CountKeyword(self, request, context):
        pass

async def main():
    # wildcard address automatically resolved by the container instance
    address = "0.0.0.0:50051"

    # each worker node uses 10 internal threads
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

    # instantiate a worker node object
    node = WorkerNode()
    
    await node.ReachCluster()

    # add the servicer (worker node object) to the gRPC server
    mini_splunk_protobuf_pb2_grpc.add_MiniSplunkServicer_to_server(node, server)
    server.add_insecure_port(address)
    
    await server.start()
    
    try:
        print(f"{node.node_name} Started on {address}")
        await server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    except grpc.RpcError as e:
        pass
    finally:
        print(f"[STATUS] Shutting Down {node.node_name}...")
        await server.stop(5)

if __name__ == "__main__":
    asyncio.run(main())