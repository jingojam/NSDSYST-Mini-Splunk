import grpc.aio
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc

import hashlib
import asyncio
import os
import time
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
        self.retransmit_buffer = {}
        
    async def InitializeChannels(self):
        print(f"[STATUS] Initializing Connection to Worker Nodes...", flush=True)
        
        try:
            for address in self.worker_node_addresses:
                
                # create channels to every worker node
                channel = grpc.aio.insecure_channel(address)
                self.worker_nodes[address] = {
                    "channel": channel,
                    "stub": mini_splunk_protobuf_pb2_grpc.MiniSplunkStub(channel),
                }

                print(f"[STATUS] Worker Node Active on {address}", flush=True)
        except grpc.RpcError as e:
                print(f"[STATUS] Unable to Reach Worker Node on {address}", flush=True)
                print(e)

    # updates current worker node to next node
    def Next(self):
        # update the last worker node to next (cycles back to 0 --the first worker node)
        self.current_worker_node = (self.current_worker_node + 1) % self.worker_node_count
        
    
    async def Ingest(self, request, context):            
        res = await self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].Ingest(request)
        self.Next()
        return res

    """Service for Purging Logs. Accepts a `PurgeRequest` message to signal Server. """
    def Purge(self, request, context):
        pass

    """Service for filtering logs based on Date criterion. Returns a stream of 0-n `LogString` messages. """
    async def SearchDate(self, request, context):
        self.Next()
        try:
            async for log_batch in self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].SearchDate(request):
                yield log_batch
        except grpc.RpcError as e:
            print(e)
            yield mini_splunk_protobuf_pb2.LogBatch(
                client=request.client,
                batch_id=-1,
                messages=[]
            )

    """Service for filtering logs based on Hostname criterion. Returns a stream of 0-n `LogString` messages. """
    async def SearchHost(self, request, context):
        self.Next()
        try:
            async for log_batch in self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].SearchHost(request):
                yield log_batch
        except grpc.RpcError as e:
            print(e)
            yield mini_splunk_protobuf_pb2.LogBatch(
                client=request.client,
                batch_id=-1,
                messages=[]
            )

    """Service for filtering logs based on Process criterion. Returns a stream of 0-n `LogString` messages. """
    async def SearchDaemon(self, request, context):
        self.Next()
        try:
            async for log_batch in self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].SearchDaemon(request):
                yield log_batch
        except grpc.RpcError as e:
            print(e)
            yield mini_splunk_protobuf_pb2.LogBatch(
                client=request.client,
                batch_id=-1,
                messages=[]
            )

    """Service for filtering logs based on Severity criterion. Returns a stream of 0-n `LogString` messages. """
    async def SearchSeverity(self, request, context):
        self.Next()
        try:
            stream = self.worker_nodes[self.worker_node_addresses[self.current_worker_node]]["stub"].SearchSeverity(request)
            async for log_batch in stream:
                yield log_batch
        except grpc.RpcError as e:
            print(e)
            yield mini_splunk_protobuf_pb2.LogBatch(
                client=request.client,
                batch_id=-1,
                messages=[]
            )

    """Service for filtering logs based on Keyword/s criterion. Returns a stream of 0-n `LogString` messages. """
    async def SearchKeyword(self, request, context):
        pass

    """Service for filtering logs based on Keyword/s criterion and accumulating matches. Returns `LogCount` match amount message. """
    async def CountKeyword(self, request, context):
        pass

async def main():
    address = "0.0.0.0:50050"
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    # addresses of the worker nodes
    worker_addresses = [
        "worker_node_0:50051", # using the docker service name automatically resolves to the node's IP
        "worker_node_1:50051",
        "worker_node_2:50051",
        "worker_node_3:50051",
        "worker_node_4:50051",
    ]
    node = CentralNode(worker_addresses)
    await asyncio.sleep(30)
    await node.InitializeChannels()
    
    mini_splunk_protobuf_pb2_grpc.add_MiniSplunkServicer_to_server(node, server)
    server.add_insecure_port(address)
    
    await server.start()
    
    try:
        print(f"Central Node Started on {address}")
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"[STATUS] Shutting Down Central Gateway Server...")
        await server.stop(5)
        return
    except grpc.RpcError as e:
        return

if __name__ == "__main__":
    asyncio.run(main())