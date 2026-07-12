import grpc.aio
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
import os
from datetime import datetime
import asyncio

async def main():
    channel = grpc.aio.insecure_channel("central_gateway:50050")
    stub = mini_splunk_protobuf_pb2_grpc.MiniSplunkStub(channel)
    client_name = os.getenv("NODE_NAME")
    await asyncio.sleep(65)
        
    try:
        print("[STATUS] Central Gateway is Active.")

        logs = []
        batch = 0
        thousandth_batch = 0
        status = None
            
        print(datetime.now())
            
        with open("sample_logs/SVR1_server_auth_syslog.txt", "r") as file:
            for log in file:
                batch += 1 
                logs.append(log.strip())
                    
                if batch == 1000:
                    status = await stub.Ingest(
                        mini_splunk_protobuf_pb2.LogBatch(
                            batch_id = thousandth_batch,
                            client=client_name,
                            messages=logs
                        )
                    )
                        
                    logs = []
                    batch = 0
                    thousandth_batch += 1
            
        if logs:
            status = await stub.Ingest(
                mini_splunk_protobuf_pb2.LogBatch(
                    batch_id=thousandth_batch,
                    client=client_name,
                    messages=logs
                )
            )
            
        if status.batch_id != -1:
            print(f"[STATUS] Successfully Ingested sample_logs/CUDA_server_auth_syslog.txt")
            print(datetime.now())
            
        async for log_batch in stub.SearchDate(
            mini_splunk_protobuf_pb2.QueryRequest(
                client=client_name,
                argument="Feb 18"
            )
        ):
            # Unpack the array of string messages sent inside the batch
            for message in log_batch.messages:
                print(message)
            
    except grpc.RpcError as e:
        print("[STATUS] Cannot Reach Central Gateway.")
        print(e)
        return

if __name__ == "__main__":
    asyncio.run(main())