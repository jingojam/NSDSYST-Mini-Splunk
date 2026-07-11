import grpc
import mini_splunk_protobuf_pb2
import mini_splunk_protobuf_pb2_grpc
import os

def SendPing(stub, name):
    return stub.SendPing(mini_splunk_protobuf_pb2.Ping(sender=name))

def main():
    channel = grpc.insecure_channel("central_gateway:50050")
    stub = mini_splunk_protobuf_pb2.MiniSplunkStub(channel)
    client_name = os.getenv("NODE_NAME")
    
    try:
        while True:
            clin = input(f"{client_name}>")

            if clin == "ping":
                pong = SendPing(stub, client_name)

                if pong.receiver and pong.sender.sender == client_name:
                    print("[STATUS] Central Gateway is Active.")
                else:
                    print("[STATUS] Cannot Reach Central Gateway.")

    except KeyboardInterrupt:
        print(f"[STATUS] {client_name} Stopped.")
        return
    except grpc.RpcError as e:
        print(e)
        return

if __name__ == "__main__":
    main()