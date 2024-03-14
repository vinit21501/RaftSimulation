import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures

# initialisation
currentTerm = 0
votedFor = None
log = 'hi'
commitLength = 0
currentRole = 'follower'
currentLeader = None
votesReceived = {}
sentLength = 'hi'
ackedLength = 'h'

nodes = {}

# recovery from crash
currentRole = 'follower'
currentLeader = None
votesReceived = {}
sentLength = 'hi'
ackedLength = 'hi'

# node nodeId suspects leader has failed, or on election timeout
currentTerm = currentTerm + 1
currentRole = 'candidate'
votedFor = 'nodeId'
votesReceived = {'nodeId'}
lastTerm = 0
if log.length > 0 :
    lastTerm = log[log.length - 1].term
msg = ('VoteRequest', 'nodeId', currentTerm, log.length, lastTerm)
for node in nodes:
    # send msg to node
    pass
# start election timer

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def ServeClient(self, request, context):
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = raft_pb2_grpc.SellerStub(channel)
        stub.ServeClient(request)

if __name__ == "__main__":
    try:
        serve()
    except:
        print('Interrupt')
