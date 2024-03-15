import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures
import random

random.seed(0)

localIP = 'localhost'
clusterIp = ['10.0.0.1', '10.0.0.2']
senderPort = '50051'
recieverPort = '50052'
nodeTotal = 5

# initialisation
currentTerm = 0
votedFor = None
log = 'NO-OP'
commitLength = 0
currentRole = 'follower'
currentLeader = None
votesReceived = {}
sentLength = 'hi'
ackedLength = 'h'

nodes = {}

def writeRequest(req):
    with open('log.txt', 'a') as file:
        file.write(req + '\n')

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
    def voteFor(self, request, context):
        # if votedFor == None:

        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port("[::]:" + recieverPort)
    server.start()
    server.wait_for_termination()

class RequestFollower():
    def __init__(self, nodeCount, ips):
        self.channelList = list(map(lambda ip : grpc.insecure_channel(ip + ':' + senderPort), ips))
        self.stubList = list(map(lambda ch : raft_pb2_grpc.RaftStub(ch), self.channelList))
    def requestVote(self):
        votes = []
        for stub in self.stubList:
            req = raft_pb2.VoteForRequest()
            req.ip = localIP
            req.term = currentTerm
            # if node is dead, then how we handle the situation
            votes.append(stub.voteFor(req))
        voteTotal = len(list(filter(lambda vote : vote.success, votes)))
        return voteTotal


def run():
    with grpc.insecure_channel('localhost:' + senderPort) as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        stub.ServeClient(request)

if __name__ == "__main__":
    pass
    # electionTimeOut = random.uniform(5, 10)
    # try:
    #     serve()
    # except:
    #     print('Interrupt')
