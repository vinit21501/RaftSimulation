import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures
import random
import time
import os
import threading

random.seed(0)

localIP = 'localhost'
clusterIp = ['10.0.0.1', '10.0.0.2']
senderPort = '50051'
recieverPort = '50052'
nodeTotal = 5
node = 1
currentTerm = 0
votedFor = None
currentRole = 'follower'
currentLeader = None
logDic = {}

# initialisation
log = 'NO-OP'
commitLength = 0

currentLeader = None
votesReceived = {}
sentLength = 'hi'
ackedLength = 'h'

nodes = {}

# recovery from crash

votesReceived = {}
sentLength = 'hi'
ackedLength = 'hi'

# node nodeId suspects leader has failed, or on election timeout
currentTerm = currentTerm + 1
currentRole = 'candidate'
votedFor = 'nodeId'
votesReceived = {'nodeId'}
lastTerm = 0
# if log.length > 0 :
#     lastTerm = log[log.length - 1].term
# msg = ('VoteRequest', 'nodeId', currentTerm, log.length, lastTerm)
# for node in nodes:
#     # send msg to node
#     pass
# # start election timer

def callrpc(rpc, req):
    try:
        rep = rpc(req, timeout=1)
        return rep
    except:
        # dump node is not alive
        return None

def writeFile(fileN, req):
    with open(f'logs_node_{node}/{fileN}.txt', 'a') as file:
        file.write(req + '\n')

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def ServeClient(self, request, context):
        pass
    def voteFor(self, request, context):
        rep = raft_pb2.SuccessReply(False)
        if votedFor == None:
            if request.term > currentTerm:
                votedFor = request.ip
                cluster.election.cancel()
                rep.success = True
                return rep
            else:
                return rep
        else:
            return rep
    def RecieveAck(self, request, context):
        currentLeader = request.ip
        currentRole = 'follower'
        rep = raft_pb2.SuccessReply(True)
        return rep
    def HeartBeat(self, request, context):
        request.leasetime
        rep = raft_pb2.SuccessReply(True)
        return rep
    def CommitEntry(self, request, context):
        # request.leasetime
        request.term
        var = request.var
        val = request.val
        logDic[var] = val
        log = f'SET {var} {val} {currentTerm}'
        rep = raft_pb2.SuccessReply(True)
        return rep
    def AppendEntry(self, request, context):
        rep = raft_pb2.SuccessReply(True)
        if not log:
            writeFile('log', log)
            log = None
            return rep
        rep.success = False
        return False

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port("[::]:" + recieverPort)
    server.start()
    server.wait_for_termination()

class RequestFollower():
    def __init__(self, nodeCount, ips):
        self.channelList = list(map(lambda ip : grpc.insecure_channel(ip + ':' + senderPort, options=[('grpc.default_timeout_ms', 1000)]), ips))
        self.stubList = list(map(lambda ch : raft_pb2_grpc.RaftStub(ch), self.channelList))
    def requestVote(self):
        currentTerm += 1
        currentRole = 'candidate'
        votes = [True]
        for stub in self.stubList:
            req = raft_pb2.VoteForRequest()
            req.ip = localIP
            req.term = currentTerm
            # if node is dead, then how we handle the situation
            # handled
            rep = callrpc(stub.voteFor, req)
            if rep != None:
                votes.append(rep)
        voteTotal = len(list(filter(lambda vote : vote.success, votes)))
        if voteTotal >= nodeTotal / 2 + 1:
            ack = []
            for stub in self.stubList:
                rep = callrpc(stub.RecieveAck, req)
                if rep != None:
                    ack.append(rep)
            ackTotal = len(list(filter(lambda ak : ak.success, ack)))
            if ackTotal >= nodeTotal / 2 + 1:
                # sending heartbeast & renewing lease
                currentLeader = 'self'
                currentRole = 'leader'
                self.heartbeatTimeout()
                self.leaderWorking()
            else:
                self.electionTimeout()
        else:
            self.electionTimeout()
    def electionTimeout(self):
        electiontime = random.uniform(5, 10)
        self.election = threading.Timer(electiontime, self.requestVote)
        self.election.start()
    def resetelection(self):
        self.election.cancel()
        self.electionTimeout()
    def heartbeatTimeout(self):
        heartbeatTime = random.uniform(0.9, 1.1)
        self.beattime = threading.Timer(heartbeatTime, self.heartbeat)
        self.beattime.start()
    def resetheartbeat(self):
        self.beattime.cancel()
        self.heartbeatTimeout()
    def heartbeat(self):
        req = raft_pb2.heartbeatRequest()
        ack = []
        for stub in self.stubList:
            rep = callrpc(stub.HeartBeat, req)
            if rep != None:
                ack.append(rep)
        ackTotal = len(list(filter(lambda ak : ak.success, ack)))
        if ackTotal >= nodeTotal / 2 + 1:
            # sending heartbeast & renewing lease
            self.heartbeatTimeout()
        else:
            # renewal failed & stepping down
            self.electionTimeout()
    def leaderWorking(self):
        while True:
            print("1. SET")
            print("2. GET")
            opt = int(input("Enter your choice: "))
            if opt == 1:
                var = input("Enter your variable: ")
                val = input("Enter value: ")
                self.appendEntry(var, val)
            elif opt == 2:
                var = input("Enter your variable: ")
                if var in logDic:
                    print(f'value of {var} : {logDic[var]}')
                else:
                    print(f'{var} is not intialized')
            else:
                print('invalid option')
        pass
    def appendEntry(self, var, val):
        req = raft_pb2.receiveVarRequest()
        # req.leasetime = 
        req.term = currentTerm
        req.var = var
        req.val = val
        ack = []
        for stub in self.stubList:
            rep = callrpc(stub.CommitEntry, req)
            if rep != None:
                ack.append(rep)
        ackTotal = len(list(filter(lambda ak : ak.success, ack)))
        if ackTotal >= nodeTotal / 2 + 1:
            logDic[var] = val
            log = f'SET {var} {val} {currentTerm}'
            writeFile('log', log)
            req = raft_pb2.heartbeatRequest()
            for stub in self.stubList:
                callrpc(stub.AppendEntry, req)
            # sending heartbeast & renewing lease
            self.resetheartbeat()
        else:
            # commit failed & i dont know
            pass

def runNode():
    cluster.electionTimeout()

if __name__ == "__main__":
    cluster = RequestFollower(nodeTotal, clusterIp)
    if os.path.exists(f'logs_node_{node}'):
        with open(f'logs_node_{node}/log.txt') as file:
            file.readline()
            for line in file.readlines():
                if line.startswith('SET'):
                    line = line.strip().split()
                    logDic[line[1]] = line[2]
    else:
        os.mkdir(f'logs_node_{node}')
        writeFile('log', 'NO-OP')
        serving = threading.Thread(target=serve(), daemon=True)
        serving.start()
        run = threading.Thread(target=runNode(), daemon=True)
        run.start()
        run.join()
        serving.join()

        # writeFile('dump', 'NO-OP')
        # writeFile('metadata', 'NO-OP')
    # electionTimeOut = random.uniform(5, 10)
    # try:
    #     serve()
    # except:
    #     print('Interrupt')
