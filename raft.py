import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures
import random
import os
import threading

# clear && python raft.py
# nodeId = 2
# recieverPort = '50052'
# clusterIp = {1:'localhost:50051', 3:'localhost:50053'}

# nodeId = 3
# recieverPort = '50053'
# clusterIp = {1:'localhost:50051', 2:'localhost:50052'}

nodeId = 1
recieverPort = '50051'
clusterIp = {2:'localhost:50052', 3:'localhost:50053'}

localIP = 'localhost'
nodeTotal = 3
leaseTime = 2.5

# currentTerm = 0
# votedFor = None

class State():
    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.currentTerm = 0
        self.votedFor = None
        self.logs = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.resetIndex()
        self.currentlogTerm = 0
        self.logsDic = {}
        self.currentRole = 'follower'
        self.currentLeader = None
        if os.path.exists(f'logs_node_{self.nodeId}/metadata.txt'):
            with open(f'logs_node_{self.nodeId}/metadata.txt') as file:
                for line in file.readlines():
                    line = line.strip(':').split()
                    if line[0] == 'nodeId':
                        self.nodeId = line[1]
                    elif line[0] == 'currentTerm':
                        self.currentTerm = line[1]
                    elif line[0] == 'commitIndex':
                        self.commitIndex = line[1]
                    elif line[0] == 'lastApplied':
                        self.lastApplied = line[1]
        else:
            os.mkdir(f'logs_node_{self.nodeId}')
            open(f'logs_node_{self.nodeId}/metadata.txt', 'w').close()
        if not os.path.exists(f'logs_node_{self.nodeId}/dump.txt'):
            open(f'logs_node_{self.nodeId}/dump.txt', 'w').close()
        self.readLogs()
        self.export()
    def resetIndex(self):
        self.nextIndex = {i:self.commitIndex for i in range(cluster.totalNode)}
        self.matchIndex = {i:self.commitIndex for i in range(cluster.totalNode)}
    def export(self):
        with open(f'logs_node_{self.nodeId}/metadata.txt', 'w') as file:
            file.write(f'nodeId:{self.nodeId}\n')
            file.write(f'currentTerm:{self.currentTerm}\n')
            file.write(f'commitIndex:{self.commitIndex}\n')
            file.write(f'lastApplied:{self.lastApplied}\n')
    def readLogs(self):
        if os.path.exists(f'logs_node_{self.nodeId}/log.txt'):
            with open(f'logs_node_{self.nodeId}/log.txt') as file:
                file.readline()
                for line in file.readlines():
                    if line.startswith('SET'):
                        line = line.strip().split()
                        self.logDic[line[1]] = line[2]
        else:
            # os.mkdir(f'logs_node_{self.nodeId}')
            open(f'logs_node_{self.nodeId}/log.txt', 'w').close()
    def appendEntries(self, log=None):
        with open(f'logs_node_{self.nodeId}/log.txt', 'a') as file:
            # for entry in self.logs:
            if log != None:
                file.write(log + '\n')
            else:
                for entry in self.logs:
                    file.write(entry[1] + '\n')
        self.export()
    def writeDump(self, query):
        with open(f'logs_node_{self.nodeId}/dump.txt', 'a') as file:
            file.write(query + '\n')
    def commitEntries(self, leaderCommit):
        if leaderCommit > self.commitIndex:
            for log in self.logs:
                self.writeDump(f'Node {self.nodeId} ({self.currentRole}) committed the entry ({log}) to the state machine.')
                if log.startswith('SET'):
                    log = log.strip().split()
                    self.logDic[log[1]] = log[2]
            self.logs = []
            self.export()
            return True
        self.export()
        return False
    def leaderCommitEntries(self):
        for log in self.logs[self.commitIndex:]:
            self.writeDump(f'Node {self.nodeId} ({self.currentRole}) committed the entry ({log}) to the state machine.')
            if log.startswith('SET'):
                log = log.strip().split()
                self.logDic[log[1]] = log[2]
        self.export()

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def AppendEntries(self, request, context):
        rep = raft_pb2.AppendReply()
        rep.term = stateMachine.currentTerm
        # if request.prevLogIndex  ==  stateMachine.lastApplied and request.prevLogTerm == stateMachine.currentlogTerm:
        print(request.term, stateMachine.currentTerm, request.prevLogTerm, stateMachine.currentlogTerm, request.prevLogIndex, stateMachine.lastApplied)
        if request.term >= stateMachine.currentTerm and request.prevLogTerm >= stateMachine.currentlogTerm and request.prevLogIndex == stateMachine.lastApplied - 1:
            stateMachine.currentTerm = request.term
            rep.term = stateMachine.currentTerm
            stateMachine.currentLeader = request.leaderId
            # request.leaderId
            # request.leaderCommit
            stateMachine.commitEntries(request.leaderCommit)
            stateMachine.commitIndex = request.leaderCommit
            stateMachine.logs = list((stateMachine.lastApplied + 1 + i, log, int(log.split()[-1])) for i, log in enumerate(request.entries))
            stateMachine.appendEntries()
            stateMachine.lastApplied += len(request.entries)
            cluster.resetheartbeat()
            rep.success = True
            return rep
        cluster.resetheartbeat()
        rep.success = False
        return rep
    def RequestVote(self, request, context):
        rep = raft_pb2.RequestVoteReply()
        rep.term = stateMachine.currentTerm
        if stateMachine.votedFor == None and stateMachine.currentRole != 'candidate':
            print(request.term, stateMachine.currentTerm, stateMachine.lastApplied, request.lastLogIndex, stateMachine.currentlogTerm, request.lastLogTerm)
            if request.term >= stateMachine.currentTerm and stateMachine.lastApplied <= request.lastLogIndex and stateMachine.currentlogTerm <= request.lastLogTerm:
                stateMachine.votedFor = request.candidateId
                rep.voteGranted = True
                # currentLeader = request.candidateId
                # currentRole = 'follower'
                # cancel the election or restart
                cluster.resetelection()
                print("voted for", request.candidateId)
                return rep
            # handle this parameter for vote granted
            # request.lastLogIndex
            # request.lastlogTerm
        rep.voteGranted = False
        return rep

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port("[::]:" + recieverPort)
    server.start()
    server.wait_for_termination()

class NodeWorking():
    def __init__(self, nodeCount, ips, leaseTime):
        self.totalNode = nodeCount
        self.leaseTime = leaseTime
        self.beattime = threading.Timer(1, self.appendEntry)
        self.leaseThread = threading.Timer(self.leaseTime, self.stepDown)
        # self.channelList = list(map(lambda ip : grpc.insecure_channel(ip + ':' + senderPort, options=[('grpc.default_timeout_ms', 1000)]), ips))
        self.channelList = dict(zip(ips.keys(), map(lambda ip : grpc.insecure_channel(ips[ip], options=[('grpc.default_timeout_ms', 1000)]), ips)))
        self.stubList = dict(zip(ips.keys(), map(lambda ch : raft_pb2_grpc.RaftStub(self.channelList[ch]), self.channelList)))
    def requestVote(self):
        stateMachine.writeDump(f'Node {stateMachine.nodeId} election timer timed out, Starting election.')
        # currentTerm += 1
        # currentRole = 'candidate'
        stateMachine.currentTerm += 1
        stateMachine.currentRole = 'candidate'
        stateMachine.currentLeader = None

        votes = []
        def parallelVoting(arg):
            req = raft_pb2.RequestVoteRequest()
            req.candidateId = stateMachine.nodeId
            req.term = stateMachine.currentTerm
            req.lastLogIndex = stateMachine.commitIndex
            print(stateMachine.logs)
            print(stateMachine.commitIndex)
            if len(stateMachine.logs) >= 1:
                print(stateMachine.logs[stateMachine.commitIndex - 1])
            req.lastLogTerm = stateMachine.logs[stateMachine.commitIndex - 1][2] if len(stateMachine.logs) >= 1 else 0
            rep = self.exceptioncallrpc(self.stubList[arg].RequestVote, req, arg)
            return rep, arg
        # print('asking for vote')
        with futures.ThreadPoolExecutor(max_workers = 5) as executor:
            results = executor.map(parallelVoting, self.stubList.keys())

        for res in results:
            # if node is dead, then how we handle the situation
            # handled
            
            if res[0] != None:
                if res[0].voteGranted:
                    stateMachine.writeDump(f'Vote granted for Node {res[1]} in term {stateMachine.currentTerm}.')
                else:
                    stateMachine.writeDump(f'Vote denied for Node {res[1]} in term {stateMachine.currentTerm}.')
                votes.append(res[0])
        voteTotal = sum(vote.voteGranted for vote in votes) + 1
        print('voted granted', voteTotal)
        if voteTotal > self.totalNode // 2 and stateMachine.currentLeader == None:
            stateMachine.currentLeader = stateMachine.nodeId
            stateMachine.currentRole = 'leader'
            stateMachine.writeDump(f'New Leader waiting for Old Leader Lease to timeout.')
            self.waitingOlderLeaseTime()
            # ack = []
            # for stub in self.stubList:
            #     rep = self.exceptioncallrpc(stub.RecieveAck, req)
            #     if rep != None:
            #         ack.append(rep)
            # ackTotal = len(list(filter(lambda ak : ak.success, ack)))
            # if ackTotal >= nodeTotal / 2 + 1:
                # sending heartbeast & renewing lease
                # currentLeader = 'self'
                # currentRole = 'leader'
                # stateMachine.logs = [f'NO-OP {stateMachine.currentTerm}']
            # else:
            #     self.electionTimeout()
        else:
            self.electionTimeout()
    def afterOlderLeaseTime(self):
        if stateMachine.currentRole == 'leader':
            stateMachine.writeDump(f'Node {stateMachine.nodeId} became the leader for term {stateMachine.currentTerm}.')
            stateMachine.resetIndex()
            stateMachine.currentlogTerm = stateMachine.currentTerm
            self.appendEntry(log=f'NO-OP {stateMachine.currentTerm}')
            # self.heartbeat()
            self.leaderWorking()
        else:
            self.resetelection()
    def waitingOlderLeaseTime(self):
        self.waiting = threading.Timer(self.leaseTime, self.afterOlderLeaseTime)
        self.waiting.start()
    def electionTimeout(self):
        electiontime = random.uniform(5, 10)
        print('election Time', electiontime)
        self.election = threading.Timer(electiontime, self.requestVote)
        self.election.start()
    def resetelection(self):
        self.election.cancel()
        self.electionTimeout()
    def heartbeatTimeout(self):
        heartbeatTime = random.uniform(0.9, 1.1)
        self.beattime = threading.Timer(heartbeatTime, self.appendEntry)
        self.beattime.start()
    def resetheartbeat(self):
        self.beattime.cancel()
        self.heartbeatTimeout()
    # def heartbeat(self):
        
    #     req = raft_pb2.AppendRequest()
    #     req.term = stateMachine.currentTerm
    #     req.leaderId = stateMachine.nodeId
    #     req.prevLogIndex = stateMachine.lastApplied
    #     req.entries.extend(stateMachine.logs)
    #     req.prevLogTerm = stateMachine.currentlogTerm
    #     req.leaderCommit = stateMachine.lastApplied
    #     ack = []
    #     for stub in self.stubList:
    #         rep = self.exceptioncallrpc(self.stubList[stub].AppendEntries, req, stub)
    #         if rep != None:
    #             ack.append(rep)
    #     ackTotal = len(list(filter(lambda ak : ak.success, ack)))
    #     # print('heartbeat', ackTotal)
    #     if ackTotal > self.nodeTotal // 2:
    #         # resetLeaseTime if not implemented
    #         self.resetLeaseTime()
    #         self.resetheartbeat()
    #     else:
    #         self.resetheartbeat()
    #         # commit failed & i dont know
    #         # pass
    def leaderWorking(self):
        pass
    #     while True:
    #         print("1. SET")
    #         print("2. GET")
    #         opt = int(input("Enter your choice: "))
    #         if opt == 1 and stateMachine.currentRole == 'leader':
    #             var = input("Enter your variable: ")
    #             val = input("Enter value: ")
    #             # stateMachine.logs = [f'SET {var} {val} {stateMachine.currentTerm}']
    #             self.appendEntry(var, val)
    #         elif opt == 2:
    #             var = input("Enter your variable: ")
    #             if var in stateMachine.logDic:
    #                 print(f'value of {var} : {stateMachine.logDic[var]}')
    #             else:
    #                 print(f'{var} is not intialized')
    #         else:
    #             print('current role is not suited for this option')
    def appendEntry(self, var=None, val=None, log=None):
        # req.leasetime = 
        if var:
            log = f'SET {var} {val} {stateMachine.currentTerm}'
            stateMachine.lastApplied += 1
            # for stub in self.stubList:
            #     stateMachine.matchIndex[stub] += 1
            stateMachine.writeDump(f'Node {stateMachine.currentLeader} (leader) received an ({log}) request')
            stateMachine.logs.append((stateMachine.lastApplied, log, stateMachine.currentTerm))
            stateMachine.writeDump(f'Node {stateMachine.currentLeader} (leader) committed the entry ({log}) to the state machine')
        elif log:
            stateMachine.lastApplied += 1
            # for stub in self.stubList:
            #     stateMachine.matchIndex[stub] += 1
            stateMachine.logs.append((stateMachine.lastApplied, log, stateMachine.currentTerm))
            stateMachine.writeDump(f'Node {stateMachine.currentLeader} (leader) committed the entry ({log}) to the state machine')
        else:
            stateMachine.writeDump(f"Leader {stateMachine.currentLeader} sending heartbeat & Renewing Lease")
        stateMachine.appendEntries(log)
        ack = []
        def parallelEntries(arg):
            req = raft_pb2.AppendRequest()
            req.term = stateMachine.currentTerm
            req.leaderId = stateMachine.nodeId
            req.prevLogIndex = stateMachine.nextIndex[arg - 1] - 1
            print(stateMachine.nextIndex[arg - 1] - 1)
            if len(stateMachine.logs) >= 1:
                stateMachine.logs[stateMachine.nextIndex[arg - 1] - 1]
            req.prevLogTerm = stateMachine.logs[stateMachine.nextIndex[arg - 1] - 1][2] if len(stateMachine.logs) >= 1 else 0
            req.leaderCommit = stateMachine.commitIndex
            # req.entries.clear()
            req.entries.extend(log[1] for log in stateMachine.logs[stateMachine.nextIndex[arg - 1]:])
            rep = self.exceptioncallrpc(self.stubList[arg].AppendEntries, req, arg)
            return rep, len(req.entries), arg
        with futures.ThreadPoolExecutor(max_workers = 5) as executor:
            results = executor.map(parallelEntries, self.stubList.keys())
        for rep in results:
            # logs send on basis of each node
            if rep[0] != None:
                # commit not append check for follower
                # Node {NodeID of follower} (follower) committed the entry {entry operation} to the state machine.
                if rep[0].success:
                    # len(stateMachine.logs) can be used as rep[1]
                    stateMachine.nextIndex[rep[2] - 1] += rep[1]
                    stateMachine.writeDump(f'Node {rep[2]} accepted AppendEntries RPC from {stateMachine.currentLeader}.')
                else:
                    stateMachine.nextIndex[rep[2] - 1] -= 1
                    stateMachine.writeDump(f'Node {rep[2]} rejected AppendEntries RPC from {stateMachine.currentLeader}.')
                ack.append(rep[0])
        ackTotal = len(list(filter(lambda ak : ak.success, ack)))
        if ackTotal > self.totalNode // 2:
            if var:
                stateMachine.commitEntries(stateMachine.commitIndex)
                # stateMachine.lastApplied = stateMachine.commitIndex
                stateMachine.commitIndex = len(stateMachine.logs)
                # stateMachine.logDic[var] = val
            elif log:
                # stateMachine.lastApplied = stateMachine.commitIndex
                stateMachine.commitIndex = len(stateMachine.logs)
            # log = f'SET {var} {val} {stateMachine.currentTerm}'
            # stateMachine.writeFile('log', log)
            # req = raft_pb2.heartbeatRequest()
            # for stub in self.stubList:
            #     self.exceptioncallrpc(self.stubList[stub].AppendEntry, req, stub)
            # sending heartbeast & renewing lease
            self.resetLeaseTime()
        self.resetheartbeat()
    def stepDown(self):
        stateMachine.writeDump(f'Leader {stateMachine.currentLeader} lease renewal failed. Stepping Down.')
        stateMachine.currentRole = 'follower'
        stateMachine.currentLeader = None
        stateMachine.writeDump(f'{stateMachine.nodeId} Stepping down"')
        self.electionTimeout()
    def leaseTimeOut(self):
        self.leaseThread = threading.Timer(self.leaseTime, self.stepDown)
        self.leaseThread.start()
    def resetLeaseTime(self):
        self.leaseThread.cancel()
        self.leaseTimeOut()
    def exceptioncallrpc(self, rpc, req, followerNodeID):
        # print(rpc, req, t)
        try:
            rep = rpc(req, timeout=1)
            return rep
        except:
            stateMachine.writeDump(f'Error occurred while sending RPC to Node {followerNodeID}.')
            # dump node is not alive
            return None

def runNode():
    cluster.electionTimeout()

if __name__ == "__main__":
    # stateMachine = State(nodeId)
    try:
        cluster = NodeWorking(nodeTotal, clusterIp, leaseTime)
        stateMachine = State(nodeId)
        
        serving = threading.Thread(target=serve, daemon=True)
        run = threading.Thread(target=runNode, daemon=True)
        serving.start()
        run.start()
        run.join()
        serving.join()
    except:
        print('Interrupt: ')

# clear && python raft.py
