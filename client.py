import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures


clusterIp = {1:'localhost:50051', 2:'localhost:50052', 3:'localhost:50053'}

class Client:
    def __init__(self, ips):
        self.leaderId = 0
        self.channelList = dict(zip(ips.keys(), map(lambda ip : grpc.insecure_channel(ips[ip], options=[('grpc.default_timeout_ms', 1000)]), ips)))
        self.stubList = dict(zip(ips.keys(), map(lambda ch : raft_pb2_grpc.RaftStub(self.channelList[ch]), self.channelList)))
    def getk(self):
        var = input("Enter your variable: ")
        log = f'GET {var}'
        req = raft_pb2.ServeClientArgs(log)
        while True:
            rep = self.exceptioncallrpc(req)
            if rep != None:
                if rep.Success:
                    self.leaderId == rep.LeaderID
                    if rep.Data == '':
                        print("K doesn't exist in the database")
                    else:
                        print(f'Value of K is {rep.Data}')
                elif rep.LeaderID != None:
                    self.leaderId = rep.LeaderID
                elif self.getallk(log):
                    if rep.Data == '':
                        print("K doesn't exist in the database")
                    else:
                        print(f'Value of K is {rep.Data}')
                else:
                    print('all Nodes does not know about the leader')
            elif self.getallk(log):
                if rep.Data == '':
                    print("K doesn't exist in the database")
                else:
                    print(f'Value of K is {rep.Data}')
            else:
                print('all Nodes does not know about the leader')
                break
    def getallk(self, log):
        def parallelEntries(arg):
            try:
                rep = self.stubList[arg[0]].ServeClient(arg[1], timeout=1)
                return rep
            except:
                return None
        with futures.ThreadPoolExecutor(max_workers = 5) as executor:
            results = executor.map(parallelEntries, (self.stubList.keys(), log))
            for res in results:
                if not res[0] and res[0].Success:
                    if not res[1]:
                        self.leaderId = res[1]
                    return res[0]
            return False
    def setk(self):
        var = input("Enter your variable: ")
        val = input("Enter value: ")
        log = f'SET {var} {val}'
        req = raft_pb2.ServeClientArgs(log)
        while True:
            rep = self.exceptioncallrpc(req)
            if rep != None:
                if rep.Success:
                    if rep.LeaderId:
                        self.leaderId == rep.LeaderID
                    print(f'SET IS SUCCESS')
                elif rep.LeaderID != None:
                    if rep.LeaderId:
                        self.leaderId = rep.LeaderID
                elif self.getallk(log):
                    print(f'SET IS SUCCESS')
                else:
                    print('all Nodes does not know about the leader')
            elif self.getallk(log):
                print(f'SET IS SUCCESS')
            else:
                print('all Nodes does not know about the leader')
    def exceptioncallrpc(self, req):
        try:
            rep = self.stubList[self.leaderId].ServeClient(req, timeout=1)
            return rep
        except:
            return None

if __name__ == "__main__":
    client = Client(clusterIp)
    while True:
            print("1. SET")
            print("2. GET")
            opt = int(input("Enter your choice: "))
            if opt == 1:
                client.setk()
            elif opt == 2:
                client.getk()
            else:
                print('current option is not valid')