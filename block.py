import json

class Transaction:
    def __init__(self, sender=None, receiver=None, amount=None, clock=None):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,sort_keys=True)
    
    def toTuple(self):
        return (self.sender, self.receiver, self.amount)
    
    @staticmethod
    def load(js):
        trans = Transaction()
        if isinstance(js, str):
            js = json.loads(js)
        trans.__dict__ = js
        return trans

class BallotNum:
    def __init__(self, num=0, pid=0):
        self.num = num
        self.pid = pid

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,sort_keys=True)
    
    def toTuple(self):
        return (self.num, self.pid)
    
    @staticmethod
    def load(js):
        ballot = BallotNum()
        if isinstance(js, str):
            js = json.loads(js)
        ballot.__dict__ = js
        return ballot
    
    def reset(self):
        self.num = 0
        self.pid = 0
    
    def isHigher(self, ballotNum):
        high = False
        if self.num > ballotNum.num:
            high = True
        elif self.num == ballotNum.num and self.pid > ballotNum.pid:
            high = True
        return high
    
    def isEqual(self, ballotNum):
        return self.num == ballotNum.num and self.pid == ballotNum.pid


class Node(object):
    def __init__(self, seq_num=0, data=None, next_node=None):
        self.seq_num = seq_num
        self.data = data[:]
        self.next_node = next_node
        
    def getBlock(self):
        a = []
        for t in self.data:
            a.append(t.toJSON())
        return a
        
    def printNode(self):
        a = []
        for v in self.data:
            a.append(v.toTuple())
        print (self.seq_num, a)

class BlockChain(object):
    def __init__(self, initial_balance=10, head=None):
        self.head = head
        self.tail = head
        self.initial_balance = initial_balance
    
    def get_head(self):
        return self.head
        
    def append(self, seq_num, data):
        if len(data) > 0:
            newNode = Node(1, data)
            if self.head is None:
                if seq_num == 1:
                    self.head = newNode
                    self.tail = newNode
                else:
                    print("INCORRECT SEQUENCE NUMBER HEAD NONE")
            else:
                if self.tail.seq_num+1 == seq_num:
                    newNode.seq_num = self.tail.seq_num + 1
                    self.tail.next_node = newNode
                    self.tail = newNode
                else:
                    print("INCORRECT SEQUENCE NUMBER")
        if self.tail is None:
            return 0
        return self.tail.seq_num

    def getBalance(self, user):
        amount = self.initial_balance
        node = self.head
        while node is not None:
            for v in node.data:
                if v.sender == user:
                    amount -= v.amount
                elif v.receiver == user:
                    amount += v.amount
            node = node.next_node
        return amount
    
    def getLastSeqNum(self):
        if self.tail is None:
            return 0
        return self.tail.seq_num
    
    def getBlocks(self, n):
        node = self.head
        while n != 0:
            node = node.next_node
            n -= 1
        blocks = []
        while node is not None:
            blocks.append(node.getBlock())
            node = node.next_node
        return blocks

    def toList(self):
        chain = []
        node = self.head
        while node is not None:
            chain.append(node.data.toTuple())
            node = node.next_node
        return chain
    
    def printChain(self):
        node = self.head
        while node is not None:
            node.printNode()
            node = node.next_node
        