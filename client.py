import socket
import sys
import json
import threading
from threading import Thread, Lock
import time
from block import Transaction, BlockChain, BallotNum
from datetime import datetime

PID = int(sys.argv[1])
print("Process id: ", PID)

IP = "127.0.0.1"
BUFFER_SIZE = 1024
NUM_CLIENTS = 3
CLIENTS = []
LAMPORT = 0
BALANCE = 10
MAJORITY = 1
SEQ_NUM = 0
BALLOT_NUM = BallotNum(0,PID)
ACCEPT_NUM = BallotNum()
ACKED_BALLOT = BallotNum()
ACCEPT_VAL = []
ACK_COUNT = 0
ACCEPT_COUNT = 0
MAX_ACK_NUM = BallotNum()
MAX_ACK_VAL = []
isLeader = False
transaction_log = []
INPUT = ""

PORT = 5000+PID
clientConn = {}
pidConfig = {}
# INTERVAL = 0   
CLIENT_SEQ_NUM = {}  

#ADDED BY MAYURESH
# To be changed when FOLLOWER receives ACCEPT from LEADER
FOLLOWER_FLAG_ACCEPT = False
# To be changed when FOLLOWER receives COMMIT from LEADER
FOLLOWER_FLAG_COMMIT = False
# To be changed when FOLLOWER receives new PREPARE message
FOLLOWER_FLAG_RESET_TIMER = False
# To be changed when decided to CRASH and regained conciousness
CRASH_FLAG = False
# To be changed when decided to CRASH and regained conciousness
PREPARE_RECEIVED_FLAG = False
ACCEPT_RECEIVED_FLAG = False

def printLog():
    global transaction_log
    print("########################################")
    print("Transaction log is: ",end=" ")
    for x in transaction_log:
        print(f"{x.toTuple()}, ",end=" ")
    print()
    print("########################################")

def sendMessage(msg, pid):
    time.sleep(2)
    try:
        s = socket.socket()
        s.connect((IP, pidConfig[pid]))
        s.sendall(msg.encode('utf-8'))
        s.close()
    except:
        print("Client" + str(pid) + " is down!")
    
def incrementAckCount():
	global ACK_COUNT
	mutex = Lock()
	mutex.acquire()
	ACK_COUNT += 1
	mutex.release()
	print ('ACK_COUNT: ' + str(ACK_COUNT))
 
def incrementAcceptCount():
	global ACCEPT_COUNT
	mutex = Lock()
	mutex.acquire()
	ACCEPT_COUNT += 1
	mutex.release()
	print ('ACCEPT_COUNT: ' + str(ACCEPT_COUNT))
 
def incrementInterval(cnt):
	global INTERVAL
	mutex = Lock()
	mutex.acquire()
	INTERVAL += cnt
	mutex.release()
	print ('Interval: ' + str(INTERVAL))

def sendPrepare():
    global SEQ_NUM
    global BALLOT_NUM
    global ACKED_BALLOT
    global NUM_CLIENTS
    global isLeader
    
    BALLOT_NUM.num += 1
    BALLOT_NUM.pid = PID
    ACKED_BALLOT.num = BALLOT_NUM.num
    ACKED_BALLOT.pid = BALLOT_NUM.pid
    isLeader = True
    # print(f"Ballot num: {BALLOT_NUM.num}")
    SEQ_NUM = chain.getLastSeqNum() + 1
    data = {
        'pid': PID,
        'type': 'prepare',
        'ballot': BALLOT_NUM.toJSON(),
        'seq_num': SEQ_NUM
    }
    message = json.dumps(data)
    for i in range(1, NUM_CLIENTS+1):
        if i != PID:
            threading.Thread(target = sendMessage, args = (message, i,)).start()
    print ('Prepare message sent to clients')  
    threading.Thread(target = startTimerForAck, args = (10,)).start() 
    
def startTimerForAck(start=15):
    # global INTERVAL
    global ACK_COUNT
    global MAJORITY
    global SEQ_NUM
    #ADDED BY MAYURESH
    global CRASH_FLAG   
    global isLeader 

    INTERVAL = start
    while CRASH_FLAG == False and not CRASH_FLAG: #CHANGED BY MAYURESH
        time.sleep(1)
        INTERVAL -= 1
        # print ('INTERVAL: ' + str(INTERVAL))
        if ACK_COUNT == NUM_CLIENTS - 1:
            INTERVAL = 0
        if INTERVAL <= 0:
            if ACK_COUNT >= MAJORITY and isLeader:  
                log = []          
                if len(MAX_ACK_VAL) != 0:
                    log = MAX_ACK_VAL[:]
                else:
                    for val in transaction_log:
                        log.append(val.toJSON())

                data = {
                    'pid': PID,
                    'type': 'accept',
                    'ballot': BALLOT_NUM.toJSON(),
                    'seq_num': SEQ_NUM,
                    'value': log   
                }
                message = json.dumps(data)
                for i in range(1, NUM_CLIENTS+1):
                    if i != PID:
                        threading.Thread(target = sendMessage, args = (message, i,)).start()
                print ('Accept message sent to followers')
                ACK_COUNT = 0
                threading.Thread(target = startTimerForAccept, args = (10,)).start()
            else:
                SEQ_NUM = chain.getLastSeqNum()
                print("Paxos Failed!!!")
                # if INPUT != "":
                #     print(f"Pending transaction: {INPUT}")
                #     handleTransaction(INPUT)
            break

def startTimerForAccept(start=15):
    # global INTERVAL
    global ACCEPT_COUNT
    global MAJORITY
    global INPUT
    global SEQ_NUM
    global CLIENT_SEQ_NUM
    #ADDED BY MAYURESH
    global CRASH_FLAG
    global isLeader

    INTERVAL = start
    while CRASH_FLAG == False and not CRASH_FLAG: #CHANGED BY MAYURESH
        time.sleep(1)
        INTERVAL -= 1
        # print ('INTERVAL: ' + str(INTERVAL))
        if ACCEPT_COUNT == NUM_CLIENTS - 1:
            INTERVAL = 0
        if INTERVAL <= 0:
            if ACCEPT_COUNT >= MAJORITY and isLeader:
                val = []
                for aval in transaction_log:
                    val.append(aval.toJSON())
                data = {
                    'pid': PID,
                    'type': 'commit',
                    'ballot': BALLOT_NUM.toJSON(),
                    'seq_num': SEQ_NUM,
                    'value': val  
                }
                
                for i in range(1, NUM_CLIENTS+1):
                    if i != PID:
                        data['prev_blocks'] = None
                        if CLIENT_SEQ_NUM.get(i) is not None and CLIENT_SEQ_NUM[i]+1 < SEQ_NUM:
                            data['prev_blocks'] = chain.getBlocks(CLIENT_SEQ_NUM[i])                           
                        message = json.dumps(data)    
                        threading.Thread(target = sendMessage, args = (message, i,)).start()
                        
                print ('Decide message sent to followers')
                ACCEPT_COUNT = 0
                SEQ_NUM = chain.append(SEQ_NUM, transaction_log)
                transaction_log.clear()
                chain.printChain()
                isLeader = False # No more a leader
                if INPUT != "":
                    print(f"Pending transaction: {INPUT}")
                    handleTransaction(INPUT)
                    INPUT = ""
            else:
                SEQ_NUM = chain.getLastSeqNum()
                print("Paxos Failed!!!")
                # if INPUT != "":
                #     print(f"Pending transaction: {INPUT}")
                #     handleTransaction(INPUT)
            break

# ADDED BY MAYURESH    
def startTimerForFollowerAccept(start, ballotNum, pid):
    # global FOLLOWER_INTERVAL
    global FOLLOWER_FLAG_RESET_TIMER 
    global FOLLOWER_FLAG_ACCEPT
    global BALLOT_NUM
    FOLLOWER_FLAG_RESET_TIMER = False #If new PREPARE message is accepted then it doesnt start paxos
    FOLLOWER_INTERVAL = start
    while FOLLOWER_FLAG_ACCEPT != True and not CRASH_FLAG:
        time.sleep(1)
        FOLLOWER_INTERVAL -= 1
        # print (f'Follower-Interval in accept of {pid}: {FOLLOWER_INTERVAL}')
        if FOLLOWER_INTERVAL <= 0:
            break
    if FOLLOWER_INTERVAL <= 0 and BALLOT_NUM.num == ballotNum.num and ((not FOLLOWER_FLAG_ACCEPT) and (not FOLLOWER_FLAG_RESET_TIMER)) and not CRASH_FLAG:
        FOLLOWER_FLAG_ACCEPT = False
        # START PAXOS
        print(f"Paxos started due to accept timer lapse of {pid}")
        sendPrepare()
        
        # RESET ALL GLOBAL VARIABLES   ???????????????????????????????????????

# ADDED BY MAYURESH       
def startTimerForCommit(start, ballotNum, pid):
    # global FOLLOWER_INTERVAL
    global FOLLOWER_FLAG_RESET_TIMER 
    global FOLLOWER_FLAG_COMMIT
    global BALLOT_NUM
    FOLLOWER_FLAG_RESET_TIMER = False #If new PREPARE message is accepted then it doesnt start paxos
    FOLLOWER_INTERVAL = start
    while not FOLLOWER_FLAG_COMMIT and not CRASH_FLAG:
        # print(f"follower flag: {FOLLOWER_FLAG_COMMIT}")
        time.sleep(1)
        FOLLOWER_INTERVAL -= 1
        # print ('FOLLOWER_INTERVAL in commit: ' + str(FOLLOWER_INTERVAL))
        if FOLLOWER_INTERVAL <= 0:
            break
    # print(f"follower flag, timer: {FOLLOWER_FLAG_COMMIT} {FOLLOWER_FLAG_RESET_TIMER} in COMMIT")
    # print(f"My ballot, started ballot: {BALLOT_NUM.num}, {ballotNum.num}")
    if FOLLOWER_INTERVAL <= 0 and BALLOT_NUM.num == ballotNum.num and ((not FOLLOWER_FLAG_COMMIT) and (not FOLLOWER_FLAG_RESET_TIMER)) and not CRASH_FLAG:
        FOLLOWER_FLAG_COMMIT = False
        # START PAXOS
        print(f"Paxos started due to commit timer lapse of {pid}")
        sendPrepare()
        # RESET ALL GLOBAL VARIABLES   ???????????????????????????????????????


def processMessage(data):  
    global BALLOT_NUM
    global ACCEPT_NUM
    global ACCEPT_VAL
    global MAX_ACK_NUM
    global MAX_ACK_VAL 
    global ACK_COUNT
    global ACCEPT_COUNT 
    global INTERVAL
    global FOLLOWER_FLAG_COMMIT
    global FOLLOWER_FLAG_ACCEPT
    global FOLLOWER_FLAG_RESET_TIMER
    global CLIENT_SEQ_NUM
    global SEQ_NUM
    global CRASH_FLAG
    global PREPARE_RECEIVED_FLAG
    global ACCEPT_RECEIVED_FLAG
    global isLeader
    global ACKED_BALLOT
    global INPUT
    data = json.loads(data)
    pid = data['pid']
    # print (f"{data['type']} Message from client {pid}")
    # print(f"Sequence Number is {SEQ_NUM}")

    if data['type'] == 'prepare' and (not CRASH_FLAG):
        print(f"Prepare Message Received from {pid}")
        ballotNum = BallotNum.load(data['ballot'])
        # print(f"Ballot mine: {BALLOT_NUM.num},{BALLOT_NUM.pid}")
        # print(f"Ballot received: {ballotNum.num},{ballotNum.pid}")
        if ballotNum.isHigher(BALLOT_NUM) and chain.getLastSeqNum() < data['seq_num']: #DOESNT ALLOW USER WITH LOWER SEQNUM TO BECOME LEADER
            isLeader = False
            PREPARE_RECEIVED_FLAG = True 
            # WE NEED TO RESET EARLIER TIMERS HERE SO THAT FOLLOWER DOESNT START PAXOS
            FOLLOWER_FLAG_RESET_TIMER = True
            FOLLOWER_FLAG_ACCEPT = False
            FOLLOWER_FLAG_COMMIT = False
            ACKED_BALLOT.num = ballotNum.num
            ACKED_BALLOT.pid = ballotNum.pid
            BALLOT_NUM.num = ballotNum.num
            
            val = []
            for aval in ACCEPT_VAL:
                val.append(aval.toJSON())
            data = {
                'pid': PID,
                'type': 'ack',
                'ballot': BALLOT_NUM.toJSON(),
                'seq_num': chain.getLastSeqNum(),
                'accept_ballot': ACCEPT_NUM.toJSON(),
                'accept_val': val 
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pid,)).start()
            print ('Ack message sent to client '+str(pid))
            #ADDED BY MAYURESH
            # print(f"Timer started for ACCEPT messages of {pid}")
            threading.Thread(target = startTimerForFollowerAccept, args = (15,ballotNum,pid,)).start()
            
         
    elif data['type'] == 'ack' and (not CRASH_FLAG) and isLeader:
        print(f"ACK Message Received from {pid}")
        #  check for majority and send accept to followers
        incrementAckCount()
        acceptBallot = BallotNum.load(data['accept_ballot'])
        acceptVal = data['accept_val']
        if len(acceptVal) != 0 and acceptBallot.isHigher(MAX_ACK_NUM):
            MAX_ACK_NUM = acceptBallot
            MAX_ACK_VAL = acceptVal[:]
                   
    elif data['type'] == 'accept' and PREPARE_RECEIVED_FLAG and (not CRASH_FLAG) and not isLeader:
        print(f"ACCEPT Message Received from {pid}")
        ballotNum = BallotNum.load(data['ballot'])
        if ACKED_BALLOT.isEqual(ballotNum):
            PREPARE_RECEIVED_FLAG = False
            ACCEPT_RECEIVED_FLAG = True
            FOLLOWER_FLAG_ACCEPT = True
            BALLOT_NUM.num = ballotNum.num
            ACCEPT_NUM.num = ballotNum.num
            ACCEPT_NUM.pid = ballotNum.pid
            val = []
            for aval in transaction_log:
                val.append(aval.toJSON())
            ACCEPT_VAL = [ Transaction.load(val) for val in data['value'] ]
            data = {
                'pid': PID,
                'type': 'accepted',
                'ballot': BALLOT_NUM.toJSON(),
                'seq_num': chain.getLastSeqNum(),
                'value': val
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pid,)).start()
            print ('Accepted message sent to client '+str(pid))
            #ADDED BY MAYURESH
            # print(f"Timer started for COMMIT messages for {pid}")
            threading.Thread(target = startTimerForCommit, args = (15,ballotNum,pid,)).start()        
  
    elif data['type'] == 'accepted' and (not CRASH_FLAG) and isLeader:
        print(f"ACCEPTED Message Received from {pid}")
        incrementAcceptCount()
        #CHANGED BY MAYURESH
        CLIENT_SEQ_NUM[data['pid']] = data['seq_num']
        #ACCESS CLIENT SEQUENCE NUMBER FROM MESSAGE ACCEPTED
        #LEADER MUST ADD PREVIOUS LOG ENTRIES
            #APPEND GENISIS BLOCKS WHICH ARE NOT AVAILABLE IN FOLLWER FROM LEADER???????????????????????????????
            #send request to follower to send its unmatched blocks??????????????????????????????????????????????
         
        for aval in data['value']:
            transaction_log.append(Transaction.load(aval)) # This transaction log has to be checked before adding 
  
    elif data['type'] == 'commit' and ACCEPT_RECEIVED_FLAG and (not CRASH_FLAG) and not isLeader:
        print(f"COMMIT Message Received from {pid}")
        # print (f'Decide message from leader')
        FOLLOWER_FLAG_COMMIT = True
        ACCEPT_RECEIVED_FLAG = False
        # Follower and Leader must check what SEQUENCE NUMBERS ARE MISSING FROM THEIR LOGS
        # FOLLOWER has LESSER SEQUENCE NUMBER THAN LEADER [can happen!]
        # LEADER has LESSER SEQUENCE NUMBER THAN FOLLOWER [can happen!]
        # WHOEVER HAS LESSER SENDS THE SEQUENCE NUMBERS AND REQUESTS FOR THE BLOCK
        # SO ONE MORE PROCESS MESSAGE TYPE
        
        # ???????? IT HAS TO CHECK THAT GIVEN DATA IS A BLOCK THEN APPEND AS A BLOCK ELSE COLLECT VALUES
        # IT IS RECOMMENDED THAT LEADER MUST ALWAYS SEND ONLY BLOCK
            
        if data['prev_blocks'] is not None:
            blocks = data['prev_blocks']
            for i in range(len(blocks)):
                a = [ Transaction.load(x) for x in blocks[i] ]
                SEQ_NUM = chain.append(SEQ_NUM+1, a)
                
        val = [ Transaction.load(x) for x in data['value'] ] 
        SEQ_NUM = chain.append(SEQ_NUM+1, val)
        transaction_log.clear()
        chain.printChain()
        if INPUT != "":
            print(f"Pending transaction: {INPUT}")
            handleTransaction(INPUT)
            INPUT = ""
            
        
  
def getBalance(pid):
    amount = chain.getBalance(pid)
    for log in transaction_log:
        amount -= log.amount
    return amount                  

def handleTransaction(data):
    dataList = data.split(',')
    if dataList[0] == 't':
        receiver = int(dataList[1])
        amount = int(dataList[2])
        amountBefore = getBalance(PID)
        if amountBefore >= amount and PID != receiver:
            transaction_log.append(Transaction(PID, receiver, amount))
            print("SUCCESS")
            print("Balance before: $"+str(amountBefore))
            print("Balance after: $"+str(amountBefore - amount))
        else:
            print("INCORRECT") 
    
def processInput(data):
    global CRASH_FLAG
    global isLeader
    global INPUT
    global FOLLOWER_FLAG_ACCEPT
    global FOLLOWER_FLAG_COMMIT
    global FOLLOWER_FLAG_RESET_TIMER
    
    dataList = data.split(',')
    if dataList[0] == 't':
        # Update getBalance to get balance
        receiver = int(dataList[1])
        amount = int(dataList[2])
        amountBefore = getBalance(PID)
        # print(f"{PID} is {receiver}")
        if amountBefore >= amount and PID != receiver:
            transaction_log.append(Transaction(PID, receiver, amount))
            print("SUCCESS")
            print("Balance before: $"+str(amountBefore))
            print("Balance after: $"+str(amountBefore-amount))
            INPUT = ""
        elif PID != receiver:
            # Run Paxos
            # print("Paxos started normally")
            sendPrepare()
        else:
            print("You cannot send money to yourself!")
            
    elif dataList[0] == 'b' :
        if len(dataList) == 1:
            dataList.append(str(PID))
        print("Balance: $"+str(getBalance(int(dataList[1]))))
        INPUT = ""

    elif dataList[0] == 'tlog' :
        INPUT = ""
        printLog()
    
    elif dataList[0] == 'blog' :
        INPUT = ""
        print("Block Chain is:")
        chain.printChain()
        print("###########################################")

    # CHANGED BY MAYURESH    
    elif dataList[0] == "s" : #TO CRASH the client
        # ALSO ASK FOR TIME IT WANTS TO STOP
        INPUT = ""
        print(f"Server is unconcious!!!")
        CRASH_FLAG = True
        FOLLOWER_FLAG_ACCEPT = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        FOLLOWER_FLAG_COMMIT = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        FOLLOWER_FLAG_RESET_TIMER = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        time.sleep(int(dataList[1])) #??????????????????????????
        print(f"Server regained conciousness!!!")
        CRASH_FLAG = False
        #DO NOTHING from NOW till new transaction

def createServer(pid):
    global INPUT
    global CRASH_FLAG
    try: 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', PORT))         
        print("socket binded to %s" %(PORT))
        s.listen(1)      
        print("socket successfully created")
    except socket.error as err: 
        print("socket creation failed with error %s" %(err)) 

    while True:
        try:
            conn, addr = s.accept()
            data = conn.recv(BUFFER_SIZE).decode('utf-8')
            if not data:
                break
            inp_data = data.split()
            # print(f"inp data received at {datetime.now()} : {data}")
            if inp_data[0] == "trans" and not CRASH_FLAG:
                INPUT = inp_data[1]
                print(f"Input: {inp_data[1]}")
                processInput(inp_data[1])
            elif not CRASH_FLAG:
                # print(f"calling process message at {datetime.now()}")
                processMessage(data)
        except expression as identifier:
            print ("Socket error in receiving message")
   
     
if __name__ == "__main__":
    # Reading the client configurations
    f = open(sys.argv[2], 'r')
    configList = f.readlines()
    NUM_CLIENTS = len(configList)
    for i in range(1, NUM_CLIENTS+1):
        config = configList[i-1].strip().split(',')
        if len(config) != 3:
            print("Incorrect configuration")
            sys.exit()
        pidConfig[i] = int(config[1])
        
    config = configList[PID-1].strip().split(',')
    IP = config[0]
    PORT = int(config[1])
    BALANCE = int(config[2])
    
    # Creating server to listen for connections
    server_thread = threading.Thread(target = createServer,args = (PID,)) 
    server_thread.start() 

    print("Balance: $"+str(BALANCE))
    print("######################################################")
    print("TYPES OF MESSAGES TO INPUT:")
    chain = BlockChain(BALANCE)
    print("Balance: b\nCrash: s\nBlockchain Log: blog\nTransaction Log: tlog\nTransaction: t,pid_to,amount")
    print("######################################################")
    while True:
        INPUT = input()
        processInput(INPUT)

