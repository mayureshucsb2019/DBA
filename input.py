import socket
import sys
import json
import threading
from threading import Thread, Lock
import time

IP = "127.0.0.1"
pidPort = {}
pidPort[1] = 5001
pidPort[2] = 5002
pidPort[3] = 5003

def sendMessage(msg, pid):
    # time.sleep(2)
    try:
        s = socket.socket()
        s.connect((IP, pidPort[pid]))
        s.sendall(msg.encode('utf-8'))
        s.close()
    except:
        print("Client" + str(pid) + " is down!")
        
        
if __name__ == "__main__":
    # Reading the client configurations
    # f = open(sys.argv[2], 'r')
    
    while True:
        inp = input("Enter input: \n")
        inpList = inp.strip().split(';')
        for x in inpList:
            x = x.strip().split()
            pid = int(x[0])
            trans = x[1]
            msg = "trans "+trans
            sendMessage(msg, pid)
            

