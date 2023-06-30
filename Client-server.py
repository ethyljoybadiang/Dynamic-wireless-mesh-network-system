import socket
import os
import subprocess
import sys
import threading
import time
import gzip
import csv
import shutil
from queue import Queue
import binascii

t=threading.Thread()
NUMBER_OF_THREADS =2 
JOB_NUMBER = [1,2]
queue = Queue()
all_connections = []
all_address = []
FORMAT='UTF-8'
hostname=socket.gethostname()
localIP=socket.gethostbyname(hostname)
NAME ='10.10.26.199'
SIZE =4096
goal_filename='test4'
bridge_filename='test4'
# Create a Socket ( connect two computers)
def create_socket_as_client():
    

    try:
        print('\n ***Client mode**** \n')
        global clientHost
        global clientPort
        global sClient
        clientHost ='10.10.26.180'
        clientPort = 1111
        sClient = socket.socket()
        addrClient=(clientHost, clientPort)
        sClient.connect(addrClient)

  #      sClient.send(str.encode('N1'))
      # list_connections()
    except socket.error as msg:
        print("Client mode: Socket creation error: " + str(msg))
        print("START AGAIN AS CLIENT")
        create_workers()
        create_jobs()

    while True:
        receive_as_client(sClient)
    
# Create a Socket ( connect two computers)
def create_socket_as_server():
    try:
        #print('serverrr')
        print('\n ***Server mode**** \n')
        global serverHost
        global serverPort
        global sServer
        serverHost = ""
        serverPort = 2222
        sServer = socket.socket()
        sServer.bind((serverHost, serverPort))
        sServer.listen(5)
        print('server created')
    
    except socket.error as msg:
        print("Socket creation error: " + str(msg))
        print("START AGAIN AS SERVER")
        create_workers()
        create_jobs()
    
# Handling connection from multiple clients and saving to a list
# Closing previous connections when server.py file is restarted

def accepting_connections():
    for c in all_connections:
        c.close()

    del all_connections[:]
    del all_address[:]

    while True:

        try:
            conn, address = sServer.accept()
            sServer.setblocking(1)  # prevents timeout

            all_connections.append(conn)
            all_address.append(address)  
            #list_connections()
            print("Connection has been established :" + address[0])

        except:
            print("Error accepting connections")
    


# Display all current active connections with client

def list_connections():
    global grandchildren
    global tree
    tree=''
    grandchildren=''
    global direct_children
    direct_children='N1:'+NAME
    arr=[]
   # broadcast()
    for i, conn in enumerate(all_connections):       
        grandchildren=receive_list(conn)
        tree=tree+' '+grandchildren
        direct_children =direct_children+' ' + str(all_address[i][0])
        
        print("Client "+" " + direct_children)
        print('grandchildren:'+'\n'+grandchildren)
        
    tree=direct_children+' '+tree
    send_as_client(tree)

def branching(goal):
    global path
    branch=''
    direct=''
    if(direct_children.count(goal)==1):
        for i,conn in enumerate(all_connections):
            if(goal==all_address[i][0]):
                direct=all_address[i][0]
    else:
        for x in tree:
            x.strip('')
            branch=x.split(' ')
            near=branch[1:]

            if(branch.count(str(goal))):
                direct=str(''.join(near[:1])).split(':')[1]

    print('direct: '+str(direct))
    send(direct, goal)

def send(direct,goal):
    global conn
    print('send function')
    for i in range(len(all_address)):
        print(i)
        if(str(direct)==str(all_address[i][0])):
            conn=all_connections[i]
    conn.send(str.encode('GOAL:'+str(goal)))
    print('sent')
    receive_as_server(direct)

def receive_list(conn):
    conn.send(str.encode('list'))
    grandchildren=conn.recv(1024).decode(FORMAT)
    print('tree: '+grandchildren)
    return grandchildren
    
def send_as_server(conn,message):

    conn.send(str.encode(message))
    data=conn.recv(1024).decode(FORMAT)
    print('send_as_server: '+data)
    receive_as_server(conn)

def send_as_client(data):
    print('RPi: send_as_client working')
    print('list sent to server:'+str(data)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    )
    sClient.send(str.encode(data))
    
def receive_as_server(conn):
    print('receive as server')
    grandchildren=''
    data=conn.recv(1024)
    print(data)
    conn.send(b'RPI: to end client')
    if(data[0]=='N'):
    	grandchildren=grandchildren+' '+data
    	return grandchildren
    if(data[:11]=='Sending File'):
        sendFile(sCLient,Client)
    if(data[:9]=='File sent'):
    	sendFile(sClient,Client)
    if(data[:5]=='Error'):
        send_as_client(data)
        
def receive_as_client(sClient):
        global goal
        global NAME
        data=sClient.recv(1024).decode(FORMAT)
        print(data)
        for i,conn in enumerate(all_connections):
            if(data==str(all_address[i][0])):
               #conn=all_connections[i]
               conn.send(str.encode('RPI:receive_as_client() function working'))
               simultoserver(conn)
               
               break
        if(data[:4]=='GOAL'):
            data=data.split(':')
            goal=data[1]
            if(str(goal)==NAME):
                print('chosen')
                sendFile(sClient)
                #receive(conn)
            else:
                branching(str(goal))

        if(data=='list'):
            list_connections()


def asClient():
    print(str('5454'))
    sClient.send(b'ethyl')
        #data=sClient.recv(1024).decode(FORMAT)
        #print(data)
    receive_as_client(sClient)
        
def asServer():
    create_socket_as_server()
    accepting_connections()

def simultoserver(conn):
    send_as_server(conn)
    receive_as_server(conn)
    

# Do next job that is in the queue (handle connections, send commands)
def sendFile(client):
    global crcdata
    
    global data1
    file_number=1
    print("Compressing file...")

    start_time = time.time()
    
    with open(goal_filename+'.csv', 'rb') as f_in, gzip.open(goal_filename+'.csv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    CRC_value=CRC()
    crcdata='CRC:'+str(CRC_value)
    print(CRC_value)
    sClient.send(str.encode(crcdata))
    print("File compression is successful.") 
    print("Time taken: {:.2f} seconds.".format(elapsed_time))
    #SIZE = int(os.stat(output_file).st_size)
    
    with open(goal_filename+'.csv.gz', 'rb') as file:
        while True:
            gz_data = file.read(4096*4096)
            client.send(gz_data)
            time.sleep(1)
            if not gz_data:
                break
        file.close()
    asClient()
    print('File sent')
    print('done sent')
   # os.remove(goal_filename+'.csv')
    print(goal_filename+'csv'+' deleted successfully')  
    #os.remove(goal_filename+'.csv.gz')
    print(goal_filename+'csv.gz'+' deleted successfully')  
    
        
def send(direct,goal):
    global conn
    for i in range(len(all_address)):
        if(str(direct)==str(all_address[i][0])):
            conn=all_connections[i]
            conn.send(str.encode('GOAL:'+str(goal)))
    t.daemon=False
    queue.task_done()
    receive(conn)

def receive(conn):
    global hop
    global SIZE
    global file_data
    global elapsed_time
    global crcdata
    data=conn.recv(1024).decode('utf-8')
    start_time = time.time()
    if(data[:3]=='CRC'):
        crcdata=data
        file_data=b''
        data=b''
        try:
            with open(bridge_filename+'.csv.gz','wb') as  file:
                while True:
                    file_data=conn.recv(4096*4096)

                    if not file_data:
                        break
                    if (file_data==b'ethyl'):
                        break
                    print('done')
                    file.write(file_data)
                file.close()
            end_time = time.time()
            elapsed_time = end_time - start_time
            print('from N3-N1 receive:'+str(elapsed_time))
            bridgeFile(sClient)
            
        except:
            bridgeFile(sClient)

    if(data=='Node Error'):
        sClient.send(data)

def bridgeFile(client):
    global data1
    try:
        file_number=1
        #print("Compressing file...")
        start_time = time.time()
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        client.send(str.encode(crcdata))
        print("File compression is successful.")
        print("Time taken: {:.2f} seconds.".format(elapsed_time))
        #SIZE = int(os.stat(output_file).st_size)
        
        with open(bridge_filename+'.csv.gz', 'rb') as file:
            while True:
                gz_data = file.read(4096*4096)
                client.send(gz_data)
                time.sleep(1)
                if not gz_data:
                    break
            file.close()
        elapsed_time = end_time - start_time
        print('from N1 to server send:'+str(elapsed_time))
        #asClient()
        print('File hopped')
        print('done sent')
       # os.remove(bridge_filename+'.csv.gz')
        print(bridge_filename+'.csv.gz'+' deleted successfully')
        
    except:
        create_workers()
        create_jobs()

    
def CRC():
    buf=open(goal_filename+str('.csv.gz'),'rb').read()
    buf=(binascii.crc32(buf) & 0xFFFFFFFF)
    
    return "%08X" % buf

# Do next job that is in the queue (handle connections, send commands)
def work():
    while True:
        x = queue.get()
        if x == 1: #AS CLIENT
            create_socket_as_client()
            
        if x == 2: #AS SERVER
            asServer()
        if x==3:
            receive_as_client(sClient)
        
    #receive(conn)
    queue.task_done()

# Create worker threads
def create_workers():
    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work)
        t.daemon = True
        t.start()

def create_jobs():
    for x in JOB_NUMBER:
        queue.put(x)

    queue.join()


    
create_workers()
create_jobs()
