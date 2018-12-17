# Author: Kevin Ersoy
# distServer.py

##### Script description
# This script is meant to be used with distClient instances.  This server will queue up a bunch of jobs and 
# distribute them to available clients while load balancing.  If a client drops off, the server should know
# which jobs were lost and return those jobs to the queue.  Server should print the current state of all
# clients and jobs *every so often.

# inputs:
#	1 server port                           def: 49999


import sys       # used for various including CLI args
import os        # used to determine OS version for terminal clear
import socket    # used for communication with clients
import select    # used for reading from sockets
import time      # used for wait
import pickle    # used for serialization
import job as j  # used for Job running
from prettytable import PrettyTable # used for terminal printing
 
 
def check_server(address, port):
	# Checks if the given port is available to start the server
	s = socket.socket()
	try:
		readyForUse = s.connect_ex((address, port))
		if (readyForUse == 0):
			return True
		else:
			return False
	except:
		pass
 
def client_disconnected(sock):
	# Removes the client and handles job hand-offs
	toremove = [element for element in CLIENTS if element[0] == sock] # returns list of clients where socket matches
	# toremove should be a list of 1 element, the client we're interested in
	client = toremove[0]
	clientJobQueue = client[3]
	# Return all jobs from this client to the unassignedJobs queue
	for jobQueue in clientJobQueue:
		if(not jobQueue[0] in unassignedJobs):
			unassignedJobs.append(jobQueue[0])
	CLIENTS.remove(client)
	print "Client (%s, %s) disconnected" % (client[1], client[2])
	sock.close()
 
def print_status_table():
	# Print updated job status table in the terminal
	#  Client IP/Port, Job Instance, Completion %
	table = PrettyTable(['Client IP/Port', 'Job ID', 'Completion %'])
	for client in CLIENTS:
		clientJobQueue = client[3]
		for jobTuple in clientJobQueue:
			table.add_row(["{0}, {1}".format(client[1], client[2]), jobTuple[0].getId(), jobTuple[1]])
	for job in unassignedJobs:
		table.add_row(["unassigned", job.getId(), 0])
	os.system('cls' if os.name == 'nt' else 'clear') # Clear terminal
	print table
 
def get_client_stats(client):
	sock = client[0]
	sock.settimeout(4)
	sock.sendall('stats')
	MFC = ""
	appending = 1
	time.sleep(3)
	while(appending > 0):
		try:
			partial = sock.recv(RECV_BUFFER)
			MFC = MFC + partial
			if(len(partial) < RECV_BUFFER):
				# nothing, or EOF detected
				break
		except socket.timeout as msg:
			appending = 0
		except socket.error as msg:
			appending = 0
			print msg
	# Expect at this point MFC is a pickled jobQueue from the client
	clientJobQueue = []

	if(len(MFC) > 0):
		try:
			clientJobQueue = pickle.loads(MFC)
		except:
			pass
		client[3] = clientJobQueue
	else:
		# no message from client received, don't send anything
		pass
	
 
SERVER_PORT = 49999
SERVER_IP = "0.0.0.0"
RECV_BUFFER = 4096  
MAX_JOBS_PER_CLIENT = 3

CLIENTS = [] # List of lists, inner list is [clientSocket, clientIP, clientPORT, clientJobQueue] 
jobQueue = [] # All jobs to be processed, including ones assigned to clients
unassignedJobs = [] # List unassigned jobs
completedJobs = [] # tuples (client, job)


# Check if arguments provided
if len(sys.argv) > 1:
	SERVER_PORT = argv[1]

	
# Build list of jobs here
# This demo uses a job class which simply runs time.sleep for some number of seconds (provided below)
# For a real application, you would update this job class to take in relevant arguments and do real work
id = 100
for i in {10, 30, 19, 9, 13, 15, 18, 17, 8, 22, 16, 14, 12, 33, 31, 28, 29, 21, 20}:
	job = j.Job(id, i, i)
	jobQueue.append(job)
	unassignedJobs.append(job)
	id += 5

	
# Validate server port
# Try desired port, if unavailable increment until you find one
while check_server('127.0.0.1',SERVER_PORT):
	print str(SERVER_PORT) + " in use, incrementing..."
	SERVER_PORT += 1
	
# Start listening server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.setblocking(0)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(5) # 5 is the max number of queued connections waiting to be accepted
print "Server started on port " + str(SERVER_PORT)

while(1):
	# Infinite loop to read from client and server sockets, process new client connections, check client status
	# and distribute jobs to clients.  Tell clients to remove jobs when they're complete.
	
	# Get the list sockets which are ready to be read through select
	socket_list = [item[0] for item in CLIENTS] # get first item of each item in CLIENTS
	socket_list.append(server_socket) # server socket is also readable
	read_sockets, write_sockets, error_sockets = select.select(socket_list, [], [], 0) # 0 makes non blocking
	
	for sock in read_sockets:
		#New connection
		if sock == server_socket:
			# Handle the case in which there is a new connection received through server_socket
			sockfd, addr = server_socket.accept()  #accept returns a new socket object, sockfd, and address
			emptyList = []
			CLIENTS.append([sockfd,addr[0],addr[1], emptyList])  #add the new socket to the list                           
			print "Client %s, %s connected" % (addr[0],addr[1])
			print len(CLIENTS)
		else: 
			#Some incoming message from a client
			try:
				# In Windows, when a TCP program closes abruptly, a "Connection reset by peer"
				# exception may be thrown
				data = sock.recv
				if(len(data) == 0): #Client disconnected, return jobs to pool
					client_disconnected(sock)
				else:
					print "Client sending data unprompted : " + str(sock)
			except:
				client_disconnected(sock)

	# Update client jobQueues on server and request add/remove jobs on client
	for client in CLIENTS:
		get_client_stats(client)

		sock = client[0]
		clientJobQueue = client[3]
		operationsSendToClientTuples = [] # these tuples are (operation, jobInstance) 1=add, 0=remove
		lengthClientQueue = len(clientJobQueue)
		for jobTuple in clientJobQueue:
			# request client to remove complete jobs 
			if(jobTuple[1] == 100):
				operationsSendToClientTuples.append((0, jobTuple[0]))   # request removal in client
				try:
					jobQueue.remove(jobTuple[0])							# remove in server
					completedJobs.append((client, jobTuple[0]))
					print "job removed from jobQueue: {0}".format(jobTuple[0].getId())
				except ValueError:
					# Try/Catch because some delay between requesting client to remove and updated stats
					# Don't attempt to remove on server more than once
					pass
				clientJobQueue.remove(jobTuple)
		# send new jobs to client
		jobCount = lengthClientQueue - len(operationsSendToClientTuples)    # how many jobs on client's plate
		while(jobCount < MAX_JOBS_PER_CLIENT):
			if(len(unassignedJobs) == 0):
				break;
			poppedJob = unassignedJobs.pop()
			clientJobQueue.append((poppedJob, 0))
			operationsSendToClientTuples.append((1, poppedJob)) # request client to add job
			jobCount = jobCount + 1
		if(len(operationsSendToClientTuples) > 0): #request needs to be sent for adds and removals
			sock.sendall(pickle.dumps(operationsSendToClientTuples))
			client[3] = clientJobQueue # local update to clientJobQueue until client confirms
				
		# Print updated job status table in the terminal
		print_status_table()
	
	# Print updated job status table in the terminal
	print_status_table()
	time.sleep(1)
	if(len(jobQueue) < 1):
		break

os.system('cls' if os.name == 'nt' else 'clear') # Clear terminal
print "All Jobs Complete!"
# Print updated job status table in the terminal
#  Client IP/Port, Job Instance, Completion %
table = PrettyTable(['Client IP/Port', 'Job ID', 'Completion %'])
for jobTuple in completedJobs:
	job = jobTuple[1]
	client = jobTuple[0]
	table.add_row(["{0}, {1}".format(client[1], client[2]), job.getId(), 100])
os.system('cls' if os.name == 'nt' else 'clear') # Clear terminal
print table
