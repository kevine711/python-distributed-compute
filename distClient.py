# Author: Kevin Ersoy
# distClient.py

##### Script description
# This script is meant to be used with distServer.  This client should connect to the server via default or cli args.
# Once connected to the server, we create and maintain a list of Job objects.  The server will poll us for status updates
# in which we supply a list of jobs and completion statistics.  The server is in charge of managing our load and 
# distributing new jobs.

# inputs:
#	1 server IP (if provided, provide port) def: localhost
#	2 server port                           def: 49999
#	3 allow parallel jobs (1 yes/0 no)      def: 0

import sys       # used for various including CLI args
import os        # used to detect OS version
import socket    # used for communication with server
import time      # used for wait
import pickle    # used for serialization
import job as j  # used for Job running
from prettytable import PrettyTable # used for terminal printing

if __name__ == "__main__":
  
	SERVER_PORT = 49999
	SERVER_IP = "127.0.0.1"
	RECV_BUFFER = 4096  
	parallel = 0
	
	# Check if arguments provided
	if len(sys.argv) > 2:
		SERVER_IP = sys.argv[1]
		SERVER_PORT = int(sys.argv[2])
		try:
			parallel = int(sys.argv[3])
		except:
			# parallel jobs optional parameter
			pass
	print "setting up socket"
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.setblocking(0)
		s.connect((SERVER_IP, SERVER_PORT))
	except socket.error as msg:
		time.sleep(2)
	
	s.settimeout(2)
	# At this point, the connection to the server has been established
	jobQueue = [] # List of tuples (JobInstance, PercentageComplete)

	while 1:
		# Infinite loop to check for messages from server, respond to server, and initiate and monitor jobs
		time.sleep(2)
		# Update jobQueue details
		for i, jobTuple in enumerate(jobQueue):
			jobInstance, pc = jobTuple
			pc = jobInstance.getCompletionPercentage()
			jobQueue[i] = (jobInstance, pc)
		
		# Check for messages from server
		MFS = ""
		appending = 1
		while(appending > 0):
			try:
				#print "appending message from server"
				partial = s.recv(RECV_BUFFER)
				MFS = MFS + partial
				if(len(partial) < RECV_BUFFER):
					# nothing, or EOF detected
					break
			except:
				appending = 0

		if(len(MFS) > 0):
			# Message received from server, respond
			try:
				if(MFS == "stats"):
					# Stats requested from server, send a pickled jobQueue
					s.sendall(pickle.dumps(jobQueue))
				else:
					# Incoming job/s
					newJobs = pickle.loads(MFS)  # Expecting list of tuples (operation, JobInstance)
					for newJob in newJobs:
						if(newJob[0] == 1): #ADD
							# Add new jobs to the jobQueue
							print "adding job: {0}".format(newJob[1])
							jobQueue.append((newJob[1], 0))
							# In the case of a recycled Job from a previous client, reset job
							if(newJob[1].getCompletionPercentage() > 0):
								newJob[1].resetCompletionPercentage()
						if(newJob[0] == 0): #REMOVE
							# Server requesting to remove (completed) job from queue
							for i, jobTuple in enumerate(jobQueue):
								if(jobTuple[0] == newJob[1]): # Instance in queue matches requested remove instance
									jobQueue.remove(jobTuple)
									print "removing job: {0}".format(newJob[1].getId())
			except:
				print "Error processing message from server"
				
		if(parallel == 1):
			# All jobs run in parallel, multi-threaded
			for job in jobQueue:
				if(job[1] == 0): # Job hasn't been started yet
					job[0].run()
					print "Starting Job : {0}".format(job[0].getId())
		else:
			# Check if a job is running -> if not, start 1 job
			jobInProgress = 0
			for job in jobQueue:
				if(0 < job[1] < 100): # Job is running
					jobInProgress = 1
			if(jobInProgress == 0): # No active job, lets start one
				for job in jobQueue:
					if(job[1] == 0):
						job[0].run()
						print "Starting Job : {0}".format(job[0].getId())
						break
						
		# Print jobs list
		os.system('cls' if os.name == 'nt' else 'clear') # Clear terminal
		#  Client IP/Port, Job Instance, Completion %
		if(len(jobQueue) < 1):
			print "Waiting for jobs from server"
		else:
			table = PrettyTable(['Job ID', 'Completion %'])
			for i, jobTuple in enumerate(jobQueue):
				jobInstance, pc = jobTuple
				table.add_row([jobInstance.getId(), pc])
			print table
