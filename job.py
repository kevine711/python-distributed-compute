# Author: Kevin Ersoy 

##### Script description
# This script contains the Job class.  Once an instance of Job is created, __init__ is run, and you can store
# whatever local vars you need from the arguments list.  The first argument is the instance itself (self).

# When you're ready to start the job, you call .run() on your instance.  Inside the Job class, this creates
# a worker thread to handle the job and calls runInBackground on that thread.  All "work" should be performed in
# runInBackground.  Throughout your job, you can update self.completionPercentage at will.  The creator of the Job
# instance can check the percentageComplete via the getter.


import time
import threading

class Job:
	workDetails = 0
	duration = 0
	completionPercentage = 0
	id = 0
	def __init__(*args):
		#args[0] is self
		try:
			args[0].completionPercentage = 0
			args[0].id = int(args[1])
			args[0].duration = int(args[1])
			args[0].workDetails = int(args[2])
		except:
			args[0].duration = 5
			args[0].workDetails = 5

	def runInBackground(self):
		startDuration = self.workDetails
		while(self.workDetails > 0):
			time.sleep(1)
			self.workDetails -= 1
			self.completionPercentage = 100 - (float(self.workDetails)/float(startDuration))*float(100)
	
	def run(self):
		self.completionPercentage = 1 # Set > 0 to indicate to owner that job was started
		t1 = threading.Thread(target=self.runInBackground)
		t1.start()
		
	def getCompletionPercentage(self):
		return self.completionPercentage
	
	def resetCompletionPercentage(self):
		self.completionPercentage = 0
	
	def getId(self):
		return self.id

	def __eq__(self, other):
		return self.id == other.id
 
	def __ne__(self, other):
		return self.id != other.id	
		
	
if __name__ == '__main__':
	j = Job(7, 7)
	j.run()