# python-distributed-compute
Client/Server architechture used to distribute Job instances (work) to clients to run.  

Server is used to generate Job instances and manage a queue of jobs.  Jobs are then distributed to 1 or more clients running either locally or across the network.  Server will poll the clients for their local job queue's to update server statistics.  Server will request clients to add or remove jobs from their queue.

Client will run jobs either sequentially or in parallel depending on arguments.  Client will poll jobs for completion percentage to update local job queue statistics.

Job will initialize some values depending on the supplied arguments.  You can call run() on a job to kick off a worker thread and do the work within runInBackground().  Jobs are responsible for updating their own completion percentage.  Minimum requirement is to set completion percentage to 100 when done.  Client will use this to know when the job is complete.


Possible extensions:
Client can establish a message pipe with jobs if more than completion percentage is needed or coordination between jobs is required.
