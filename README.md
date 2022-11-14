# Project 2: Distributed Computation Engine

Please see: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-2.html



# Overview
This project is extended from Project1(https://github.com/usf-cs677-fa22/P1-vivian). Project 1 dealt with the storage aspect of big data while  this project shifted our focus to computing. In this project, I extended my DFS to support MapReduce jobs. Specific features include:\
* Datatype-aware chunk partitioning
* Job submission and monitoring, including pushing computations to nodes for data locality
* Load balancing across computation nodes
* The Map, Shuffle, and Reduce phases of computation


**Components**:
 - Controller: responsible for managing resources in the system.
 - Storage nodes: responsible for storing and retrieving file chunks.
 - Client: responsible for breaking files into chunks, asking the controller where to store them, and then sending them to the appropriate storage node(s). Also it is able to download/delete from the file system.
 - submit-job client: responsible for submitting job to Computation Manager.
 - Computation Manager: manage computations. For example, receive job submissions, determine relevant storage nodes based on the input file, and then transfer the job to the target nodes for execution.


## Design

### workflow:

**Client**-> **Controller**: "Put(upload)" file to DFS system.\
**Submit-job client:**: Submit job to computation manager.\
**Computation manager**: Connect to controller to get information of target file.\
**Computation manager**: Send job(including file's info, reducers' info and plugin) to mappers.\
**Mapper**: Accept job, responsible for map and shuffle. Once done map and shuffle, send immediate data to target reducer and send " job done" confirmation to computation manager.\
**Reducer**: Accept data from mappers and do "sort" and "reduce". Once done reduce, store its final output in the DFS system.

*Upload a file to the DFS system:*
```
go run client/client.go <controller-hostname> put <target-file-path> <destination-path>
```

*Submit job to Computation Manager:*
```
go run submit/submit_job.go <plugin-file-path> <input-file> <output-file> <computation manager hostname> <reducer amount>
```



### diagram:
![Image](https://user-images.githubusercontent.com/86545567/201746764-9417c7c2-30fd-4e88-bb41-bbc67f312cd4.png)




## Additional Notes:
1.Start Controller by running:
```
go run controller/controller.go
```

2.Start Storage node by running:
```
go run storage/storagenode.go <Storage path> <controller-hostname> <:Storagenode-path>
```
