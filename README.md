# Distributed Data Analysis Platform


## Overview
This project has two main parts. First, I created a distributed file system using Go, allowing multiple storage nodes to manage data efficiently. It's similar to HDFS and supports parallel storage and retrieval, as well as fault tolerance. I improved data processing speed and system reliability by using chunking and parallel uploading techniques and replicating data across multiple machines. I also used Google Protocol Buffers for message serialization, making it easier for other applications to use the same format. Clients can interact with the system through command-line operations like storing, retrieving, deleting, listing files, and checking active nodes.

In the second part, I utilized this DFS to develop my own MapReduce implementation. I employed Go plugins (compiled shared object files) to encapsulate MapReduce jobs as plugin files, which were then distributed to the storage nodes for processing. These plugin files included the Map phase, Shuffle phase, and Reduce phase. Through the implementation of MapReduce, my DFS had the capability to analyze extensive datasets and extract valuable insights.

**Specific features include:**
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

### diagram:
![Image](https://user-images.githubusercontent.com/86545567/201746764-9417c7c2-30fd-4e88-bb41-bbc67f312cd4.png)

## How to run the project
*Upload a file to the DFS system:*
```
go run client/client.go <controller-hostname> put <target-file-path> <destination-path> -text <chunk-size>
```

*Submit job to Computation Manager:*
```
go run submit/submit_job.go <plugin-file-path> <input-file> <output-file> <computation manager hostname> <reducer amount>
```

*Download a file from the DFS system:*
```
go run client/client.go <controller-hostname> get <target-file-path> <destination-path>
```


## Additional Notes
1.Start cluster by running:
```
./start-cluster.sh
```

2.Stop cluster by running:
```
./stop-cluster.sh
```
