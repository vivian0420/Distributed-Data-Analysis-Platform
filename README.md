# Project 1: Distributed File System

Please see: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html


# Overview
This is a distributed file system (DFS) based on the technologies from Amazon, Google, and others. It supports multiple storage nodes responsible for managing data. Key features include: Parallel retrievals, Replication, Interoperability and Persistence.
The whole implementation is done in Go. Communication between components is implemented via sockets.


**Components**:
 - Controller: responsible for managing resources in the system.
 - Storage nodes: responsible for storing and retrieving file chunks.
 - Client: responsible for breaking files into chunks, asking the controller where to store them, and then sending them to the appropriate storage node(s). Also it is able to download/delete from the file system.


## Design

### 1. Putting(Uploading) data to the DFS system

### workflow:
**Client**-> **Controller**: "Put(upload)" file to DFS request.\
**Controller**: Check if this file has already existed. If yes, reject the client's request.If not, randomly assign 3 storage nodes for each chunk of the file for storing. Send back to the client.\
**Client**: Connect to each storage node for uploading chunks and assign storage nodes to replicate chunks.\
**Storage nodes**: Save chunks that it received and replicate chunks to the other specified nodes.

*Uploading a file to the DFS system:*
```
go run client/client.go <controller-hostname> put <source-file-path-in-local> <destination-path-in-controller> <chunk-size(int)>
```
### diagram:
![Image](https://user-images.githubusercontent.com/86545567/193107610-183d6af4-7920-485e-b251-678426347dd1.png)

### 2. Getting(Retrieving) data from the DFS system

### workflow:
**Client**-> **Controller**: "Get(retrieve)" file from DFS request.\
**Controller**: Check if this file has already existed. If not, reject the client's request.If yes, reply a set of storage nodes that store chunks of this file to the client.\
**Client**: Connect to storage nodes for downloading chunks in parallel. If a client is not able to connect to the storage node that stores the origin chunk, it will try to connect a replica.\
**Storage nodes**: send out the order and content of the chunk that the client requests.

*Downloading a file from the DFS system:*
```
go run client/client.go <controller-hostname> get <source-file-path-in-controller> <destination-path-in-local>
```

### diagram:
![Image](https://user-images.githubusercontent.com/86545567/193108208-94882132-56d5-4583-950e-41aa556e1446.png)

### 3. Deleting data from the DFS system

### workflow:
**Client**-> **Controller**: "Delete" file from DFS request.\
**Controller**: Check if this file has already existed. If not, reject the client's request.If yes, reply "yes" to the client and send out delete requests to storage nodes.\
**Storage nodes**: Check if the path exists or not. If yes, delete the whole path, otherwise, do nothing.

*Downloading a file from the DFS system:*
```
go run client/client.go <controller-hostname> delete <target-file-path-in-controller>
```
### diagram:
![Image](https://user-images.githubusercontent.com/86545567/193118319-b68549b5-c1ac-4d44-b133-60a93fe27076.png)

### 4. Browsing the file system tree (ls command)
### workflow:
**Client**-> **Controller**: "ls" request.\
**Controller**: Check if this file has already existed. If not, reject the client's request.If yes, reply "yes" and send out a list of file/directory names, which under the path that the client specified to the client.\
**Client**: Print out the list.

*Browsing the file system tree:*
```
go run client/client.go <controller-hostname> ls <target-path-in-controller>
```

### 5. Viewing the activated node list
### workflow:
**Client**-> **Controller**: "list activated node" request.\
**Controller**: Send out a list of activated nodes with their information including "free space", "number of requests handled"by each node.\
**Client**: Print out the activated-node list.

*Viewing the activated node list:*
```
go run client/client.go <controller-hostname> listnode
```


## Additional Notes:
1.Start cluster by running:
```
./start-cluster.sh
```

2.Stop cluster by running:
```
./stop-cluster.sh
```

