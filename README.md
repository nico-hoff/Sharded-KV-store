# Task 2 - Sharded KV store

In the previous task you implemented a single node key value store that serves GET/PUT requests from a client. In this task you will implement a sharding protocol that distributes keys and their values across several key-value servers.
You will extend the client and server that was built during task 1.

### Master
The master is a single-threaded, single process application that serves the following functions:

- Listens to new key-value servers that are ready to join the cluster.
- Distribute the keys and consequently their values across the servers in the cluster.
- Serve client requests to find server (shard) containing the corresponding key.
- When a new server joins the cluster, redistribute the keys and values across the new set of servers in the cluster.

The master process is to be run as follows for the tests to succeeded:
```
./build/dev/master-svr -p <MASTER_PORT>
``` 

#### Parameter description

- MASTER_PORT : port at which the master listens to for client requests and new servers joining the cluster.

### Client
The client for this task executes the workload (`PUT`/`GET` requests). 
The client is a single-threaded, single-process application and is run only once, i.e., sends a `PUT` or `GET` request and then terminates.

The client process is to be run as follows for the tests to succeed:
```
./build/dev/clt -p <PORT> -o <OPERATION> -k <KEY> -v <VALUE> -m <MASTER_PORT> -d <DIRECT>
``` 

#### Parameter description

- PORT : port at which the target server listens to. This parameter should only be valid when DIRECT is set to `1`.
- OPERATION : either a GET or PUT request. The testing script will specify the operations in uppercase characters.
- KEY : key for the operation
- VALUE : value for the operation corresponding to the key. Only valid if the OPERATION is PUT.
- MASTER_PORT : Port at which the master listens to for the client.
- DIRECT : Specifies whether the client can talk to the server at port PORT. It is **important** that the implementation of your client can talk directly to server at PORT. It is set to `0` meaning false, or `1` meaning true i.e. the client talks to the server directly without the help from master.

#### Return values

The client should return the following values:

- 0 : for a successful `PUT` or `GET` operation.
- 1 : `PUT` or `GET` failed.
- 2 : `GET` failure as key doesn't exist

### Server

The server is a single-threaded, single-process application that performs the following functions:

- On startup, the server contacts the master server to join the cluster.
- Responds to a client GET/PUT request.

The master process is to be run as follows for the tests to succeeded:
```
./build/dev/svr -p <PORT> -m <MASTER_PORT>
``` 

#### Parameter description

- PORT : port at which the server listens to client or master requests
- MASTER_PORT : port at which the master server is listening

### Things to note

- Names of the executables must be the same (clt, svr, master-svr)
- All of the processes run locally (localhost address)
- Pay special attention to the option names

## Build the code

```
./build.sh
```

We add no new dependencies from task 1, so this should work with minimum problems.

## Tests

### Test 1 - Single shard

This test checks if a single shard i.e. a single key-value server is able to serve `PUT` or `GET` requests from a client. 

### Test 2 - Two shards

This test checks if a two shards i.e. a single key-value servers are able to serve `PUT` or `GET` requests from a client. 

#### Implementation hint

The master provides the client with the location of the shard/server that is supposed to store the value.

### Test 3 - Test sharding

This test checks if sharding has been implemented correctly.

#### Implementation hint

The master provides the client with the location of the shard/server that is supposed to store the value. The master can do this by using a simple hash function to map the key to its corresponding shard as follows:
```
shard = (key % num_servers) +1
```

### Test 4 - Test new server joining

This test checks if the master is able to add a new server to the cluster and redistribute keys to their shards.

#### Implementation hint

The master needs to make sure that keys are redistributed across all servers, including the new one.

### Important note:
The master, server and client are not executed in docker containers as in task 1, but rather as simple processes within the CI container.
