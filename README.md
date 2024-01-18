# Yet Another Distributed File System (YADFS)

This distributed file system is designed to efficiently store, manage, and retrieve large files across multiple machines in a network.

## High-Level Architecture

The architecture of YADFS involves the implementation of data nodes and name nodes for the storage and management of data. The key components are:

### Name Nodes

- Manages metadata about files and directories.
- Maintains a namespace hierarchy and file-to-block mapping.
- Monitors the health and availability of Data Nodes.

#### Availability Check

- Periodically pings each Data Node.
- Data Nodes acknowledge the Name Node ping.

### Data Nodes

- Responsible for handling the reading and writing of data.
- Provides API for write and read operations on data blocks.
- Considers replication across Data Nodes with a replication factor of 3.

## Organizing Data in Data Nodes

- Data is stored in the form of files within folders.
- At least one root folder exists.
- Users can view a virtual tree of all folders and files.

## Note on Data Blocks

- Data blocks are used to store and manage large files efficiently.
- Each file is divided into fixed-size blocks distributed across Data Nodes.
- Metadata tracks the location of each block.

## Fault Tolerance

- Implements mechanisms to handle Data Node failures.
- Maintains multiple replicas of data blocks.
- Detects failed nodes and redistributes data blocks to healthy nodes.

## Client Interaction and Features

Develop a command line interface or web interface for interacting with YADFS, supporting the following actions:

### Metadata Operations

- Create, delete, move, and copy directories and files.
- List files and directories within a directory.
- Traverse directories.

### DFS Operations

- Upload and download files from YADFS.

## Process Flows

### Upload Process Flow

1. **File Splitting:** - The client divides the file into fixed-size blocks.

2. **Block Creation:** - The client assigns a unique identifier to each block.

3. **Uploading Blocks:** - Client sends data blocks to Data Nodes.

4. **Replication:** - System creates replicas for fault tolerance.

5. **Metadata Update:** - Client updates metadata with file information.

6. **Namespace Resolution:** - Client and file system determine block storage.

7. **Client Acknowledgment:** - Client receives acknowledgements from Data Nodes.

### Download Process Flow

1. **Client Request:** - Client requests file download from DFS.

2. **Metadata Retrieval:** - Client retrieves file information from Name Node.

3. **Block Location Retrieval:** - Client learns data block locations.

4. **Data Block Retrieval:** - Client retrieves data blocks from Data Nodes.

5. **Data Transfer:** - Data Nodes transfer blocks to the client.

6. **Reassembly:** - Client reassembles blocks into the original file.

7. **File Completion Check:** - Client checks successful retrieval of all data blocks.

8. **Cleanup:** - Client may delete temporary data and close connections.

### Metadata Operation Process Flow

1. **Client Request:** - Client sends operation request to Name Node.

2. **NameNode Verification:** - Name Node verifies the validity of the operation.

3. **NameNode Operation:** - Name Node operates and updates metadata.

4. **Client Response:** - The client receives a response from Name Node regarding operation status.


