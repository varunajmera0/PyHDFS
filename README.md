# **PyHDFS Overview**

<aside>
💡 This project sounds like a great way to delve into the world of distributed file systems (DFS)! Let's break down how DFS works under the hood, assuming you're familiar with Hadoop's functionality. PyHDFS, which utilizes RPyC for remote procedure calls (RPC), consists of five key components:

</aside>

- **Zookeeper (Custom)**: This acts as a lightweight coordination service. It tracks data node registrations, removes inactive nodes (those not sending heartbeats), and provides the namenode with live data node information upon request.
- **Metadata Database**: This database stores metadata information received from the namenode. It essentially acts as a central repository for file system information.
- **Namenode (Master Machine)**: This is the central authority, managing file system metadata and allocating data blocks across the cluster. It essentially orchestrates the entire file system.
- **Datanode (Slave Machine)**: These are worker nodes responsible for storing actual file chunks based on the configured block size. They passively wait for instructions from the namenode.
- **Client (User Entry Point)**: This is the interface users interact with. It communicates with the namenode to retrieve metadata, locates the relevant datanodes, and sends files for storage or retrieval.

<aside>
🏗️ Scalability and resilience are two major strengths of distributed file systems like PyHDFS.

</aside>

Scalability through Data Nodes:

---

Adding more datanodes to the cluster is a straightforward way to scale storage capacity. PyHDFS allows you to seamlessly add new datanodes, which automatically register with Zookeeper. This increases the total storage available to the file system.

**Resilience through Data Replication:**

---

Data replication across multiple datanodes is a key factor in ensuring data resilience. By storing copies of your data on multiple machines, PyHDFS ensures that even if a datanode fails, your data remains accessible from the other replicas. Zookeeper's role in maintaining an up-to-date list of active datanodes further strengthens this resilience by allowing the system to quickly identify and route requests around failed nodes.

<aside>
📚 The flow of PyHDFS involves a series of steps to initialize and start its various components before accessing the distributed file system. Here's the flow:

</aside>

1. Start Zookeeper:
    - Zookeeper tracks data node registration.
    - Removes inactive data nodes.
    - Provides live data node information to the name node upon request.
2. Start DataNode(s):
    - Data nodes store physical file chunks.
    - Multiple data nodes can be started for scalability.
3. Start NameNode:
    - The master machine managing metadata and block allocation.
    - Responsible for coordinating file operations.
4. Start Metadata Service:
    - Metadata service captures and stores metadata information received from the name node.
    - Stores information about files, such as file names and locations.
5. Access via Client:
    - The client serves as the entry point for users.
    - Interacts with the name node to perform file operations.
    - Connects with corresponding data nodes to read from or write data to the distributed file system.

<aside>
🏬 **PyHDFS Architecture**

</aside>

![PyHDFS Architecture](/doc/PyHDFS.svg "PyHDFS Architecture")

<aside>
☁️ Running PyHDFS on AWS EC2

</aside>

This guide outlines how to set up and run PyHDFS on an AWS EC2 instance.

> **Prerequisites:**
> 
> - Local machine with Git and Python 3.9+ installed
> - AWS account with an EC2 instance running Amazon Linux

👉 Steps:

1. **Clone the PyHDFS repository:**
    
    Clone the PyHDFS repository to your local machine using Git.
    
2. Set up the EC2 machine (if using Amazon Linux):
Run the script `sh install-python-3.9-on-centos.sh`
    
    > **install-python-3.9-on-centos.sh**
    > 
    
    ```bash
    #!/bin/sh
    sudo yum install gcc openssl-devel bzip2-devel libffi-devel zlib-devel
    cd /tmp
    wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
    tar -xvf Python-3.9.6.tgz
    cd Python-3.9.6
    ./configure --enable-optimizations
    sudo make altinstall
    python3.9 --version
    sudo yum install python3-pip
    ```
    
3. **Install Dependencies**: Install RPyC and create an app folder:
    
    ```bash
    
    pip3 install rpyc
    mkdir app
    ```
    
4. **Create Configuration File**: Create a **`config.ini`** file in the **`app/data_node`** folder for both AWS. Adjust the IP addresses and ports as needed:
    
    ```bash
    [name_node]
    block_size = 134217728
    replication_factor = 2
    name_name_hosts = localhost:1800
    
    [data_node]
    data_node_hosts = 100.25.217.45:1801,localhost:1802
    data_node_dir_1801 = /path/to/dfs_data/1801
    data_node_dir_1802 = /path/to/dfs_data/1802
    
    [metadata]
    metadata_hosts = localhost:18005
    
    [zookeeper]
    zookeeper_hosts = 100.25.217.45:18861
    
    ```
    
5. **Copy Files (Only these 2 files are required. If you want you can copy entire repository)**:
    - Create **`data_service/data_node.py`** in the **`app`** folder and copy the content from the repository.
    - Create **`zookeeper/zk.py`** in the **`app`** folder and copy the content from the repository.
6. **Configure Security Group**:
    1. **Go to the AWS Management Console**.
    2. **Navigate to the EC2 dashboard**.
    3. **Select the EC2 instance** that is running your RPyC service.
    4. **Under the "Description" tab**, find the "Security groups" section and click on the security group associated with your instance.
    5. **Click on the "Inbound rules" tab** and then click "Edit inbound rules".
    6. **Add a new rule** to allow traffic on the port your RPyC service is using. For example, if your RPyC service is using port 18812, add a rule with the following settings:
        - Type: Custom TCP Rule
        - Protocol: TCP
        - Port Range: 18812 (or the port your RPyC service is using)
        - Source: Custom IP, and enter the IP address or range from which you want to allow inbound traffic. If you want to allow traffic from any IP address, you can use **0.0.0.0/0**, but this is less secure.
    7. **Click "Save rules"** to apply the changes.
    
    ![Screenshot 2024-04-27 at 7.16.20 PM.png](/doc/Screenshot_2024-04-27_at_7.16.20_PM.png)
    
7. **Start Services**:
    - Start Zookeeper on AWS:
        
        ```bash
        python3 zookeeper/zk.py
        ```
        
        ![Screenshot 2024-04-27 at 11.25.32 PM.png](/doc/Screenshot_2024-04-27_at_11.25.32_PM.png)
        
    - Start DataNodes(AWS + Local System):
        
        Run DataNodes on both AWS and the local system for scalability. You can add more DataNodes in the **`config.ini`** file and specify their indices accordingly.
        Example:
        
        ```bash
        python3 data_service/data_node.py 0 -> on AWS
        python3 data_service/data_node.py 1 -> on Local System/it can also be on AWS.
        
        ```
        
        In the **`config.ini`** file, **`data_node_hosts`** is set to **`100.25.217.45:1801,localhost:1802`**, where **`0`** represents **`100.25.217.45:1801`** and **`1`** represents **`localhost:1802`**.
        
        You can choose where to run the DataNode instances based on your specific requirements and infrastructure setup. If you decide to run both DataNode instances on AWS, you would need to ensure that the **`config.ini`** file reflects the public IP address of the AWS EC2 instance for the appropriate DataNode host entry.
        
        For instance, if both DataNode instances are on AWS, your **`config.ini`** might look like this:
        
        ```bash
        [data_node]
        data_node_hosts = 100.25.217.45:1801,100.25.217.45:1802
        data_node_dir_1801 = /path/to/dfs_data/1801
        data_node_dir_1802 = /path/to/dfs_data/1802
        ```
        
        Where **`100.25.217.45`** represents the public IP address of your AWS EC2 instance. Ensure that the appropriate security group settings are configured to allow communication between the instances as well.
        
        ![Screenshot 2024-04-27 at 11.26.23 PM.png](/doc/Screenshot_2024-04-27_at_11.26.23_PM.png)
        
    - Start NameNode on Local System:
        
        ```bash
        python3 data_service/name_node.py
        ```
        
    - Start Metadata Service on Local System:
        
        ```bash
        python3 metadata_serivce/metadata.py
        ```
        
8. **Store and Retrieve Files on Local System**:
    - Use the client to store and retrieve files:
        
        ```bash
        python3 data_service/client.py put <file_path>/<filename> <destination>
        python3 data_service/client.py get <destination>
        ```
        
        Example -`python3 data_service/client.py put /Users/theflash/Desktop/s3/data_service/10mb-examplefile-com.txt /Users/theflash/Desktop/s3/data/tmp/dfs_data`
        Example - `python3 data_service/client.py get /Users/theflash/Desktop/s3/data/tmp/dfs_data`
        
9. **Stop Services**: Use Ctrl + C to stop services and dump the namespace.

<aside>
💻 Running PyHDFS Locally

</aside>

This guide walks you through setting up and running PyHDFS on your local machine.

> **Prerequisites:**
> 
> - Local machine with Git and Python 3.9+ installed

👉 Steps:

1. **Clone the PyHDFS repository:**
    
    Clone the PyHDFS repository to your local machine using Git.
    
2. **Install Dependencies**: Install RPyC
    
    ```bash
    pip3 install rpyc
    ```
    
3. Edit config.ini (if necessary):
    - **Note:** Since everything runs locally, you can likely keep the default hostnames (`localhost`) for most settings.
        
        ```bash
        [name_node]
        block_size = 134217728
        replication_factor = 2
        name_name_hosts = localhost:1800
        
        [data_node]
        data_node_hosts = localhost:1801,localhost:1802
        data_node_dir_1801 = /Users/theflash/Desktop/s3/data/tmp/dfs_data/1801
        data_node_dir_1802 = /Users/theflash/Desktop/s3/data/tmp/dfs_data/1802
        
        [metadata]
        metadata_hosts = localhost:18005
        
        [zookeeper]
        zookeeper_hosts = localhost:18861
        ```
        
4. **Start PyHDFS services:**
    - Open a terminal window.
    - Navigate to your PyHDFS project directory using the `cd` command.
5. **Start services in this order:**
    1. **Zookeeper:**
        
        `python3 zookeeper/zk.py`
        
    2. **DataNode(s):**
        - You can run multiple DataNodes for local testing.
        - Start each DataNode with its corresponding index from `data_node_hosts` in `config.ini`:
        `python3 data_service/data_node.py 0  # for the first DataNode
         python3 data_service/data_node.py 1  # for the second DataNode (and so on)`
    3. **NameNode:**
        
        `python3 data_service/name_node.py`
        
    4. **Metadata service:**
        
        `python3 metadata_service/metadata.py`
        
    - **Interact with PyHDFS:**
        - Use the client script to store and retrieve files:
            - Store a file: `python3 data_service/client.py put <source_file_path> <destination_path>`
            - Retrieve a file: `python3 data_service/client.py get <destination_path>`
    - **Stop PyHDFS:**
        - Use `Ctrl+C` to stop each service individually. This will dump the namespace.

<aside>
💻 PyHDFS TODO List:

</aside>

This list outlines enhancements for PyHDFS:

- **Implement Delete:** Currently, PyHDFS lacks functionality to delete files. This feature would allow users to remove data from the file system.
- **Namenode Heartbeat:** The Namenode should periodically send heartbeat messages to DataNodes. DataNodes can use these heartbeats to detect failures and maintain the health of the system.
- **Standby Secondary Namenode:** Introducing a standby secondary namenode would enhance fault tolerance. In case of a primary namenode failure, the secondary can take over, minimizing downtime.
- **Datanode Block Reports:** DataNodes should regularly report block information (availability, usage) to the namenode. This enables the namenode to maintain an accurate view of data distribution and facilitates tasks like load balancing and replication.
- **Metadata Update on Successful Write:** Currently, metadata updates might occur before the write operation finishes successfully. To ensure data consistency, updates should only be committed to the metadata service after successful data storage on DataNodes.

These improvements would elevate PyHDFS's functionality and robustness. Consider prioritizing them based on your project's specific needs.