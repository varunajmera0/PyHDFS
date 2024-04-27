import mmap
import os
import pickle
import time
import threading
import uuid

import rpyc
import sys

# DATA_DIR = '/Users/theflash/Desktop/s3/data/tmp/dfs_data'


class DataNodeService(rpyc.Service):
    class exposed_DataNodeService():
        data_dir = ''

        def __init__(self):
            self.blocks = {}  # Dictionary to store blocks (block_id: data)
            self.session_id = str(uuid.uuid4())   # if machine is working
            self.zk = rpyc.connect(zk_servers[0], int(
                zk_servers[1]))  # Connect to ZooKeeper
            self.register_with_zookeeper()  # Register DataNode with ZooKeeper
            self.start_heartbeat()  # Start sending heartbeats to the NameNode

        def exposed_stop(self):
            data_node_service.close()

        def register_with_zookeeper(self):
            data_node_info = {'host': host, 'port': port,
                              'timestamp': time.time(), 'session_id': self.session_id}
            self.zk.root.create_node(
                f"/data_nodes/{host}:{port}", pickle.dumps(data_node_info), ephemeral=True)

        def start_heartbeat(self):
            def heartbeat():
                while True:
                    data_node_info = {'host': host, 'port': port, 'timestamp': time.time(
                    ), 'session_id': self.session_id}
                    self.zk.root.set_data(
                        f"/data_nodes/{host}:{port}", pickle.dumps(data_node_info))
                    time.sleep(5)  # Send heartbeat every 5 seconds
            threading.Thread(target=heartbeat, daemon=True).start()

        def exposed_store_block(self, block_id, data, destination):
            try:
                print(f"block_id {block_id} DATA_DIR {destination}")
                path = f"{os.getcwd()}/{destination}/{port}/"
                if not os.path.isdir(path):
                    os.makedirs(path, exist_ok=True)
                # Write data to a file named after the block ID
                with open(f"{os.path.join(path, block_id)}.txt", 'wb') as f:
                    # Create a memory map for writing
                    f.write(data)
                self.blocks[block_id] = data
                return "Block stored successfully"
            except Exception as e:
                return f"Failed to store block: {e}"

        def exposed_get_block(self, block_id, source_path):
            print("block_id: ", block_id)
            try:
                path = f"{os.getcwd()}/{source_path}/{port}/"
                with open(f"{os.path.join(path, block_id)}.txt", 'rb') as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        # Return the contents of the memory-mapped file
                        return mm[:]
            except FileNotFoundError:
                return None


# Start the server
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_node.py data node index")
        sys.exit(1)

    data_node_index = int(sys.argv[1])

    import configparser
    config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config.read(f'{current_dir}/config.ini')
    # data_node_dir = config['data_node'][f'data_node_dir_{sys.argv[2]}']
    data_node_servers = config['data_node']['data_node_hosts'].split(',')
    data_node_server = data_node_servers[data_node_index].split(":")
    zk_servers = config['zookeeper']['zookeeper_hosts'].split(':')
    host = data_node_server[0]
    port = int(data_node_server[1])
    print(f"DataNode server started at {host}:{port}")
    DataNodeService.exposed_DataNodeService()
    from rpyc.utils.server import ThreadedServer
    data_node_service = ThreadedServer(DataNodeService, port=port)
    data_node_service.start()
    print("DataNode server closed")
