import hashlib
import json
import math
import os
import uuid
import rpyc
import time
import threading
import random


class DirectoryExistsError(Exception):
    pass


class NameNodeService(rpyc.Service):
    class exposed_NameNodeService():
        data_node_connections = []
        block_size = 0
        metadata_servers = []

        def __init__(self):
            self.zk = rpyc.connect(zk_servers[0], int(
                zk_servers[1]))  # Connect to ZooKeeper
            self.data_node_watcher()  # Start the watcher for data nodes

        def exposed_stop(self):
            print("name_node_service", name_node_service)
            # name_node_service.close()

        def data_node_watcher(self):
            def watch():
                last_data = None
                while True:
                    data = self.zk.root.exposed_get_all_data_nodes(
                        "/data_nodes")
                    print("exposed_get_all_data_nodes", data)
                    if data != last_data:
                        self.update_data_node_connections(data)
                        last_data = data
                    time.sleep(5)  # Simulate watching by sleeping

            threading.Thread(target=watch, daemon=True).start()

        def update_data_node_connections(self, data_nodes):
            self.__class__.data_node_connections = data_nodes
            print("Updated data node connections:", data_nodes)

        def calc_num_blocks(self, file_size):
            return int(math.ceil(float(file_size) / self.__class__.block_size))

        def get_all_data_nodes(self):
            # Get all data nodes from the DataNodeService
            return self.__class__.data_node_connections

        def select_primary_data_node(self, block_id, all_data_nodes):
            # Implement your logic to select the primary data node using consistent hashing or any other method
            # Convert block_id UUID to a unique integer using hash function
            print("remaining data nodes: ", all_data_nodes)
            block_id_int = int(hashlib.md5(block_id.encode()).hexdigest(), 16)
            index = block_id_int % len(all_data_nodes)
            return all_data_nodes[index]

        def allocation_blocks(self, destination, num_blocks):
            with rpyc.connect(self.__class__.metadata_servers[0], int(self.__class__.metadata_servers[1])) as metadata_server:
                try:
                    primary_blocks = []
                    replica_blocks = []
                    all_data_nodes = remaining_data_nodes = self.get_all_data_nodes()
                    """if remaining_data_nodes does not take then all_data_nodes 
                    will not get primary_data_node as replica data node in next iteration
                    all_data_nodes [('localhost', 1801), ('localhost', 1802)]
                    Data node index:  0
                    all_data_nodes [('localhost', 1802)]
                    Data node index:  0
                    all_data_nodes [('localhost', 1802)]
                    Data node index:  0
                    all_data_nodes []
                    """
                    print(f"{'*'*25}Starting allocation blocks{'*'*25}")
                    for i in range(0, num_blocks):
                        block_id = str(uuid.uuid4())
                        primary_data_node = self.select_primary_data_node(
                            block_id, remaining_data_nodes)
                        primary_blocks.append((block_id, primary_data_node))
                        if replication_factor != 0 and len(self.__class__.data_node_connections) >= 2:
                            # Replicate block to other data nodes
                            replica_nodes = [
                                node for node in all_data_nodes if node != primary_data_node]
                            for rf in range(replication_factor-1):
                                replica_data_node = self.select_primary_data_node(
                                    block_id, replica_nodes)
                                replica_blocks.append(
                                    (block_id, replica_data_node))
                                # excluding primary data node so next block should not go to same machine
                                remaining_data_nodes = replica_nodes
                        else:
                            print(
                                f"Replicating data is not possible either replication_factor({replication_factor}) or availability of data node is {len(self.__class__.data_node_connections)}.")
                        print(
                            f"primary_blocks: {primary_blocks} \nreplica_blocks: {replica_blocks}")
                    metadata_server.root.save_file_blocks(
                        destination, primary_blocks, replica_blocks)
                    print(f"{'*'*25}Finished allocation blocks{'*'*25}")
                    return primary_blocks, replica_blocks
                except Exception as e:
                    print(f"error occurred at allocation_blocks: {e}")
                    raise Exception(e)
                finally:
                    print("closed metadata service....")
                    # metadata_server.close()

        def exposed_get_file_table_entry(self, fname):
            with rpyc.connect(self.__class__.metadata_servers[0], int(self.__class__.metadata_servers[1])) as metadata_server:
                try:
                    blocks_detail = metadata_server.root.get_file_blocks(fname)
                    print("blocks_detail", blocks_detail)
                    if len(blocks_detail) > 0:
                        return json.dumps(blocks_detail)
                    return None
                except Exception as e:
                    print(
                        f"error occurred at exposed_get_file_table_entry: {e}")
                    raise Exception(e)
                finally:
                    print("closed metadata service....")
                    # metadata_server.close()

        def check_directory(self, directory):
            return os.path.exists(directory)

        def exposed_create_blocks(self, destination, file_size):
            try:
                if self.check_directory(destination):
                    raise DirectoryExistsError(
                        f"The directory '{destination}' already exists.")
                num_blocks = self.calc_num_blocks(file_size)
                primary_blocks, replica_nodes = self.allocation_blocks(
                    destination, num_blocks)
                return primary_blocks, replica_nodes
            except Exception as e:
                print(f"error occurred at exposed_create_file: {e}")
                raise Exception(e)


# Start the server
if __name__ == "__main__":
    try:
        # Parse the configuration file
        import configparser
        config = configparser.ConfigParser()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config.read(f'{current_dir}/config.ini')
        data_node_servers = config['data_node']['data_node_hosts'].split(',')
        block_size = int(config['name_node']['block_size'])
        metadata_servers = config['metadata']['metadata_hosts'].split(':')
        zk_servers = config['zookeeper']['zookeeper_hosts'].split(':')
        replication_factor = int(config['name_node']['replication_factor'])
        # Extract host-port pairs
        data_node_servers_detail = [(host, int(
            port)) for data_node_server in data_node_servers for host, port in [data_node_server.split(":")]]
        print(f"{'*' * 25}Started Setting up configs{'*' * 25}")
        print(
            f"""Data node server details: {data_node_servers_detail}\nblock_size: {block_size} bytes\nmetadata_servers: {metadata_servers}""")
        NameNodeService.exposed_NameNodeService.data_node_connections = data_node_servers_detail
        NameNodeService.exposed_NameNodeService.block_size = block_size
        NameNodeService.exposed_NameNodeService.metadata_servers = metadata_servers
        NameNodeService.exposed_NameNodeService()
        print(f"{'*'*25}Finished Setting up configs{'*'*25}")
        from rpyc.utils.server import ThreadedServer
        print("Name node server started")
        name_node_service = ThreadedServer(
            NameNodeService, port=1800, auto_register=False)
        name_node_service.start()
        print("Name node server closed")
    except Exception as e:
        print(f"Exception occurred at Name Node Service: {e}")
        raise Exception(e)
