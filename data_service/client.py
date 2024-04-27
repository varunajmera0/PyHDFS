import json
import os
import rpyc
import sys
import threading
import mmap


class FilesystemClient:
    def __init__(self, block_size):
        self.block_size = block_size

    def create_replicate_data_to_data_node(self, block_id, replica_nodes, data, destination):
        for replica_data_node in replica_nodes:
            # Use threading for asynchronous replication
            threading.Thread(target=self.send_to_data_node, args=(
                block_id, replica_data_node[1], data, destination, [])).start()

    def send_to_data_node(self, block_id, data_node, data, destination, replica_nodes=[]):
        try:
            with rpyc.connect(data_node[0], port=data_node[1]) as data_node_con:
                dn = data_node_con.root.DataNodeService()
                dn.store_block(block_id, data, destination)
                if len(replica_nodes) > 0:
                    self.create_replicate_data_to_data_node(
                        block_id, replica_nodes, data, destination)
                return True
        except Exception as e:
            print(f"Failed to send block to {data_node}: {e}")
            return False

    def read_from_data_node(self, block_id, data_node, replica_blocks, source_path):
        """1) first trying to get data from primary block otherwise from multiple replica"""
        try:
            with rpyc.connect(data_node[0], port=data_node[1]) as data_node_con:
                dn = data_node_con.root.DataNodeService()
                data = dn.get_block(block_id, source_path)
                if data:
                    return data
                raise Exception("Block is corrupted")
        except Exception as e:
            print(
                f"Failed to read block from primary data node {data_node}: {e}")
            for replica_block in replica_blocks:
                print(f"trying to read from replica {replica_block}")
                if replica_block[0] == block_id:
                    try:
                        with rpyc.connect(replica_block[1][0], port=replica_block[1][1]) as data_node_replica_con:
                            dnr = data_node_replica_con.root.DataNodeService()
                            return dnr.get_block(block_id, source_path)
                    except Exception as e:
                        print(f"error occurred at {e}")
            raise Exception("unable to retrieve block(s)")

    def create_file(self, filename, destination):
        with rpyc.connect(host, port) as name_node_conn:
            try:
                file_size = os.path.getsize(filename)
                primary_blocks, replica_nodes = name_node_conn.root.NameNodeService(
                ).create_blocks(destination, file_size)
                print("Primary Block Detail: ", primary_blocks)
                print("Replica Block Detail: ", replica_nodes)
                with open(filename, "rb") as f:
                    # Create a memory map for the entire file
                    """Using mmap for reading blocks can improve efficiency by avoiding the need to read the entire file into memory at once. It allows you to read specific portions of the file directly from the operating system's file cache, which can be beneficial for large files."""
                    with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                        for block_id, data_node in primary_blocks:
                            # Calculate the starting position of the block
                            start_pos = self.block_size
                            if self.block_size > file_size:
                                # Read the block directly from the memory-mapped file
                                data = mm[:]
                            else:
                                data = mm[start_pos:start_pos +
                                          self.block_size]
                            self.send_to_data_node(
                                block_id, data_node, data, destination, replica_nodes)
            except EOFError as e:
                print(f"Server was closed")
            except Exception as e:
                print(f"Error occurred: {e}")
                raise Exception(e)
            finally:
                print("Name node server connection closed.")

    def find_replica_block(self, replica_blocks, block_id):
        for replica_block in replica_blocks:
            if replica_block[0] == block_id:
                return replica_block

    def read_file(self, source_path):
        """1) get the data from primary block otherwise fetch it from replica
           2) if blocks_details is None it means there is no entry in metadata service
           3) data will be copied on multiple data nodes based on replication factor in config
        """
        with rpyc.connect(host, port) as name_node_conn:
            try:
                blocks_details = name_node_conn.root.NameNodeService().get_file_table_entry(source_path)
                # 2 times json dumps first time metadata then namenode
                blocks_details = json.loads(json.loads(blocks_details))
                if not blocks_details:
                    print("File not found")
                    raise Exception("File not found")
                content = ''
                # this json.loads because in metadata service we are saving as json.dumps
                primary_blocks = json.loads(blocks_details['primary_block'])
                replica_blocks = json.loads(blocks_details['replica_block'])
                for primary_block in primary_blocks:
                    block_id, data_node = primary_block
                    data = self.read_from_data_node(
                        block_id, data_node, replica_blocks, source_path)
                    if data:
                        content += data.decode('utf-8')
                    else:
                        print("No data found for block")
                return content
            except EOFError as e:
                print(f"Server was closed")
            except Exception as e:
                print(f"error occurred at read_file: {e}")
                raise Exception(e)
            finally:
                print("Name node server connection closed.")


if __name__ == "__main__":
    print("sys.argv", sys.argv)
    # if len(sys.argv) != 4:
    #     print("Usage: python client.py get <filename> <destination>")
    #     sys.exit(1)

    import configparser
    # Parse the configuration file
    config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config.read(f'{current_dir}/config.ini')
    name_node_server = config['name_node']['name_name_hosts'].split(':')
    block_size = int(config['name_node']['block_size'])
    print("name_node_server: ", name_node_server)
    host = name_node_server[0]
    port = int(name_node_server[1])

    name_node_client = FilesystemClient(block_size)

    if sys.argv[1] == "get":
        destination = sys.argv[2]
        print(name_node_client.read_file(destination))
    elif sys.argv[1] == "put":
        file = sys.argv[2]
        destination = sys.argv[3]
        name_node_client.create_file(file, destination)
    else:
        print("Error")
# python3 data_service/client.py put /Users/theflash/Desktop/s3/data_service/10mb-examplefile-com.txt /Users/theflash/Desktop/s3/data/tmp/dfs_data
# python3 data_service/client.py get /Users/theflash/Desktop/s3/data/tmp/dfs_data
# python3 data_service/name_node.py
# python3 data_service/data_node.py localhost 1801
# python3 data_service/data_node.py localhost 1802
# python3 metadata_serivce/metadata.py
# python3 zookeeper/zk.py
