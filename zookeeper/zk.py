import threading
import time
import pickle
import rpyc


class ZooKeeper(rpyc.Service):
    def __init__(self):
        self.lock = threading.Lock()
        self.watched_nodes = {}
        self.expiry_check_interval = 6  # Interval in seconds for checking node expiry
        self.start_expiry_checker()

    def start_expiry_checker(self):
        threading.Thread(target=self.check_node_expiry_loop,
                         daemon=True).start()

    def check_node_expiry_loop(self):
        try:
            while True:
                self.remove_expired_nodes()
                time.sleep(self.expiry_check_interval)
        except Exception as e:
            print(f"Error occurred at check_node_expiry_loop: {e}")
            self.check_node_expiry_loop()

    def remove_expired_nodes(self):
        try:
            current_time = time.time()
            expired_nodes = []
            with self.lock:
                # Use list() to avoid concurrent modification
                for path, data in list(self.watched_nodes.items()):
                    data_loads = pickle.loads(data)
                    if 'timestamp' in data_loads:
                        node_timestamp = data_loads['timestamp']
                        if current_time - node_timestamp > 5:
                            # Node has expired, remove it from the watched list
                            expired_nodes.append(path)
                            del self.watched_nodes[path]
            print("Expired nodes:", expired_nodes)
        except Exception as e:
            print(f"Error occurred at remove_expired_nodes: {e}")
            self.check_node_expiry_loop()

    def exposed_create_node(self, path, data=None, ephemeral=False):
        with self.lock:
            data = data or {}
            # If data is provided as bytes, decode it into a dictionary
            if isinstance(data, bytes):
                try:
                    data = pickle.loads(data)
                except pickle.UnpicklingError:
                    # Handle unpickling errors, e.g., invalid data format
                    raise ValueError(
                        "Invalid data format. Expected pickled dictionary.")
            """If ephemeral is False in the create_node method of the ZooKeeperClient, 
                           it means that the node being created is not ephemeral. In this case, we won't attach any session ID or
                            ephemeral information to the node's data."""
            if path not in self.watched_nodes:
                if ephemeral:
                    pass
                    # session_id = threading.current_thread().ident  # Simulating session ID
                    # data["session_id"] = session_id
                    print(f"data: {data}")
                self.watched_nodes[path] = data

    def exposed_set_data(self, path, data):
        with self.lock:
            print("Data node server signal: ", data)
            if path in self.watched_nodes:
                self.watched_nodes[path] = data
            else:
                raise ValueError(f"Node does not exist: {path}")

    def exposed_get_data(self, path):
        with self.lock:
            if path in self.watched_nodes:
                return self.watched_nodes[path]
            else:
                return None

    def exposed_get_all_data_nodes(self, node_path):
        data_nodes = []
        with self.lock:
            for path, data in self.watched_nodes.items():
                if path.startswith(node_path):
                    data_loads = pickle.loads(data)
                    print(f"Data node details: {data_loads}", )
                    data_nodes.append((data_loads['host'], data_loads['port']))
            return data_nodes


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    print("ZooKeeper server started")
    zk_server = ThreadedServer(ZooKeeper(), port=18861)
    zk_server.start()
