import rpyc
from metadata_db import MetadataDBService

class MetadataService(rpyc.Service):
    def __init__(self):
        self.metadata_db = MetadataDBService()

    def exposed_save_file_blocks(self, file_path, primary_blocks, replica_blocks):
        return self.metadata_db.save_file_blocks(file_path, primary_blocks, replica_blocks)

    # def exposed_delete_file(self, destination):
    #     metadata_service = MetadataService()
    #     return metadata_service.delete_file(destination)
    #
    def exposed_get_file_blocks(self, file_path):
        return self.metadata_db.get_file_blocks(file_path)

# Start the server
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    metadata_service = ThreadedServer(MetadataService(), port=18005, auto_register=False)
    metadata_service.start()
