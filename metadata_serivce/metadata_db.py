import sqlite3
import json
import threading
import os


class MetadataDBService:
    def __init__(self):
        self.db_file = 'metadata_table.db'  # SQLite database file
        self.table = 'metadata_table'  # SQLite table
        self.connection_lock = threading.Lock()
        self.create_table_if_not_exists()

    def get_connection(self):
        return sqlite3.connect(self.db_file)

    def create_table_if_not_exists(self):
        with self.connection_lock:
            conn = self.get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.table} (
                                    file_path TEXT PRIMARY KEY,
                                    primary_blocks TEXT,
                                    replica_blocks TEXT
                                  )''')
                conn.commit()
            finally:
                conn.close()

    def save_file_blocks(self, file_path, primary_blocks, replica_blocks):
        with self.connection_lock:
            conn = self.get_connection()
            try:
                cursor = conn.cursor()
                primary_blocks_serializable = json.dumps(
                    [list(primary_block) for primary_block in primary_blocks])
                replica_blocks_serializable = json.dumps(
                    [list(replica_block) for replica_block in replica_blocks])
                cursor.execute(f"INSERT OR REPLACE INTO {self.table} (file_path, primary_blocks, replica_blocks) VALUES (?, ?, ?)", (
                    file_path, primary_blocks_serializable, replica_blocks_serializable))
                conn.commit()
            finally:
                conn.close()

    def delete_file(self, destination):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM file_table WHERE file_path=?", (destination,))
            conn.commit()
        finally:
            conn.close()

    def get_file_blocks(self, file_path):
        with self.connection_lock:
            conn = self.get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    f"SELECT primary_blocks, replica_blocks FROM {self.table} WHERE file_path=?", (file_path,))
                result = cursor.fetchone()
                return json.dumps({'primary_block': result[0],
                                   'replica_block': result[1]
                                   }) if result else None
            finally:
                conn.close()
