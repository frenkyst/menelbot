
import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

class LocalDataStore:
    def __init__(self, loop, user_data_file='user_data.json', scan_status_file='scan_status.json'):
        print("💾 [DataStore] Initializing local data store...")
        self.user_data_file = user_data_file
        self.scan_status_file = scan_status_file
        self.user_data = {}
        self.scan_status = {}
        self.loop = loop
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._dirty = False
        self.load_data()
        print("✅ [DataStore] Data store initialized and loaded.")

    def load_data(self):
        """Loads data from JSON files into memory, handling empty or corrupt files."""
        print("   [DataStore] Loading data from disk...")
        
        # Load user_data.json
        if os.path.exists(self.user_data_file) and os.path.getsize(self.user_data_file) > 0:
            try:
                with open(self.user_data_file, 'r') as f:
                    self.user_data = json.load(f)
                print(f"   [DataStore] Loaded {len(self.user_data)} users from {self.user_data_file}")
            except json.JSONDecodeError:
                print(f"   [DataStore] WARNING: {self.user_data_file} is corrupted or empty. Starting with fresh data.")
                self.user_data = {}
        else:
            print(f"   [DataStore] {self.user_data_file} not found or is empty. Starting fresh.")

        # Load scan_status.json
        if os.path.exists(self.scan_status_file) and os.path.getsize(self.scan_status_file) > 0:
            try:
                with open(self.scan_status_file, 'r') as f:
                    self.scan_status = json.load(f)
                print(f"   [DataStore] Loaded {len(self.scan_status)} scan statuses from {self.scan_status_file}")
            except json.JSONDecodeError:
                print(f"   [DataStore] WARNING: {self.scan_status_file} is corrupted or empty. Starting with fresh data.")
                self.scan_status = {}
        else:
            print(f"   [DataStore] {self.scan_status_file} not found or is empty. Starting fresh.")


    def _save_data_to_disk(self):
        """The actual blocking I/O operation to save data."""
        if not self._dirty:
            return

        print("   [DataStore] Saving data to disk in background thread...")
        try:
            with open(self.user_data_file, 'w') as f:
                json.dump(self.user_data, f, indent=4)
            with open(self.scan_status_file, 'w') as f:
                json.dump(self.scan_status, f, indent=4)
            self._dirty = False
            print("   [DataStore] Data successfully saved to disk.")
        except Exception as e:
            print(f"🛑 [DataStore] CRITICAL: Failed to save data to disk: {e}")

    async def save_data_async(self):
        """Asynchronously triggers the save operation in a separate thread."""
        if self._dirty:
            print("   [DataStore] Scheduling immediate async save...")
            await self.loop.run_in_executor(self._executor, self._save_data_to_disk)

    def mark_dirty(self):
        """Marks the data as changed and needing a save."""
        self._dirty = True

    def is_dirty(self):
        return self._dirty

    async def start_scheduled_save(self, interval=70):
        """Periodically saves data to disk if it has changed."""
        print(f"   [DataStore] Scheduled background saving enabled (every {interval} seconds).")
        while True:
            await asyncio.sleep(interval)
            if self.is_dirty():
                print("   [DataStore] Periodic save triggered...")
                await self.save_data_async()
            
    def get_user_history(self, user_id):
        return self.user_data.get(user_id)

    def get_scan_status(self, chat_id):
        return self.scan_status.get(str(chat_id))
