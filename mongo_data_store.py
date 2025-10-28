
import asyncio
import os
import time
import motor.motor_asyncio
from pymongo import UpdateOne
from pymongo.server_api import ServerApi

class MongoDataStore:
    def __init__(self, loop):
        print("🗄️ [MongoDataStore] Initializing MongoDB connection...")
        self.client = None
        self.db = None
        self.user_data_collection = None
        self.scan_status_collection = None
        self.loop = loop
        self.mongo_uri = os.getenv('MONGO_URI')

        if not self.mongo_uri:
            raise ValueError("MONGO_URI is not set in the environment variables.")

    async def connect(self):
        try:
            print("   [Mongo] Connecting to MongoDB Atlas...")
            self.client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri, server_api=ServerApi('1'))
            await self.client.admin.command('ping')
            print("   [Mongo] ✅ Pinged your deployment. You successfully connected to MongoDB!")
            
            self.db = self.client.telescrape_db
            self.user_data_collection = self.db.user_data
            self.scan_status_collection = self.db.scan_status
            
            await self.user_data_collection.create_index("user_id", unique=True)
            await self.scan_status_collection.create_index("chat_id", unique=True)
            print("   [Mongo] ✅ Database and collections are ready.")

        except Exception as e:
            print(f"   [Mongo] ❌ Could not connect to MongoDB: {e}")
            raise

    async def get_user_history(self, user_id_str):
        user_doc = await self.user_data_collection.find_one({'user_id': user_id_str})
        return user_doc.get('history', []) if user_doc else []

    async def update_user_history(self, user_id_str, history_list):
        await self.user_data_collection.update_one(
            {'user_id': user_id_str},
            {'$set': {'history': history_list}},
            upsert=True
        )
        
    async def update_user_history_batch(self, user_batch):
        if not user_batch:
            return 0
            
        print(f"   [MongoBatch] Processing batch of {len(user_batch)} users...")
        user_ids = [str(u['user_entity'].id) for u in user_batch]
        existing_users_cursor = self.user_data_collection.find({'user_id': {'$in': user_ids}})
        existing_users = {doc['user_id']: doc.get('history', []) async for doc in existing_users_cursor}
        
        bulk_operations = []

        for item in user_batch:
            user_entity = item['user_entity']
            active_chat_id = item['active_chat_id']
            user_id = str(user_entity.id)

            current_identity = {
                'full_name': (user_entity.first_name or "") + (" " + (user_entity.last_name or "") if user_entity.last_name else ""),
                'username': user_entity.username
            }
            history = existing_users.get(user_id, [])

            if history and history[-1].get('username') and not current_identity['username']:
                continue

            new_entry = {
                'timestamp': int(time.time()),
                'full_name': current_identity['full_name'],
                'username': current_identity['username'],
                'active_chats_snapshot': [active_chat_id] if active_chat_id else [],
                'shared_chats': []
            }
            
            if not history:
                history.append(new_entry)
            else:
                last_entry = history[-1]
                last_active_chats = last_entry.get('active_chats_snapshot', [])
                if not isinstance(last_active_chats, list): last_active_chats = []

                identity_changed = (last_entry.get('full_name') != new_entry['full_name'] or last_entry.get('username') != new_entry['username'])
                active_chat_added = active_chat_id and active_chat_id not in last_active_chats

                if identity_changed:
                    new_entry['active_chats_snapshot'] = sorted(list(set(last_active_chats + new_entry['active_chats_snapshot'])))
                    history.append(new_entry)
                elif active_chat_added:
                    last_entry['active_chats_snapshot'] = sorted(list(set(last_active_chats + [active_chat_id])))
                    last_entry['timestamp'] = int(time.time())
            
            bulk_operations.append(UpdateOne(
                {'user_id': user_id},
                {'$set': {'history': history}},
                upsert=True
            ))

        if not bulk_operations:
            return 0

        result = await self.user_data_collection.bulk_write(bulk_operations)
        print(f"   [MongoBatch] ✅ Batch complete. Matched: {result.matched_count}, Upserted: {result.upserted_count}, Modified: {result.modified_count}")
        return result.upserted_count + result.modified_count


    async def get_total_user_count(self):
        return await self.user_data_collection.count_documents({})


    async def get_scan_status(self, chat_id: int):
        status_doc = await self.scan_status_collection.find_one({'chat_id': chat_id})
        return status_doc if status_doc else {}

    async def update_scan_status(self, chat_id: int, status_data: dict):
        await self.scan_status_collection.update_one(
            {'chat_id': chat_id},
            {'$set': status_data},
            upsert=True
        )

    async def mark_scan_as_completed(self, chat_id: int):
        await self.scan_status_collection.update_one(
            {'chat_id': chat_id},
            {
                '$set': {'scan_completed': True, 'last_scan_timestamp': int(time.time())},
                '$unset': {'filter_index': "", 'total_saved_since_start': ""}
            },
            upsert=True
        )

    async def get_completed_scan_ids(self):
        completed_scans_cursor = self.scan_status_collection.find({'scan_completed': True})
        return {doc['chat_id'] async for doc in completed_scans_cursor}

    async def clear_scan_record(self, chat_id: int):
        await self.scan_status_collection.delete_one({'chat_id': chat_id})

    async def save_user_data_logic(self, user_id, new_entry):
        history = await self.get_user_history(user_id)
        
        if not history:
            print(f"📝 [Mongo] New user tracked: {user_id}. Creating first entry.")
            history.append(new_entry)
        else:
            last_entry = history[-1]
            last_active_chats = last_entry.get('active_chats_snapshot', [])
            if not isinstance(last_active_chats, list): last_active_chats = []
            
            identity_changed = (last_entry.get('full_name') != new_entry['full_name'] or last_entry.get('username') != new_entry['username'])
            active_chat_added = new_entry['active_chats_snapshot'] and new_entry['active_chats_snapshot'][0] not in last_active_chats

            if identity_changed:
                print(f"🔄 [Mongo] Identity change for {user_id}. Creating new snapshot.")
                new_entry['active_chats_snapshot'] = sorted(list(set(last_active_chats + new_entry['active_chats_snapshot'])))
                history.append(new_entry)
            elif active_chat_added:
                print(f"➕ [Mongo] Merging new active group for {user_id} to last entry.")
                last_entry['active_chats_snapshot'] = sorted(list(set(last_active_chats + [new_entry['active_chats_snapshot'][0]] )))
                last_entry['timestamp'] = int(time.time())

        await self.update_user_history(user_id, history)
        
    def mark_dirty(self):
        pass

    async def save_data_async(self):
        print("   [Mongo] Data is saved in real-time. No explicit save needed.")
        pass

    def is_dirty(self):
        return False
