
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import time

class MongoDataStore:
    def __init__(self, loop, connection_string):
        print("🗄️ [MongoDataStore] Initializing...")
        if not connection_string:
            raise ValueError("MONGO_CONNECTION_STRING is not set in the .env file.")
        self.client = AsyncIOMotorClient(connection_string, io_loop=loop)
        self.db = self.client['telegram_scraper_db']
        self.users = self.db['users']
        self.scan_status = self.db['scan_status']
        print("✅ [MongoDataStore] Collections initialized for STRING-based IDs.")

    async def connect(self):
        try:
            await self.client.admin.command('ping')
            print("✅ [MongoDataStore] Database connection successful.")
        except Exception as e:
            print(f"🔥 [MongoDataStore] CRITICAL: Database connection failed: {e}")
            raise

    async def get_user_history(self, user_id: str):
        user_doc = await self.users.find_one({'user_id': user_id})
        return user_doc.get('history', []) if user_doc else []

    async def save_user_data_logic(self, user_id: str, new_history_entry: dict, is_update=False):
        if is_update:
            # Gunakan positional operator '$' yang aman berdasarkan timestamp
            await self.users.update_one(
                {'user_id': user_id, 'history.timestamp': new_history_entry['timestamp']},
                {'$set': {'history.$': new_history_entry}}
            )
        else:
            await self.users.update_one(
                {'user_id': user_id},
                {'$push': {'history': new_history_entry}},
                upsert=True
            )

    async def update_user_history_batch(self, user_batch: list):
        if not user_batch: return 0

        user_ids = [str(u['user_entity'].id) for u in user_batch]
        
        pipeline = [{'$match': {'user_id': {'$in': user_ids}}}, {'$project': {'user_id': 1, 'last_history': {'$arrayElemAt': ['$history', -1]}}}]
        existing_users_cursor = self.users.aggregate(pipeline)
        existing_users_map = {doc['user_id']: doc['last_history'] async for doc in existing_users_cursor}
        
        bulk_ops = []
        saved_count = 0

        for user_data in user_batch:
            user_entity = user_data['user_entity']
            active_chat_id = str(user_data['active_chat_id'])
            user_id = str(user_entity.id)
            
            current_identity = {'full_name': (user_entity.first_name or "") + (" " + (user_entity.last_name or "") if user_entity.last_name else ""), 'username': user_entity.username}
            last_entry = existing_users_map.get(user_id, {})
            
            if last_entry.get('username') and not current_identity['username']: continue

            last_active_chats = set(last_entry.get('active_chats_snapshot', []))
            is_new_group = active_chat_id not in last_active_chats
            identity_changed = (last_entry.get('full_name') != current_identity['full_name'] or last_entry.get('username') != current_identity['username'])

            if not last_entry or identity_changed:
                last_active_chats.add(active_chat_id)
                new_entry = {'timestamp': int(time.time()), 'full_name': current_identity['full_name'], 'username': current_identity['username'], 'active_chats_snapshot': sorted(list(last_active_chats)), 'shared_chats': last_entry.get('shared_chats', [])}
                bulk_ops.append(UpdateOne({'user_id': user_id}, {'$push': {'history': new_entry}}, upsert=True))
                saved_count += 1
            elif is_new_group:
                last_active_chats.add(active_chat_id)
                new_snapshot = sorted(list(last_active_chats))
                # === INI ADALAH PERBAIKAN KRITIS ===
                # Gunakan positional operator '$' untuk memperbarui field di dalam elemen array
                bulk_ops.append(UpdateOne(
                    {'user_id': user_id, 'history.timestamp': last_entry['timestamp']},
                    {'$set': {'history.$.active_chats_snapshot': new_snapshot}}
                ))
                saved_count += 1
        
        if bulk_ops: 
            try:
                await self.users.bulk_write(bulk_ops, ordered=False)
            except Exception as e:
                print(f"🔥 [DB Batch Error] An error occurred during bulk write: {e}")

        return saved_count

    async def get_scan_status(self, group_id: str):
        status = await self.scan_status.find_one({'group_id': group_id})
        return status if status else {}

    async def update_scan_status(self, group_id: str, status_data: dict):
        if not status_data: await self.scan_status.delete_one({'group_id': group_id})
        else: await self.scan_status.update_one({'group_id': group_id}, {'$set': status_data}, upsert=True)

    async def mark_scan_as_completed(self, group_id: str):
        await self.scan_status.update_one({'group_id': group_id}, {'$set': {'completed': True}}, upsert=True)
        print(f"   [DB] Marked group {group_id} as completed.")

    async def get_completed_scan_ids(self) -> set:
        completed_scans = self.scan_status.find({'completed': True})
        return {doc['group_id'] async for doc in completed_scans}

    async def add_completed_scan_id(self, group_id: str):
        await self.mark_scan_as_completed(group_id)

    async def get_total_user_count(self):
        return await self.users.count_documents({})

    async def clear_scan_record(self, group_id: str):
        await self.scan_status.delete_one({'group_id': group_id})
        print(f"   [DB] Cleared all scan records for group {group_id}.")
