
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import time

class MongoDataStore:
    def __init__(self, loop, connection_string):
        print("🗄️ [MongoDataStore] Initializing...")
        self.client = AsyncIOMotorClient(connection_string, io_loop=loop)
        self.db = self.client['telegram_scraper_db']
        
        # --- PERBAIKAN: Menggunakan 2 koleksi sesuai struktur database Anda ---
        self.users = self.db['users']
        self.scan_status = self.db['scan_status'] # Koleksi ini menangani checkpoint & status selesai
        
        print(f"✅ [MongoDataStore] Connected to database '{self.db.name}'. Using collections: 'users', 'scan_status'.")

    async def get_user_history(self, user_id: str):
        user_doc = await self.users.find_one({'user_id': user_id})
        return user_doc.get('history', []) if user_doc else []

    async def save_user_data_logic(self, user_id: str, new_entry: dict, is_update: bool):
        if is_update:
            await self.users.update_one(
                {'user_id': user_id},
                {'$pop': {'history': 1}}
            )
            await self.users.update_one(
                {'user_id': user_id},
                {'$push': {'history': new_entry}}
            )
        else:
            await self.users.update_one(
                {'user_id': user_id},
                {'$push': {'history': new_entry}},
                upsert=True
            )

    async def update_user_history_batch(self, batch: list):
        if not batch: return 0
            
        print(f"   [DB Batch] Processing batch of {len(batch)} users...")
        bulk_operations = []
        user_ids_in_batch = {str(item['user_entity'].id) for item in batch}
        
        existing_users_cursor = self.users.find({'user_id': {'$in': list(user_ids_in_batch)}})
        existing_users = {u['user_id']: u for u in await existing_users_cursor.to_list(length=len(user_ids_in_batch))}
        
        saved_count = 0

        for item in batch:
            user_entity = item['user_entity']
            active_chat_id = item['active_chat_id']
            user_id = str(user_entity.id)
            current_identity = {'full_name': (user_entity.first_name or "") + (" " + (user_entity.last_name or "") if user_entity.last_name else ""), 'username': user_entity.username}
            if not current_identity['username'] and not current_identity['full_name']: continue
            user_doc = existing_users.get(user_id)
            history = user_doc['history'] if user_doc and 'history' in user_doc else []
            last_entry = history[-1] if history else {}
            if last_entry.get('username') and not current_identity['username']: continue
            identity_changed = (last_entry.get('full_name') != current_identity['full_name'] or last_entry.get('username') != current_identity['username'])
            last_active_chats = set(last_entry.get('active_chats_snapshot', []))
            is_new_group = active_chat_id and active_chat_id not in last_active_chats

            if not history or identity_changed:
                if active_chat_id: last_active_chats.add(active_chat_id)
                new_entry = {'timestamp': int(time.time()), 'full_name': current_identity['full_name'], 'username': current_identity['username'], 'active_chats_snapshot': sorted(list(last_active_chats)), 'shared_chats': last_entry.get('shared_chats', [])}
                bulk_operations.append(UpdateOne({'user_id': user_id}, {'$push': {'history': new_entry}}, upsert=True))
                saved_count += 1
            elif is_new_group:
                history.pop()
                last_active_chats.add(active_chat_id)
                last_entry['active_chats_snapshot'] = sorted(list(last_active_chats))
                history.append(last_entry)
                bulk_operations.append(UpdateOne({'user_id': user_id}, {'$set': {'history': history}}, upsert=True))
                saved_count += 1

        if bulk_operations:
            await self.users.bulk_write(bulk_operations)
            print(f"   [DB Batch] ✅ Bulk write completed. Saved/Updated {saved_count} records.")
        return saved_count

    async def get_completed_scan_ids(self):
        # --- PERBAIKAN: Mencari di 'scan_status' dimana 'completed' adalah True ---
        cursor = self.scan_status.find({'completed': True})
        return {doc['group_id'] for doc in await cursor.to_list(length=None)}

    async def mark_scan_as_completed(self, group_id: str):
        # --- PERBAIKAN: Menulis status 'completed: true' ke 'scan_status' ---
        await self.scan_status.update_one(
            {'group_id': group_id},
            {'$set': {'completed': True, 'group_id': group_id}},
            upsert=True
        )

    async def add_completed_scan_id(self, group_id: str):
        await self.mark_scan_as_completed(group_id)

    async def get_scan_status(self, group_id: str):
        status = await self.scan_status.find_one({'group_id': group_id})
        return status or {}

    async def update_scan_status(self, group_id: str, status_doc: dict):
        if not status_doc:
            # Jika status kosong, hapus checkpoint tapi pertahankan status 'completed'
            await self.scan_status.update_one(
                {'group_id': group_id},
                {'$unset': {'filter_index': "", 'total_saved_since_start': ""}}
            )
        else:
            status_doc_with_id = {'group_id': group_id, **status_doc}
            await self.scan_status.update_one(
                {'group_id': group_id},
                {'$set': status_doc_with_id},
                upsert=True
            )

    async def clear_scan_record(self, group_id: str):
        # --- PERBAIKAN: Menghapus status 'completed' dari 'scan_status' ---
        await self.scan_status.update_one(
            {'group_id': group_id},
            {'$set': {'completed': False}},
        )

    async def get_total_user_count(self):
        return await self.users.estimated_document_count()
