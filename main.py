
import asyncio
import os
import time
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, ChatFull
from telethon.errors import MessageNotModifiedError
from telethon.errors.rpcerrorlist import UserNotParticipantError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest


from mongo_data_store import MongoDataStore

# Muat konfigurasi dari file .env
load_dotenv()

class TeleScrapeTracker:
    def __init__(self, session_name='bot_session'):
        print("🤖 [TeleScrapeTracker] Initializing Bot...")
        self.API_ID = int(os.getenv('TG_API_ID'))
        self.API_HASH = os.getenv('TG_API_HASH')
        self.ADMIN_IDS = [int(i) for i in os.getenv('ADMIN_IDS', '').split(',') if i]
        self.BATCH_SIZE = 500
        
        self.client = TelegramClient(session_name, self.API_ID, self.API_HASH)
        self.loop = asyncio.get_event_loop()
        self.my_id = None
        self.chat_titles_cache = {}
        self.completed_scan_group_ids = set() 
        
        self.data_store = MongoDataStore(self.loop)
        
        self.client.add_event_handler(self.handle_command, events.NewMessage(pattern=r'^/[a-zA-Z_]+', forwards=False, from_users=self.ADMIN_IDS))
        self.client.add_event_handler(self.handle_passive_tracking, events.NewMessage(incoming=True, forwards=False))

        print("✅ [TeleScrapeTracker] Bot ready. Command and passive tracking handlers attached.")

    async def _initialize_connections(self):
        await self._ensure_my_id()
        await self.data_store.connect()
        self.completed_scan_group_ids = await self.data_store.get_completed_scan_ids()
        print(f"✅ [Init] Loaded {len(self.completed_scan_group_ids)} completed scan groups from DB.")
        
        total_users_in_db = await self.data_store.get_total_user_count()
        print(f"📊 [Init] Total users currently tracked in database: {total_users_in_db}")


    async def _ensure_my_id(self):
        if self.my_id is None:
            print("   [Auth] Fetching bot's own ID...")
            me = await self.client.get_me()
            self.my_id = me.id
            print(f"   [Auth] Bot ID is {self.my_id}")

    async def _get_chat_title(self, chat_id):
        chat_id_int = int(chat_id)
        if chat_id_int in self.chat_titles_cache:
            return self.chat_titles_cache[chat_id_int]
        try:
            entity = await self.client.get_entity(chat_id_int)
            title = entity.title if hasattr(entity, 'title') else f"Chat Privasi"
            self.chat_titles_cache[chat_id_int] = title
            return title
        except Exception as e:
            print(f"   [Cache] Could not fetch title for {chat_id_int}: {e}")
            return f"[Grup Tidak Dapat Diakses: {chat_id_int}]"

    def start(self):
        print("🚀 [TeleScrapeTracker] Starting client...")
        with self.client:
            self.client.loop.run_until_complete(self._initialize_connections())
            print(f"✅ [TeleScrapeTracker] Client connected. Bot is now running...")
            self.client.run_until_disconnected()
        print("🛑 [TeleScrapeTracker] Client disconnected.")

    def _extract_user_identity(self, user: User):
        return {
            'full_name': (user.first_name or "") + (" " + (user.last_name or "") if user.last_name else ""),
            'username': user.username
        }

    async def save_user_data(self, user_entity: User, active_chat_id=None, shared_chats=None):
        if not isinstance(user_entity, User): return
        user_id = user_entity.id # Removed str() conversion
        current_identity = self._extract_user_identity(user_entity)
        
        history = await self.data_store.get_user_history(user_id)
        if history and history[-1].get('username') and not current_identity['username']: return

        new_entry = {
            'timestamp': int(time.time()),
            'full_name': current_identity['full_name'],
            'username': current_identity['username'],
            'active_chats_snapshot': [active_chat_id] if active_chat_id else [],
            'shared_chats': shared_chats if shared_chats is not None else []
        }
        await self.data_store.save_user_data_logic(user_id, new_entry)
    
    async def handle_passive_tracking(self, event):
        print(f"DEBUG: [PassiveTrack] Event received (chat_id: {event.chat_id}, sender_id: {event.sender_id})")

        if event.chat_id not in self.completed_scan_group_ids:
            print(f"DEBUG: [PassiveTrack] Ignoring message from non-scanned group {event.chat_id}.")
            return
            
        if event.sender_id == self.my_id:
            print(f"DEBUG: [PassiveTrack] Ignoring message from bot itself ({event.sender_id}).")
            return
        if event.sender_id in self.ADMIN_IDS:
            print(f"DEBUG: [PassiveTrack] Ignoring message from admin ({event.sender_id}).")
            return
        
        try:
            sender = await event.get_sender()
            if isinstance(sender, User) and event.is_group:
                chat_title = await self._get_chat_title(event.chat_id)
                print(f"🕵️  [PassiveTrack] Saw message from {sender.id} ({sender.first_name}) in group {chat_title} ({event.chat_id}).")
                await self.save_user_data(sender, active_chat_id=event.chat_id)
            else:
                print(f"DEBUG: [PassiveTrack] Ignoring non-user or non-group message. Sender type: {type(sender)}, Is group: {event.is_group}")
        except Exception as e:
            print(f"-❗️- [PassiveTrack] Minor exception on handle_passive_tracking: {e}")

    async def handle_command(self, event):
        print(f"⚙️ [HandleCommand] Admin {event.sender_id} sent command: {event.raw_text}")
        command, *args = event.raw_text.split()
        command_map = {
            '/hisz': self.show_history,
            '/scan_group': self.scan_group,
            '/scan_allgrup': self.scan_all_groups,
            '/scan_user': self.scan_user_details,
            '/findmutual': self.find_mutual_groups,
            '/clear_checkpoint': self.clear_checkpoint
        }
        if command in command_map:
            await command_map[command](event, *args)
        else:
            print(f"   [HandleCommand] Unknown command: {command}")

    async def show_history(self, event, *args):
        if not args:
            await event.reply("Usage: `/hisz <user_id>`")
            return
        target_id_str = args[0]
        if not target_id_str.isdigit():
            await event.reply("Error: Harap masukkan User ID yang valid.")
            return

        print(f"   [CMD /hisz] Looking up history for User ID: {target_id_str}")
        history = await self.data_store.get_user_history(int(target_id_str)) # Ensure integer is passed to data_store
        live_entity = None
        try:
            print(f"   [CMD /hisz] Fetching live data for {target_id_str}...")
            live_entity = await self.client.get_entity(int(target_id_str))
            
            if not history:
                 print(f"   [CMD /hisz] No history found, but live entity exists. Saving.")
                 await self.save_user_data(live_entity)
                 history = await self.data_store.get_user_history(int(target_id_str)) # Ensure integer is passed to data_store
            else:
                last_db_entry = history[-1]
                live_identity = self._extract_user_identity(live_entity)
                if last_db_entry.get('username') != live_identity['username'] or last_db_entry.get('full_name') != live_identity['full_name']:
                     print(f"   [CMD /hisz] Live data is different. Saving update for {target_id_str}.")
                     await self.save_user_data(live_entity)
                     history = await self.data_store.get_user_history(int(target_id_str)) # Ensure integer is passed to data_store

        except Exception as e:
            print(f"   [CMD /hisz] Could not fetch live data for {target_id_str}. Displaying from DB only. Reason: {e}")

        if not history:
            await event.reply(f"❌ Tidak ada riwayat yang ditemukan untuk User ID `{target_id_str}`.")
            return

        header_lines = ["**Riwayat Pelacakan User**\n"]
        live_username_str = f"@{live_entity.username}" if live_entity and live_entity.username else "N/A"
        last_db_username = history[-1].get('username', 'N/A')
        username_to_show = live_username_str if live_entity else f"@{last_db_username} (Offline)"
        header_lines.append(f"<code>ID       : {target_id_str}</code>")
        header_lines.append(f"<code>Username : {username_to_show}</code>")
        output_message = '\n'.join(header_lines)
        history_blocks = []
        for i, entry in enumerate(reversed(history)):
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(entry['timestamp']))
            block = [f"\n--- **#{len(history) - i}** ({ts}) ---"]
            block.append(f"<code>Nama     : {entry['full_name']}</code>")
            block.append(f"<code>Username : @{entry.get('username', 'N/A')}</code>")
            active_chats = entry.get('active_chats_snapshot', [])
            if active_chats:
                chat_titles = [await self._get_chat_title(cid) for cid in active_chats]
                block.append(f"\n*Terlihat di Grup:*\n- " + "\n- ".join(chat_titles))
            history_blocks.append('\n'.join(block))
        await event.reply(output_message + '\n'.join(history_blocks), parse_mode='html')

    async def scan_group(self, event, *args):
        target_id_str = args[0] if args else str(event.chat_id)
        if not target_id_str.lstrip('-').isdigit():
            await event.reply("❌ **Error:** Harap berikan ID grup yang valid.")
            return

        target_chat_id = int(target_id_str)
        status_message = await event.reply(f"<code>Mempersiapkan pemindaian untuk grup {target_chat_id}...</code>", parse_mode='html')
        
        await self._perform_group_scan(target_chat_id, status_message)

    async def _perform_group_scan(self, target_chat_id, status_message, chat_title_override=None):
        print(f"🔎 [_perform_group_scan] Initiating scan for target Chat ID: {target_chat_id}")
        try:
            chat = await self.client.get_entity(target_chat_id)
            if isinstance(chat, User):
                await status_message.edit(f"❌ **Error:** ID `{target_chat_id}` adalah milik pengguna.")
                return False

            full_chat = await self.client(GetFullChannelRequest(channel=chat))
            member_count = full_chat.full_chat.participants_count
            chat_title = chat.title
        except TypeError:
            try:
                full_chat = await self.client(GetFullChatRequest(chat_id=target_chat_id))
                member_count = len(full_chat.users)
                chat_title = chat.title
            except Exception as e:
                await status_message.edit(f"❌ **Error:** Tidak dapat mengambil detail grup {chat_title_override or target_chat_id}.\n`{e}`")
                return False
        except Exception as e:
            await status_message.edit(f"❌ **Error:** Tidak dapat mengakses grup {chat_title_override or target_chat_id}.\n`{e}`")
            return False
        
        print(f"   [ScanGroup] Group '{chat_title}' has {member_count} members.")

        if member_count < 10000:
            await self._direct_scan_group(status_message, chat, target_chat_id, member_count, chat_title)
        else:
            await self._filtered_scan_group(status_message, chat, target_chat_id, chat_title)
        
        await self.data_store.mark_scan_as_completed(target_chat_id) # Removed str()
        self.completed_scan_group_ids.add(target_chat_id)
        print(f"   [ScanGroup] ✅ Marked group {target_chat_id} as completed.")
        return True

    async def _direct_scan_group(self, status_message, chat, target_chat_id, member_count, chat_title):
        total_processed = 0
        total_saved = 0
        last_edit_time = 0
        user_batch = []

        try:
            async for participant in self.client.iter_participants(chat):
                total_processed += 1
                if isinstance(participant, User) and participant.id != self.my_id:
                    user_batch.append({'user_entity': participant, 'active_chat_id': target_chat_id})

                if len(user_batch) >= self.BATCH_SIZE:
                    saved_count = await self.data_store.update_user_history_batch(user_batch)
                    total_saved += saved_count
                    user_batch = [] 

                if time.time() - last_edit_time > 3:
                    progress_percent = int((total_processed / member_count) * 100) if member_count > 0 else 0
                    progress_bar = '█' * int(progress_percent / 5) + ' ' * (20 - int(progress_percent / 5))
                    status_text = (
                        f"GROUP: {chat_title}\n"
                        f"METHOD: Direct Scan (Batch)\n"
                        f"=================================\n"
                        f"PROGRESS : [{progress_bar}] {progress_percent}%\n"
                        f"PROCESSED: {total_processed} / {member_count}\n"
                        f"SAVED    : {total_saved} users\n"
                        f"STATUS   : Scanning..."
                    )
                    await status_message.edit(f"<pre>{status_text}</pre>", parse_mode='html')
                    last_edit_time = time.time()
            
            if user_batch: 
                saved_count = await self.data_store.update_user_history_batch(user_batch)
                total_saved += saved_count

        except MessageNotModifiedError: pass
        except Exception as e:
            print(f"🛑 [DirectScan] CRITICAL ERROR: {e}")
            await status_message.edit(f"<pre>❌ ERROR during direct scan:\n{e}</pre>", parse_mode='html')
            raise 

        final_text = (
            f"GROUP: {chat_title}\n"
            f"=================================\n"
            f"STATUS : ✅ Direct Scan Complete!\n"
            f"TOTAL  : {total_saved} pengguna disimpan/diperbarui."
        )
        await status_message.edit(f"<pre>{final_text}</pre>", parse_mode='html')

    async def _filtered_scan_group(self, status_message, chat, target_chat_id, chat_title):
        status = await self.data_store.get_scan_status(target_chat_id) # Removed str()
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        filter_index = status.get('filter_index', 0)
        total_saved = status.get('total_saved_since_start', 0)
        last_edit_time = 0
        user_batch = []

        if filter_index >= len(alphabet):
            await status_message.edit(f"<pre>✅ Pemindaian (lanjutan) untuk \"{chat_title}\" selesai.</pre>", parse_mode='html')
            return
        
        for i in range(filter_index, len(alphabet)):
            filter_char = alphabet[i]
            
            try:
                progress_percent = int(((i + 1) / len(alphabet)) * 100)
                progress_bar = '█' * int(progress_percent / 5) + ' ' * (20 - int(progress_percent / 5))
                status_text = (
                    f"GROUP: {chat_title}\n"
                    f"METHOD: Filtered Scan (Batch)\n"
                    f"=================================\n"
                    f"PROGRESS : [{progress_bar}] {progress_percent}%\n"
                    f"FILTER   : Sedang memproses '{filter_char}'...\n"
                    f"SAVED    : {total_saved} users\n"
                    f"STATUS   : Scanning..."
                )
                if time.time() - last_edit_time > 2:
                    await status_message.edit(f"<pre>{status_text}</pre>", parse_mode='html')
                    last_edit_time = time.time()

                print(f"   [FilteredScan] Iterating with filter: '{filter_char}' in group {target_chat_id}")
                async for participant in self.client.iter_participants(chat, search=filter_char):
                    if isinstance(participant, User) and participant.id != self.my_id:
                        user_batch.append({'user_entity': participant, 'active_chat_id': target_chat_id})

                    if len(user_batch) >= self.BATCH_SIZE:
                        saved_count = await self.data_store.update_user_history_batch(user_batch)
                        total_saved += saved_count
                        user_batch = []

                if user_batch: 
                    saved_count = await self.data_store.update_user_history_batch(user_batch)
                    total_saved += saved_count
                    user_batch = []
                
                print(f"   [FilteredScan] Filter '{filter_char}' selesai.")
                await self.data_store.update_scan_status(target_chat_id, {'filter_index': i + 1, 'total_saved_since_start': total_saved}) # Removed str()
                await asyncio.sleep(5)

            except MessageNotModifiedError: pass
            except Exception as e:
                print(f"🛑 [FilteredScan] CRITICAL ERROR on filter '{filter_char}': {e}")
                await status_message.edit(f"<pre>❌ ERROR on filter '{filter_char}'\nREASON: {e}</pre>", parse_mode='html')
                raise 

        final_text = (
            f"GROUP: {chat_title}\n"
            f"=================================\n"
            f"STATUS : ✅ Filtered Scan Complete!\n"
            f"TOTAL  : {total_saved} pengguna disimpan/diperbarui."
        )
        await status_message.edit(f"<pre>{final_text}</pre>", parse_mode='html')
        await self.data_store.update_scan_status(target_chat_id, {}) # Removed str()

    async def scan_all_groups(self, event, *args):
        print("🔎 [CMD /scan_allgrup] Initiating scan for all groups.")
        status_message = await event.reply("<code>Mempersiapkan pemindaian semua grup...</code>", parse_mode='html')
        
        try:
            all_dialogs = await self.client.get_dialogs()
            groups_to_scan = [d.entity for d in all_dialogs if d.is_group or (d.is_channel and d.entity.megagroup)]
        except Exception as e:
            await status_message.edit(f"❌ **Error:** Gagal mengambil daftar grup.\n`{e}`")
            return

        if not groups_to_scan:
            await status_message.edit("ℹ️ Bot tidak berada di grup manapun.")
            return

        total_groups = len(groups_to_scan)
        print(f"   [ScanAll] Found {total_groups} groups to scan.")
        
        summary = {"scanned_count": 0, "failed_groups": []}
        last_edit_time = 0

        for i, chat in enumerate(groups_to_scan):
            try:
                current_status_text = (
                    f"SCAN SEMUA GRUP ({i+1}/{total_groups})\n"
                    f"=================================\n"
                    f"GRUP SAAT INI: {chat.title} (<code>{chat.id}</code>)\n"
                    f"STATUS: Memulai pemindaian..."
                )
                await status_message.edit(f"<pre>{current_status_text}</pre>", parse_mode='html')
                last_edit_time = time.time()
            except MessageNotModifiedError: pass
            except Exception as e:
                print(f"   [ScanAll] ERROR editing status message for {chat.title}: {e}")

            try:
                success = await self._perform_group_scan(chat.id, status_message, chat_title_override=chat.title)
                
                if success:
                    summary["scanned_count"] += 1
                    print(f"   [ScanAll] Finished scanning '{chat.title}'.")
                else:
                    summary["failed_groups"].append(chat.title)
                    print(f"   [ScanAll] Failed to scan '{chat.title}'.")

                await asyncio.sleep(10) 

            except Exception as e:
                print(f"🛑 [ScanAll] CRITICAL ERROR scanning group '{chat.title}': {e}")
                summary["failed_groups"].append(f"{chat.title} (Critical Error)")
                await asyncio.sleep(5) 
                continue
        
        final_summary_text = [
            "✅ **PEMINDAIAN SEMUA GRUP SELESAI**\n",
            f"Grup Sukses Dipindai: {summary['scanned_count']}/{total_groups}",
        ]
        if summary['failed_groups']:
            final_summary_text.append("\n❌ **Grup Gagal:**")
            final_summary_text.extend([f"- {group_title}" for group_title in summary['failed_groups']])
        
        await status_message.edit('\n'.join(final_summary_text), parse_mode='html')
        print("✅ [CMD /scan_allgrup] All groups scan finished.")

    async def scan_user_details(self, event, *args):
        if not args:
            await event.reply("Usage: `/scan_user <user_id>`")
            return

        target_id_str = args[0]
        if not target_id_str.isdigit():
            await event.reply("Error: Harap masukkan User ID yang valid.")
            return

        target_user_id = int(target_id_str)
        print(f"🔎 [CMD /scan_user] Initiating scan for User ID: {target_user_id}")
        
        try:
            live_entity = await self.client.get_entity(target_user_id)
            if isinstance(live_entity, User):
                await self.save_user_data(live_entity)
                await event.reply(f"✅ Data untuk pengguna `{target_user_id}` telah diperbarui dari Telegram dan disimpan ke database.")
            else:
                await event.reply(f"⚠️ Entitas `{target_id_str}` bukan seorang pengguna.")
        except Exception as e:
            print(f"   [ScanUser] Could not fetch live data for {target_user_id}: {e}")
            await event.reply(f"❌ **Error:** Tidak dapat menemukan pengguna dengan ID `{target_user_id}` di Telegram.\n`{e}`")

    async def find_mutual_groups(self, event, *args):
        if not args:
            await event.reply("Usage: `/findmutual <user_id>`")
            return

        target_id_str = args[0]
        if not target_id_str.isdigit():
            await event.reply("Error: Harap masukkan User ID yang valid.")
            return
        
        target_user_id = int(target_id_str)
        
        status_message = await event.reply(f"<code>Mencari grup bersama dengan user {target_user_id}...</code>", parse_mode='html')
        
        try:
            target_user = await self.client.get_entity(target_user_id)
            print(f"🔎 [CMD /findmutual] Initiating mutual group search for {target_user.first_name} ({target_user_id})")
        except Exception as e:
            await status_message.edit(f"❌ **Error:** Tidak dapat menemukan pengguna dengan ID `{target_user_id}`.\n`{e}`")
            return

        mutual_groups = []
        last_edit_time = time.time()
        
        try:
            all_dialogs = await self.client.get_dialogs()
            groups_to_check = [d for d in all_dialogs if d.is_group or (d.is_channel and d.entity.megagroup)]
            total_groups = len(groups_to_check)
            
            for i, dialog in enumerate(groups_to_check):
                chat = dialog.entity
                
                if time.time() - last_edit_time > 3:
                    try:
                        await status_message.edit(f"<code>Mengecek grup {i+1}/{total_groups}: {chat.title}...</code>", parse_mode='html')
                        last_edit_time = time.time()
                    except MessageNotModifiedError: pass

                try:
                    async for _ in self.client.iter_participants(chat, filter=lambda p: p.id == target_user_id, limit=1):
                        print(f"   [FindMutual] Found user in '{chat.title}'")
                        mutual_groups.append(f"{chat.title} (<code>{chat.id}</code>)")
                        await self.save_user_data(target_user, active_chat_id=chat.id)
                        break 

                except Exception as e:
                    print(f"   [FindMutual] Could not check group '{chat.title}': {e}")
                    continue
            
            if not mutual_groups:
                reply_text = f"✅ Pencarian selesai.\n\nTidak ditemukan grup bersama antara bot dan user `{target_user_id}`."
            else:
                groups_list_str = "\n- ".join(mutual_groups)
                reply_text = (
                    f"✅ Pencarian selesai.\n\n"
                    f"Bot dan user `{target_user_id}` sama-sama berada di grup berikut:\n\n"
                    f"- {groups_list_str}"
                )
            await status_message.edit(reply_text, parse_mode='html')

        except Exception as e:
            print(f"🛑 [FindMutual] CRITICAL ERROR: {e}")
            await status_message.edit(f"❌ Terjadi kesalahan internal saat mencari grup bersama.\n`{e}`")

    async def clear_checkpoint(self, event, *args):
        target_id_str = args[0] if args else str(event.chat_id)
        if not target_id_str.lstrip('-').isdigit():
            await event.reply("❌ **Error:** Harap berikan ID grup yang valid.")
            return

        print(f"🗑️ [CMD /clear_checkpoint] Clearing ALL scan records for Chat ID: {target_id_str}")
        await self.data_store.clear_scan_record(int(target_id_str)) # Added int()
        
        target_chat_id = int(target_id_str)
        if target_chat_id in self.completed_scan_group_ids:
            self.completed_scan_group_ids.remove(target_chat_id)
            
        await event.reply(f"✅ Semua catatan pemindaian (termasuk status selesai) untuk grup `{target_id_str}` telah dihapus.")

if __name__ == '__main__':
    bot = None
    try:
        bot = TeleScrapeTracker()
        bot.start()
    except Exception as e:
        print(f"💥 [Main] An unexpected error occurred: {e}")
