
import asyncio
import os
import time
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel
from telethon.errors import MessageNotModifiedError

from local_data_store import LocalDataStore

# Muat konfigurasi dari file .env
load_dotenv()

class TeleScrapeTracker:
    def __init__(self, session_name='bot_session'):
        print("🤖 [TeleScrapeTracker] Initializing Bot...")
        self.API_ID = int(os.getenv('TG_API_ID'))
        self.API_HASH = os.getenv('TG_API_HASH')
        self.ADMIN_IDS = [int(i) for i in os.getenv('ADMIN_IDS', '').split(',') if i]
        
        self.client = TelegramClient(session_name, self.API_ID, self.API_HASH)
        self.loop = asyncio.get_event_loop()
        self.my_id = None
        self.chat_titles_cache = {}
        
        self.data_store = LocalDataStore(self.loop)
        
        self.client.add_event_handler(self.handle_command, events.NewMessage(pattern=r'^/[a-zA-Z_]+'))
        print("✅ [TeleScrapeTracker] Bot ready. Command handler attached.")

    async def _ensure_my_id(self):
        if self.my_id is None:
            print("   [Auth] Fetching bot\'s own ID...")
            me = await self.client.get_me()
            self.my_id = me.id
            print(f"   [Auth] Bot ID is {self.my_id}")

    async def _get_chat_title(self, chat_id):
        chat_id_int = int(chat_id)
        if chat_id_int in self.chat_titles_cache:
            return self.chat_titles_cache[chat_id_int]
        try:
            print(f"   [Cache] Fetching title for chat ID: {chat_id_int}")
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
            self.client.loop.run_until_complete(self._ensure_my_id())
            self.client.loop.create_task(self.data_store.start_scheduled_save())
            print(f"✅ [TeleScrapeTracker] Client connected. Bot is now running...")
            self.client.run_until_disconnected()
        print("🛑 [TeleScrapeTracker] Client disconnected.")

    def _extract_user_identity(self, user: User):
        return {
            'full_name': (user.first_name or "") + (" " + (user.last_name or "") if user.last_name else ""),
            'username': user.username
        }

    async def save_user_data(self, user_entity: User, active_chat_id=None, shared_chats=None):
        if not isinstance(user_entity, User):
            return
        user_id = str(user_entity.id)
        current_identity = self._extract_user_identity(user_entity)
        history = self.data_store.user_data.get(user_id, [])
        if history and history[-1].get('username') and not current_identity['username']:
            print(f"🟡 [SaveUserData] Ignored change for {user_id}: username became null.")
            return
        new_entry = {
            'timestamp': int(time.time()),
            'full_name': current_identity['full_name'],
            'username': current_identity['username'],
            'active_chats_snapshot': [active_chat_id] if active_chat_id else [],
            'shared_chats': shared_chats if shared_chats is not None else []
        }
        if not history:
            print(f"📝 [SaveUserData] New user tracked: {user_id}. Creating first entry.")
            self.data_store.user_data[user_id] = [new_entry]
            self.data_store.mark_dirty()
            return
        last_entry = history[-1]
        identity_changed = (last_entry.get('full_name') != new_entry['full_name'] or last_entry.get('username') != new_entry['username'])
        active_chat_added = active_chat_id and active_chat_id not in last_entry.get('active_chats_snapshot', [])
        if identity_changed:
            print(f"🔄 [SaveUserData] Identity change for {user_id}. Creating new snapshot.")
            new_entry['active_chats_snapshot'] = sorted(list(set(last_entry.get('active_chats_snapshot', []) + new_entry['active_chats_snapshot'])))
            history.append(new_entry)
            self.data_store.mark_dirty()
        elif active_chat_added:
            print(f"➕ [SaveUserData] Merging new active group for {user_id} to last entry.")
            last_entry['active_chats_snapshot'].append(active_chat_id)
            last_entry['active_chats_snapshot'] = sorted(list(set(last_entry['active_chats_snapshot'])))
            last_entry['timestamp'] = int(time.time())
            self.data_store.mark_dirty()

    async def handle_command(self, event):
        if event.sender_id not in self.ADMIN_IDS:
            return
        print(f"⚙️ [HandleCommand] Admin {event.sender_id} sent command: {event.raw_text}")
        command, *args = event.raw_text.split()
        command_map = {
            '/hisz': self.show_history,
            '/scan_group': self.scan_group,
            '/scan_allgrup': self.scan_all_groups,
            '/scan_user': self.scan_user_details,
            '/clear_checkpoint': self.clear_checkpoint
        }
        if command in command_map:
            await command_map[command](event, *args)
        else:
            print(f"   [HandleCommand] Unknown command: {command}")

    async def show_history(self, event, *args):
        # ... (fungsi ini tetap sama)
        if not args:
            await event.reply("Usage: `/hisz <user_id>`")
            return
        target_id_str = args[0]
        if not target_id_str.isdigit():
            await event.reply("Error: Harap masukkan User ID yang valid.")
            return
        print(f"   [CMD /hisz] Looking up history for User ID: {target_id_str}")
        history = self.data_store.get_user_history(target_id_str)
        live_entity = None
        try:
            print(f"   [CMD /hisz] Fetching live data for {target_id_str}...")
            live_entity = await self.client.get_entity(int(target_id_str))
            if history:
                last_username = history[-1].get('username')
                live_username = live_entity.username
                if last_username != live_username or history[-1].get('full_name') != self._extract_user_identity(live_entity)['full_name']:
                     print(f"   [CMD /hisz] Live data is different. Saving update for {target_id_str}.")
                     await self.save_user_data(live_entity)
                     history = self.data_store.get_user_history(target_id_str)
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
            await event.reply("❌ **Error:** Harap berikan ID grup yang valid atau jalankan perintah ini di dalam grup target.")
            return

        target_chat_id = int(target_id_str)
        print(f"🔎 [CMD /scan_group] Initiating scan for target Chat ID: {target_chat_id}")

        try:
            chat = await self.client.get_entity(target_chat_id)
            if isinstance(chat, User):
                await event.reply(f"❌ **Error:** ID `{target_chat_id}` adalah milik seorang pengguna, bukan grup.")
                return
        except Exception as e:
            await event.reply(f"❌ **Error:** Tidak dapat mengakses grup `{target_chat_id}`. Pastikan bot adalah anggota.\n`{e}`")
            return

        status_message = await event.reply("<pre>Initializing scan...</pre>", parse_mode='html')
        
        status = self.data_store.get_scan_status(str(target_chat_id)) or {}
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        filter_index = status.get('filter_index', 0)
        total_processed = status.get('total_processed_since_start', 0)
        last_edit_time = 0

        if filter_index >= len(alphabet):
            await status_message.edit(f"<pre>✅ Pemindaian untuk grup \"{chat.title}\" sudah selesai sebelumnya.</pre>", parse_mode='html')
            return
        
        for i in range(filter_index, len(alphabet)):
            filter_char = alphabet[i]
            count_in_char = 0
            
            try:
                progress_percent = int(((i + 1) / len(alphabet)) * 100)
                progress_bar = '█' * int(progress_percent / 5) + ' ' * (20 - int(progress_percent / 5))
                
                status_text = (
                    f"GROUP: {chat.title}\n"
                    f"=================================\n"
                    f"PROGRESS : [{progress_bar}] {progress_percent}%\n"
                    f"FILTER   : Sedang memproses '{filter_char}'...\n"
                    f"TOTAL    : {total_processed} pengguna\n"
                    f"STATUS   : Scanning..."
                )
                # Edit pesan hanya jika sudah lebih dari 2 detik untuk menghindari flood
                if time.time() - last_edit_time > 2:
                    await status_message.edit(f"<pre>{status_text}</pre>", parse_mode='html')
                    last_edit_time = time.time()

                print(f"   [ScanGroup] Iterating with filter: '{filter_char}' in group {target_chat_id}")
                async for participant in self.client.iter_participants(chat, search=filter_char):
                    if isinstance(participant, User) and participant.id != self.my_id:
                        await self.save_user_data(participant, active_chat_id=target_chat_id)
                        total_processed += 1
                        count_in_char += 1
                
                print(f"   [ScanGroup] Filter '{filter_char}' selesai: {count_in_char} user ditemukan.")
                self.data_store.scan_status[str(target_chat_id)] = {'filter_index': i + 1, 'total_processed_since_start': total_processed}
                self.data_store.mark_dirty()
                await self.data_store.save_data_async()

                await asyncio.sleep(5) # Jeda 5 detik antar filter

            except MessageNotModifiedError:
                # Ini normal, terjadi jika teks status tidak berubah. Abaikan saja.
                pass
            except Exception as e:
                print(f"🛑 [ScanGroup] CRITICAL ERROR on filter '{filter_char}': {e}")
                error_text = f"<pre>❌ ERROR on filter '{filter_char}'\nREASON: {e}</pre>"
                await status_message.edit(error_text, parse_mode='html')
                return

        final_text = (
            f"GROUP: {chat.title}\n"
            f"=================================\n"
            f"STATUS : ✅ Scan Complete!\n"
            f"TOTAL  : {total_processed} pengguna diproses."
        )
        await status_message.edit(f"<pre>{final_text}</pre>", parse_mode='html')
        
        if str(target_chat_id) in self.data_store.scan_status:
            del self.data_store.scan_status[str(target_chat_id)]
            self.data_store.mark_dirty()

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
        
        summary = {
            "scanned_count": 0,
            "total_users_processed": 0,
            "failed_groups": []
        }
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        last_edit_time = 0

        for i, chat in enumerate(groups_to_scan):
            target_chat_id = chat.id
            users_in_group = 0
            
            status_text = (
                f"SCAN SEMUA GRUP ({i+1}/{total_groups})\n"
                f"=================================\n"
                f"GRUP SAAT INI: {chat.title}\n"
                f"STATUS: Memulai..."
            )
            if time.time() - last_edit_time > 2:
                await status_message.edit(f"<pre>{status_text}</pre>", parse_mode='html')
                last_edit_time = time.time()

            try:
                print(f"   [ScanAll] Scanning group '{chat.title}' ({target_chat_id})")
                # Loop through alphabet filters for the current group
                for filter_char in alphabet:
                    print(f"      [ScanAll] Using filter '{filter_char}' for group '{chat.title}'")
                    async for participant in self.client.iter_participants(chat, search=filter_char):
                        if isinstance(participant, User) and participant.id != self.my_id:
                            await self.save_user_data(participant, active_chat_id=target_chat_id)
                            users_in_group += 1
                
                summary["scanned_count"] += 1
                summary["total_users_processed"] += users_in_group
                print(f"   [ScanAll] Finished scanning '{chat.title}', found {users_in_group} new entries.")
                
                # Save data after each successful group scan
                await self.data_store.save_data_async()

            except Exception as e:
                print(f"🛑 [ScanAll] CRITICAL ERROR scanning group '{chat.title}': {e}")
                summary["failed_groups"].append(f"{chat.title} ({e})")
                continue # Lanjutkan ke grup berikutnya
            
            await asyncio.sleep(10) # Jeda 10 detik antar grup

        # Final Summary Report
        summary_text = [
            "✅ **PEMINDAIAN SEMUA GRUP SELESAI**\n",
            f"Grup Sukses Dipindai: {summary['scanned_count']}/{total_groups}",
            f"Total Pengguna Diproses: {summary['total_users_processed']}"
        ]
        if summary['failed_groups']:
            summary_text.append("\n❌ **Grup Gagal:**")
            summary_text.extend([f"- {group_info}" for group_info in summary['failed_groups']])
        
        await status_message.edit('\n'.join(summary_text))
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
            # Coba ambil data pengguna dari cache/DB dulu
            user_history = self.data_store.get_user_history(str(target_user_id))
            
            if user_history:
                print(f"   [ScanUser] User {target_user_id} found in local data.")
                # Optionally, refresh data from Telegram
                try:
                    live_entity = await self.client.get_entity(target_user_id)
                    await self.save_user_data(live_entity)
                    await event.reply(f"✅ Data untuk pengguna `{target_user_id}` telah diperbarui dari Telegram.")
                except Exception as e:
                    await event.reply(f"✅ Pengguna `{target_user_id}` ditemukan di database. Tidak dapat mengambil data live: `{e}`")
            else:
                # Jika tidak ada di DB, fetch dari telegram
                print(f"   [ScanUser] User {target_user_id} not in DB. Fetching from Telegram.")
                try:
                    user_entity = await self.client.get_entity(target_user_id)
                    if isinstance(user_entity, User):
                        await self.save_user_data(user_entity)
                        await event.reply(f"✅ Pengguna baru `{target_user_id}` telah ditemukan dan disimpan ke database.")
                    else:
                        await event.reply(f"⚠️ Entitas `{target_user_id}` bukan seorang pengguna.")
                except Exception as e:
                    await event.reply(f"❌ **Error:** Tidak dapat menemukan pengguna dengan ID `{target_user_id}` di Telegram.\n`{e}`")

        except Exception as e:
            print(f"🛑 [ScanUser] CRITICAL ERROR: {e}")
            await event.reply(f"❌ Terjadi kesalahan internal saat memproses permintaan Anda.")

    async def clear_checkpoint(self, event, *args):
        target_id_str = args[0] if args else str(event.chat_id)
        print(f"🗑️ [CMD /clear_checkpoint] Clearing checkpoint for Chat ID: {target_id_str}")
        if target_id_str in self.data_store.scan_status:
            del self.data_store.scan_status[target_id_str]
            self.data_store.mark_dirty()
            await self.data_store.save_data_async()
            await event.reply(f"✅ Checkpoint pemindaian untuk grup `{target_id_str}` telah dihapus.")
        else:
            await event.reply(f"ℹ️ Tidak ada checkpoint aktif untuk grup `{target_id_str}`.")

if __name__ == '__main__':
    bot = None
    try:
        bot = TeleScrapeTracker()
        bot.start()
    except Exception as e:
        print(f"💥 [Main] An unexpected error occurred: {e}")
    finally:
        if bot and bot.data_store.is_dirty() and bot.loop.is_running():
            print("\n🛑 [Main] Shutting down. Performing final data save...")
            bot.loop.run_until_complete(bot.data_store.save_data_async())
            print("✅ [Main] Final data save complete. Bot stopped.")
