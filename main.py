
import asyncio
import os
import time
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import MessageNotModifiedError
from telethon.utils import get_peer_id # <--- PERBAIKAN PENTING
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

from mongo_data_store import MongoDataStore

load_dotenv()

class TeleScrapeTracker:
    def __init__(self, session_name='bot_session'):
        print("🤖 [TeleScrapeTracker] Initializing Bot...")
        self.API_ID = int(os.getenv('TG_API_ID'))
        self.API_HASH = os.getenv('TG_API_HASH')
        self.ADMIN_IDS = [int(i) for i in os.getenv('ADMIN_IDS', '').split(',') if i]
        self.MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
        self.BATCH_SIZE = 500

        self.client = TelegramClient(session_name, self.API_ID, self.API_HASH)
        self.loop = asyncio.get_event_loop()
        self.my_id = None
        self.chat_titles_cache = {}
        self.completed_scan_group_ids = set() 
        
        self.data_store = MongoDataStore(self.loop, self.MONGO_CONNECTION_STRING)
        
        self.client.add_event_handler(self.handle_command, events.NewMessage(pattern=r'^/[a-zA-Z_]+', forwards=False, from_users=self.ADMIN_IDS))
        self.client.add_event_handler(self.handle_passive_tracking, events.NewMessage(incoming=True, forwards=False))
        print("✅ [TeleScrapeTracker] Bot ready.")

    async def _initialize_connections(self):
        await self._ensure_my_id()
        await self.data_store.connect()
        self.completed_scan_group_ids = await self.data_store.get_completed_scan_ids()
        print(f"✅ [Init] Loaded {len(self.completed_scan_group_ids)} completed scan groups from DB.")
        total_users_in_db = await self.data_store.get_total_user_count()
        print(f"📊 [Init] Total users in database: {total_users_in_db}")

    async def _ensure_my_id(self):
        if self.my_id is None:
            me = await self.client.get_me()
            self.my_id = me.id
            print(f"   [Auth] Bot ID is {self.my_id}")

    async def _get_chat_title(self, chat_id: str):
        if chat_id in self.chat_titles_cache: return self.chat_titles_cache[chat_id]
        try:
            entity = await self.client.get_entity(int(chat_id)) # Convert to int ONLY for API call
            title = entity.title if hasattr(entity, 'title') else f"Chat Privasi"
            self.chat_titles_cache[chat_id] = title
            return title
        except Exception: return f"[Grup Tidak Dapat Diakses: {chat_id}]"

    def start(self):
        print("🚀 [TeleScrapeTracker] Starting client...")
        with self.client:
            self.client.loop.run_until_complete(self._initialize_connections())
            print(f"✅ [TeleScrapeTracker] Client connected.")
            self.client.run_until_disconnected()
        print("🛑 [TeleScrapeTracker] Client disconnected.")

    async def save_user_data(self, user_entity: User, active_chat_id: str = None, shared_chats: list = None):
        if not isinstance(user_entity, User): return False
        
        user_id = str(user_entity.id)
        current_identity = {'full_name': (user_entity.first_name or "") + (" " + (user_entity.last_name or "") if user_entity.last_name else ""), 'username': user_entity.username}
        if not current_identity['username'] and not current_identity['full_name']: return False

        history = await self.data_store.get_user_history(user_id)
        last_entry = history[-1] if history else {}
        
        if last_entry.get('username') and not current_identity['username']: return False

        last_active_chats = set(last_entry.get('active_chats_snapshot', []))
        is_new_group = active_chat_id and active_chat_id not in last_active_chats
        identity_changed = (last_entry.get('full_name') != current_identity['full_name'] or last_entry.get('username') != current_identity['username'])

        if not history or identity_changed:
            if active_chat_id: last_active_chats.add(active_chat_id)
            new_entry = {'timestamp': int(time.time()), 'full_name': current_identity['full_name'], 'username': current_identity['username'], 'active_chats_snapshot': sorted(list(last_active_chats)), 'shared_chats': shared_chats or last_entry.get('shared_chats', [])}
            await self.data_store.save_user_data_logic(user_id, new_entry, is_update=False)
            return True
        elif is_new_group:
            last_active_chats.add(active_chat_id)
            last_entry['active_chats_snapshot'] = sorted(list(last_active_chats))
            if shared_chats:
                existing_shared = set(last_entry.get('shared_chats', [])); existing_shared.update(shared_chats)
                last_entry['shared_chats'] = sorted(list(existing_shared))
            await self.data_store.save_user_data_logic(user_id, last_entry, is_update=True)
            return True
        return False

    async def handle_passive_tracking(self, event):
        chat_id_str = str(event.chat_id)
        if chat_id_str not in self.completed_scan_group_ids: return
        if event.sender_id == self.my_id or event.sender_id in self.ADMIN_IDS: return
        try:
            sender = await event.get_sender()
            if isinstance(sender, User) and event.is_group:
                group_title = await self._get_chat_title(chat_id_str)
                sender_name = sender.first_name or ""
                print(f"🕵️  [PassiveTrack] Saw message from {sender_name} ({sender.id}) in group '{group_title}' ({chat_id_str}).")
                await self.save_user_data(sender, active_chat_id=chat_id_str)
        except Exception as e: print(f"-❗️- [PassiveTrack] Minor exception: {e}")

    async def handle_command(self, event):
        print(f"⚙️ [HandleCommand] Admin {event.sender_id} sent command: {event.raw_text}")
        command, *args = event.raw_text.split()
        command_map = {'/hisz': self.show_history, '/scan_group': self.scan_group, '/scan_allgrup': self.scan_all_groups, '/scan_user': self.scan_user_details, '/clear_checkpoint': self.clear_checkpoint, '/scanstatus': self.scan_status, '/addgroup': self.add_group}
        if command in command_map: await command_map[command](event, *args)

    async def show_history(self, event, *args):
        if not args: await event.reply("Usage: `/hisz <user_id>`"); return
        user_id_str = args[0]
        print(f"   [CMD /hisz] Looking up history for User ID: {user_id_str}")
        
        try:
            live_entity = await self.client.get_entity(int(user_id_str))
            await self.save_user_data(live_entity)
        except Exception: live_entity = None
        
        history = await self.data_store.get_user_history(user_id_str)
        if not history: await event.reply(f"❌ Tidak ada riwayat untuk User ID `{user_id_str}`."); return

        last_entry = history[-1]
        username = f"@{live_entity.username}" if live_entity and live_entity.username else f"@{last_entry.get('username', 'N/A')} (Offline)"
        header = f"**Riwayat untuk ID:** `{user_id_str}`\n**Nama Terakhir:** `{last_entry['full_name']}`\n**Username:** `{username}`"
        
        history_blocks = [f"`{time.strftime('%Y-%m-%d', time.localtime(e['timestamp']))}`: `{e['full_name']}` (`@{e.get('username', 'N/A')}`)" for e in reversed(history)]
        groups_output = ""
        for key, title in [('active_chats_snapshot', 'Terlihat di Grup'), ('shared_chats', 'Grup Bersama')]:
            if chat_ids := last_entry.get(key):
                titles = [await self._get_chat_title(cid) for cid in chat_ids]
                groups_output += f"\n\n**{title}:**\n- " + "\n- ".join(titles)
        
        await event.reply(f"{header}\n\n**Perubahan Identitas:**\n<pre>" + '\n'.join(history_blocks) + "</pre>" + groups_output, parse_mode='html')

    async def scan_group(self, event, *args):
        if not args: await event.reply("Usage: `/scan_group <group_id>`"); return
        chat_id_str = args[0]
        msg = await event.reply(f"<code>Mempersiapkan pemindaian grup {chat_id_str}...</code>", parse_mode='html')
        await self._perform_group_scan(chat_id_str, msg)

    async def _perform_group_scan(self, chat_id_str: str, msg, chat_title_override=None):
        print(f"🔎 [Scan] Initiating scan for Chat ID: {chat_id_str}")
        try:
            # Panggilan API selalu butuh integer
            chat_peer_id = int(chat_id_str)
            chat = await self.client.get_entity(chat_peer_id)
            if isinstance(chat, User): await msg.edit("❌ Error: ID milik pengguna."); return False
            try: full = await self.client(GetFullChannelRequest(channel=chat)); count = full.full_chat.participants_count
            except TypeError: full = await self.client(GetFullChatRequest(chat_id=chat_peer_id)); count = len(full.users)
            title = chat.title
        except Exception as e: await msg.edit(f"❌ Error: Tidak dapat mengakses grup.\n`{e}`"); return False
        
        print(f"   [Scan] Group '{title}' has {count} members.")
        if count < 10000: await self._direct_scan_group(msg, chat, chat_id_str, count, title)
        else: await self._filtered_scan_group(msg, chat, chat_id_str, title)
        
        await self.data_store.mark_scan_as_completed(chat_id_str)
        self.completed_scan_group_ids.add(chat_id_str)
        return True

    async def _update_scan_msg(self, msg, text, last_edit):
        if time.time() - last_edit > 3:
            try: await msg.edit(f"<pre>{text}</pre>", parse_mode='html'); return time.time()
            except MessageNotModifiedError: pass
        return last_edit

    async def _direct_scan_group(self, msg, chat, chat_id_str, count, title):
        processed, saved, last_edit, batch = 0, 0, 0, []
        async for p in self.client.iter_participants(chat):
            processed += 1
            if p.id != self.my_id: batch.append({'user_entity': p, 'active_chat_id': chat_id_str})
            if len(batch) >= self.BATCH_SIZE:
                saved += await self.data_store.update_user_history_batch(batch); batch = []
                print(f"   [Scan] Batch of {self.BATCH_SIZE} processed for {title}. Total saved: {saved}")
            text = f"GROUP: {title}\nMETHOD: Direct (Batch)\nPROCESSED: {processed}/{count}\nSAVED: {saved}"
            last_edit = await self._update_scan_msg(msg, text, last_edit)
        if batch: saved += await self.data_store.update_user_history_batch(batch)
        await msg.edit(f"<pre>GROUP: {title}\nSTATUS: ✅ Scan Selesai\nTOTAL DISIMPAN: {saved}</pre>", parse_mode='html')

    async def _filtered_scan_group(self, msg, chat, chat_id_str, title):
        status = await self.data_store.get_scan_status(chat_id_str)
        alphabet = "abcdefghijklmnopqrstuvwxyz"; filter_idx = status.get('filter_index', 0)
        saved = status.get('total_saved_since_start', 0)
        for i in range(filter_idx, len(alphabet)):
            char = alphabet[i]
            print(f"   [Scan] Filter: '{char}' in group {chat_id_str}")
            await msg.edit(f"<pre>GROUP: {title}\nMETHOD: Filtered\nFILTER: '{char}'\nSAVED: {saved}</pre>", parse_mode='html')
            batch = []
            async for p in self.client.iter_participants(chat, search=char):
                if p.id != self.my_id: batch.append({'user_entity': p, 'active_chat_id': chat_id_str})
                if len(batch) >= self.BATCH_SIZE:
                    saved += await self.data_store.update_user_history_batch(batch); batch = []
                    print(f"   [Scan] Batch of {self.BATCH_SIZE} for {title} on filter '{char}'. Total saved: {saved}")
            if batch: saved += await self.data_store.update_user_history_batch(batch)
            await self.data_store.update_scan_status(chat_id_str, {'filter_index': i + 1, 'total_saved_since_start': saved})
            await asyncio.sleep(2)
        await msg.edit(f"<pre>GROUP: {title}\nSTATUS: ✅ Scan Selesai\nTOTAL DISIMPAN: {saved}</pre>", parse_mode='html')
        await self.data_store.update_scan_status(chat_id_str, {})

    async def scan_all_groups(self, event, *args):
        msg = await event.reply("<code>Mempersiapkan scan semua grup...</code>", parse_mode='html')
        try:
            dialogs = await self.client.get_dialogs()
            groups = [d.entity for d in dialogs if d.is_group or (d.is_channel and d.entity.megagroup)]
        except Exception as e: await msg.edit(f"❌ Error: Gagal mendapatkan daftar grup.\n`{e}`"); return
        summary = {"scanned": 0, "failed": []}
        for i, chat in enumerate(groups):
            # === PERBAIKAN KONSISTENSI DI SINI ===
            chat_id_str = str(get_peer_id(chat))
            print(f"   [ScanAll] Scanning group {i+1}/{len(groups)}: {chat.title} ({chat_id_str})")
            await msg.edit(f"<pre>SCAN SEMUA GRUP ({i+1}/{len(groups)})\nGRUP: {chat.title}</pre>", parse_mode='html')
            try:
                if await self._perform_group_scan(chat_id_str, msg, chat_title_override=chat.title): summary["scanned"] += 1
                else: summary["failed"].append(chat.title)
            except Exception as e: print(f"   [ScanAll] CRITICAL error scanning {chat.title}: {e}"); summary["failed"].append(f"{chat.title} (Error)")
            if i < len(groups) - 1: print("   [ScanAll] Pausing for 10 seconds..."); await asyncio.sleep(10)
        failed_list = "\n- ".join(summary['failed'])
        final_text = f"✅ **PEMINDAIAN SEMUA GRUP SELESAI**\n\nSukses: {summary['scanned']}\nGagal: {len(summary['failed'])}"
        if summary['failed']: final_text += f"\n\n**Grup Gagal:**\n- {failed_list}"
        await msg.edit(final_text, parse_mode='md')

    async def scan_user_details(self, event, *args):
        if not args: await event.reply("Usage: `/scan_user <user_id>`"); return
        user_id_str = args[0]
        msg = await event.reply(f"<code>Mencari grup bersama dengan user {user_id_str}...</code>", parse_mode='html')
        try:
            user = await self.client.get_entity(int(user_id_str))
            common_chats = await self.client.get_common_chats(int(user_id_str))
            # === PERBAIKAN KONSISTENSI DI SINI ===
            common_chat_ids = [str(get_peer_id(c)) for c in common_chats]
            if await self.save_user_data(user, shared_chats=common_chat_ids):
                await msg.edit(f"✅ Data grup bersama untuk `{user_id_str}` telah diperbarui.")
            else: await msg.edit(f"ℹ️ Tidak ada data baru untuk `{user_id_str}`.")
        except Exception as e: await msg.edit(f"❌ Error: Gagal memindai user.\n`{e}`")

    async def clear_checkpoint(self, event, *args):
        if not args: await event.reply("Usage: `/clear_checkpoint <group_id>`"); return
        chat_id_str = args[0]
        await self.data_store.clear_scan_record(chat_id_str)
        if chat_id_str in self.completed_scan_group_ids: self.completed_scan_group_ids.remove(chat_id_str)
        await event.reply(f"✅ Catatan pemindaian untuk grup `{chat_id_str}` telah dihapus.")

    async def scan_status(self, event, *args):
        msg = await event.reply("<code>Mengambil status pemindaian...</code>", parse_mode='html')
        dialogs = await self.client.get_dialogs()
        # === PERBAIKAN KONSISTENSI DI SINI ===
        groups = {str(get_peer_id(d.entity)): d.entity.title for d in dialogs if d.is_group or (d.is_channel and d.entity.megagroup)}
        scanned = [f"- {groups.get(gid, '[N/A]')} (`{gid}`)" for gid in self.completed_scan_group_ids if gid in groups]
        unscanned = [f"- {title} (`{gid}`)" for gid, title in groups.items() if gid not in self.completed_scan_group_ids]
        text = "**Status Pemindaian Grup**\n\n✅ **Selesai (Pasif Aktif):**\n" + ('\n'.join(scanned) or "Tidak ada.")
        text += "\n\n❌ **Belum di Scan:**\n" + ('\n'.join(unscanned) or "Semua grup telah dipindai.")
        await msg.edit(text, parse_mode='md')

    async def add_group(self, event, *args):
        if not args: await event.reply("Usage: `/addgroup <group_id>`"); return
        group_id_str = args[0]
        await self.data_store.add_completed_scan_id(group_id_str)
        self.completed_scan_group_ids.add(group_id_str)
        await event.reply(f"✅ Grup `{group_id_str}` ditambahkan ke daftar pelacakan pasif.")

if __name__ == '__main__':
    bot = TeleScrapeTracker()
    bot.start()
