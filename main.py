import asyncio
import logging
import os
import sys
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from typing import Optional, Union, Tuple, Dict, Any, List

# Third-party imports
from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.errors import FloodWaitError, PeerIdInvalidError, RPCError, UserNotParticipantError

# Tambahkan import dotenv
from dotenv import load_dotenv

# --- MUAT VARIABEL LINGKUNGAN (.env) ---
# PENTING: Panggil fungsi ini di awal untuk memuat variabel TG_API_ID, TG_API_HASH, dan ADMIN_IDS
load_dotenv() 

# --- KONFIGURASI DAN INISIALISASI LOGGING ---
# Menggunakan level DEBUG untuk melihat pesan SKIP
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
# Ubah level logger jika ingin melihat pesan SKIP
# logger.setLevel(logging.DEBUG)

# --- KONFIGURASI TELEGRAM (SEKARANG AMBIL DARI .env) ---
# Jika TG_API_ID atau TG_API_HASH tidak ada di .env, ini akan menjadi None dan Telethon akan error.
# Ini adalah perilaku yang diinginkan untuk menghindari penggunaan hardcoded value.
API_ID = os.environ.get('TG_API_ID') 
API_HASH = os.environ.get('TG_API_HASH') 
SESSION_NAME = 'bot_session'

# Konstanta Delay
MESSAGE_EDIT_DELAY_SECONDS = 10.0  # Delay 10 detik setelah mengedit pesan di grup publik (untuk menghindari flood wait).
MIN_UPDATE_INTERVAL_HOURS = 0   # Batas minimal waktu (jam) untuk memproses ulang user yang sudah ada dan tidak berubah

# --- Daftar ID user admin yang diizinkan (Diproses dari string .env) ---
# Ambil string daftar admin (dipisahkan koma) dari .env. Beri nilai default string kosong jika tidak ditemukan.
ADMIN_IDS_STRING = os.environ.get('ADMIN_IDS', '')

# Proses string menjadi list of integers.
ALLOWED_ADMIN_IDS: List[int] = [
    int(id.strip()) 
    for id in ADMIN_IDS_STRING.split(',') 
    if id.strip().isdigit()
]
# Pastikan nilai admin tidak kosong (opsional, untuk debugging)
if not ALLOWED_ADMIN_IDS:
    logger.warning("⚠️ Peringatan: ALLOWED_ADMIN_IDS kosong. Periksa konfigurasi ADMIN_IDS di .env.")

# Filter untuk iterasi alfabetis (solusi workaround untuk batas 10K API)
PARTICIPANT_FILTERS = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    # Filter kosong untuk menangkap user yang tidak memiliki nama depan atau username (fallback)
    ''
]

# --- KONFIGURASI PENYIMPANAN LOKAL ---
USER_DATA_FILE = 'user_data.json'  # File untuk menyimpan data riwayat user
SCAN_STATUS_FILE = 'scan_status.json'  # File untuk menyimpan checkpoint pemindaian grup
SAVE_INTERVAL_SECONDS = 60  # Simpan ke disk setiap 60 detik

# --- DATA STORE LOKAL ---


class LocalDataStore:
    """Mengelola penyimpanan data user dan status pemindaian di memori (cache) dan persistence ke file JSON."""

    def __init__(self):
        self.user_data_cache = {}  # {user_id_str: {'history': [...]} }
        self.scan_status_cache = {}  # {chat_id_str: {'filter_index': 0, 'scanned_count': 0, 'last_updated': '...'}}
        self.data_modified = False  # Flag untuk menandai apakah ada perubahan yang perlu disimpan ke disk
        self.executor = ThreadPoolExecutor(max_workers=1)  # Executor untuk operasi I/O file yang sinkron

    def load_data(self):
        """Memuat data dari file JSON ke dalam cache saat startup."""
        try:
            if os.path.exists(USER_DATA_FILE):
                with open(USER_DATA_FILE, 'r', encoding='utf-8') as f:
                    self.user_data_cache = json.load(f)
                    logger.info(f"✅ Memuat {len(self.user_data_cache)} entri user dari '{USER_DATA_FILE}'.")

            if os.path.exists(SCAN_STATUS_FILE):
                with open(SCAN_STATUS_FILE, 'r', encoding='utf-8') as f:
                    self.scan_status_cache = json.load(f)
                    logger.info(f"✅ Memuat {len(self.scan_status_cache)} status scan dari '{SCAN_STATUS_FILE}'.")

        except Exception:
            logger.exception(f"🚨 GAGAL MEMUAT DATA DARI DISK.")

    def save_data(self):
        """Menulis data dari cache ke file JSON (operasi sinkron, dijalankan di thread)."""
        if not self.data_modified:
            return True

        logger.info(f"⏳ Menyimpan perubahan data ke disk... (User: {len(self.user_data_cache)}, Scan: {len(self.scan_status_cache)})")

        try:
            # FIX: Membuat salinan data untuk menghindari RuntimeError
            user_data_copy = self.user_data_cache.copy()
            scan_status_copy = self.scan_status_cache.copy()

            # Simpan data user
            with open(USER_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(user_data_copy, f, indent=4, ensure_ascii=False)

            # Simpan status scan
            with open(SCAN_STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(scan_status_copy, f, indent=4, ensure_ascii=False)

            self.data_modified = False
            logger.info("✅ Penyimpanan data ke disk berhasil.")
            return True
        except TypeError as te:
            logger.error(f"🚨🚨 GAGAL SIMPAN KE JSON (TIPE DATA TIDAK VALID) 🚨🚨: {te}")
            logger.error("Pastikan semua item di cache adalah tipe data dasar Python (string, int, dict, list).")
            return False
        except Exception:
            logger.exception("🚨🚨 STACK TRACE LENGKAP DARI SAVE JSON GAGAL 🚨🚨")
            return False

    def get_user_snapshot(self, user_id_str: str) -> Optional[dict]:
        """Mengambil data user dari cache."""
        return self.user_data_cache.get(user_id_str, None)

    def set_user_data(self, user_id_str: str, data: dict):
        """Menyimpan data user ke cache dan mengatur flag modifikasi."""
        self.user_data_cache[user_id_str] = data
        self.data_modified = True

    def get_scan_status(self, chat_id_str: str) -> Optional[Dict[str, Any]]:
        """Mengambil status scan dari cache."""
        return self.scan_status_cache.get(chat_id_str, None)

    def set_scan_status(self, chat_id_str: str, data: Dict[str, Any]):
        """Menyimpan status scan ke cache."""
        self.scan_status_cache[chat_id_str] = data
        self.data_modified = True

    def delete_scan_status(self, chat_id_str: str):
        """Menghapus status scan dari cache."""
        if chat_id_str in self.scan_status_cache:
            del self.scan_status_cache[chat_id_str]
            self.data_modified = True
            logger.info(f"✅ Status scan untuk '{chat_id_str}' berhasil dihapus dari cache.")
            return True
        return False

    def shutdown(self):
        """Menutup executor dan memastikan data tersimpan."""
        logger.info("Memastikan data terakhir tersimpan sebelum shutdown...")
        self.save_data()
        self.executor.shutdown(wait=True)
        logger.info("ThreadPoolExecutor JSON ditutup.")


# Inisialisasi Data Store
local_store = LocalDataStore()

# --- FUNGSI UTILITY CHECKPOINT (Fokus pada COUNT dan Filter Index) ---


async def get_scan_checkpoint(chat_id: Union[int, str]) -> Tuple[int, int]:
    """Mengambil index filter terakhir dan jumlah total yang telah dipindai (di sesi sebelumnya) dari cache."""
    chat_id_str = str(chat_id)
    status_data = local_store.get_scan_status(chat_id_str)

    filter_index = 0
    scanned_count = 0

    if status_data:
        filter_index = status_data.get('filter_index', 0)
        scanned_count = status_data.get('scanned_count', 0)

        # Pastikan index tidak melebihi batas filter
        if filter_index >= len(PARTICIPANT_FILTERS):
            filter_index = 0  # Reset jika sudah melebihi batas
            scanned_count = 0

        current_filter = PARTICIPANT_FILTERS[filter_index]

        logger.info(f"Checkpoint ditemukan untuk Grup ID {chat_id_str}: Filter Index: {filter_index} (Filter: '{current_filter}'), Scanned Count: {scanned_count}")

    return filter_index, scanned_count


async def set_scan_checkpoint(chat_id: Union[int, str], filter_index: int, count: int):
    """Menyimpan index filter terakhir dan jumlah total user yang berhasil dipindai sebagai checkpoint ke cache."""
    chat_id_str = str(chat_id)

    update_data = {
        'filter_index': filter_index,
        'scanned_count': count,
        'last_updated': datetime.now(timezone.utc).isoformat()
    }

    local_store.set_scan_status(chat_id_str, update_data)
    logger.info(f"Checkpoint disimpan ke cache untuk Grup ID {chat_id_str}: Filter Index: {filter_index}, Scanned Count: {count}")


# --- FUNGSI HAPUS STATUS SCAN LOKAL ---


async def delete_scan_status_local(chat_id_str: str):
    """Fungsi async untuk menghapus status scan dari cache."""
    if local_store.delete_scan_status(chat_id_str):
        logger.info(f"✅ Status scan '{chat_id_str}' berhasil dihapus dari cache.")
    else:
        logger.error(f"❌ Gagal menghapus status scan '{chat_id_str}' dari cache.")


# --- FUNGSI SAVE USER DATA (Anti Redundansi & Passive Tracking yang DITINGKATKAN) ---

# ...existing code...
async def save_user_data(user_id_str, user_entity, extra_data: Optional[Dict[str, Any]] = None):
    """
    Simpan/merge riwayat user:
    - extra_data: {'shared_chats': [...]} untuk /scan_user (manual)
                  {'active_group_info': {'chat_id': ..., 'title': ...}} untuk passive/scan_group
    """
    try:
        if user_entity is None:
            logger.warning(f"🚨 GAGAL MENDAPATKAN ENTITAS: Melewatkan penyimpanan data untuk user ID {user_id_str}. Entitas adalah None.")
            return False
        if getattr(user_entity, 'bot', False):
            return False

        doc_data = local_store.get_user_snapshot(user_id_str)

        # Ambil history terakhir
        history_list = []
        last_entry = None
        last_full_name = None
        last_username = None
        last_shared_chats = []
        last_active_chats = []

        if doc_data:
            history_list = doc_data.get('history', [])[:]
            if history_list:
                last_entry = history_list[-1]
                last_full_name = last_entry.get('full_name')
                last_username = last_entry.get('username')
                last_shared_chats = last_entry.get('shared_chats') or []
                last_active_chats = last_entry.get('active_chats_snapshot') or []

        is_manual_scan = bool(extra_data and 'shared_chats' in extra_data)
        is_passive_chat_update = bool(extra_data and 'active_group_info' in extra_data)
        is_passive_identity_update = not is_manual_scan and not is_passive_chat_update

        # Ekstrak data baru
        first_name = user_entity.first_name or ''
        last_name = user_entity.last_name or ''
        current_full_name = (first_name + ' ' + last_name).strip()
        current_username = user_entity.username  # bisa None

        # Helper merge/dedupe (id jadi string)
        def merge_chat_lists(*lists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            merged: Dict[str, Dict[str, str]] = {}
            for lst in lists:
                if not lst:
                    continue
                for c in lst:
                    cid = str(c.get('id'))
                    if not cid:
                        continue
                    title = c.get('title') or merged.get(cid, {}).get('title') or f"Chat {cid}"
                    # title terbaru (last appearance) menggantikan
                    merged[cid] = {'id': cid, 'title': title}
            return list(merged.values())

        # Normalisasi incoming active group (jika ada)
        current_active_chats = [c.copy() for c in (last_active_chats or [])]
        active_chat_added = False
        if is_passive_chat_update:
            ag = extra_data.get('active_group_info')
            if ag and 'chat_id' in ag:
                new_cid = str(ag['chat_id'])
                new_title = ag.get('title') or f"Chat {new_cid}"
                existing_ids = {str(c.get('id')) for c in (last_active_chats or [])}
                if new_cid not in existing_ids:
                    current_active_chats.append({'id': new_cid, 'title': new_title})
                    active_chat_added = True
                    logger.info(f"💾 Grup aktif baru ({new_title}) untuk user {user_id_str}.")
            else:
                logger.debug(f"Passive update tanpa active_group_info yang valid untuk user {user_id_str}.")

        # Shared chats jika manual
        current_shared_chats = extra_data.get('shared_chats') if is_manual_scan else None

        # Bandingkan identitas
        name_changed = current_full_name != last_full_name
        username_changed = current_username != last_username
        identity_changed = name_changed or username_changed

        # Abaikan kasus "username ada -> hilang" untuk update pasif/scan_group (tidak manual)
        is_username_nullified = (last_username and not current_username)
        if is_username_nullified and not name_changed and not is_manual_scan:
            logger.debug(f"⚠️ SKIP NULLIFICATION: Mengabaikan username removal untuk user {user_id_str} pada update pasif.")
            return False

        # Kasus MERGE sederhana: identitas sama, ada tambahan grup aktif -> update entri terakhir (tidak buat entri baru)
        if not identity_changed and active_chat_added and last_entry:
            # gabungkan last_entry.active_chats_snapshot dengan current_active_chats
            existing_active = last_entry.get('active_chats_snapshot') or []
            merged = merge_chat_lists(existing_active, current_active_chats)
            existing_ids = {str(c.get('id')) for c in existing_active}
            merged_ids = {str(c.get('id')) for c in merged}
            if merged_ids != existing_ids:
                last_entry['active_chats_snapshot'] = merged
                # pastikan shared_chats tetap ada jika sebelumnya ada
                if last_entry.get('shared_chats') is None and last_shared_chats:
                    last_entry['shared_chats'] = last_shared_chats
                local_store.set_user_data(user_id_str, {'history': history_list})
                logger.info(f"🔄 MERGE: Entri terakhir user {user_id_str} diperbarui dengan grup baru tanpa membuat entri baru.")
                return True
            else:
                logger.debug(f"⚠️ SKIP MERGE: Tidak ada perubahan grup nyata untuk user {user_id_str}.")
                return False

        # Manual scan: simpan jika identitas berubah, shared chats berubah, atau entri pertama
        if is_manual_scan:
            last_chat_ids = {str(c.get('id')) for c in (last_shared_chats or [])}
            curr_chat_ids = {str(c.get('id')) for c in (current_shared_chats or [])}
            shared_chats_changed = last_chat_ids != curr_chat_ids
            if not (identity_changed or shared_chats_changed or not history_list):
                logger.info(f"⚠️ SKIP: Pemindaian manual user {user_id_str} dilewatkan. Tidak ada perubahan.")
                return False

        # Untuk kasus lain (identity_changed, first entry, atau passive but not merge), buat entri baru
        if not identity_changed and not active_chat_added and history_list:
            # tidak ada perubahan yang perlu disimpan
            logger.debug(f"⚠️ SKIP: Tidak ada perubahan untuk user {user_id_str}.")
            return False

        # Bangun entri baru
        new_entry: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'full_name': current_full_name,
            'username': current_username,
        }

        # Jika identity berubah -> gabungkan semua grup yg dikenal agar entri baru menurunkan semua grup ke identitas baru
        if identity_changed:
            merged_all = merge_chat_lists(last_shared_chats or [], last_active_chats or [], current_active_chats or [])
            if merged_all:
                new_entry['active_chats_snapshot'] = merged_all
            # shared chats explicit jika tersedia
            if last_shared_chats or current_shared_chats:
                new_entry['shared_chats'] = merge_chat_lists(last_shared_chats or [], current_shared_chats or [])
        else:
            # bukan identity_changed (mis. first entry atau passive forced) -> tambahkan snapshot relevan
            if current_active_chats:
                new_entry['active_chats_snapshot'] = merge_chat_lists(current_active_chats)
            if is_manual_scan and current_shared_chats:
                new_entry['shared_chats'] = merge_chat_lists(current_shared_chats)

        # Append entri baru
        history_list.append(new_entry)
        local_store.set_user_data(user_id_str, {'history': history_list})
        logger.info(f"✅ BERHASIL SIMPAN (CACHE): Entri baru ditambahkan untuk user {user_id_str}.")
        return True

    except Exception as ex:
        logger.exception(f"🚨 EXCEPTION di save_user_data untuk user {user_id_str}: {ex}")
        return False
# ...existing code...
# --- AKHIR FUNGSI SAVE USER DATA ---


# --- FUNGSI TAMPILKAN RIWAYAT (PRIVAT KEPADA ADMIN) ---


async def show_history(client, admin_id, user_id_to_check):
    """Mengambil riwayat dari cache dan membandingkannya dengan data live Telegram."""
    user_id_str = str(user_id_to_check)

    logger.info(f"🔍 PERINTAH /hisz DARI ADMIN {admin_id}: Mencoba ambil data ID {user_id_str}.")

    # --- FASE 1: Ambil Data Live dari Telegram (Prio 1) ---
    live_entity = None
    tg_status_message = ""

    try:
        live_entity = await client.get_entity(user_id_to_check)

        first_name = live_entity.first_name if live_entity.first_name is not None else ''
        last_name = ' ' + live_entity.last_name if live_entity.last_name is not None else ''
        current_tg_full_name = (first_name + last_name).strip()
        current_tg_username = live_entity.username

        tg_status_message = "✅ **LIVE DATA DITEMUKAN** (Status Terkini dari Telegram)."
    except Exception:
        current_tg_full_name = None
        current_tg_username = None
        tg_status_message = "⚠️ **LIVE DATA GAGAL** (User Privat/Anonim/Tidak Terlihat). Tidak bisa memverifikasi status terkini."

    # --- FASE 2: Ambil Data Historical dari Cache (Prio 2) ---

    doc_data = local_store.get_user_snapshot(user_id_str)

    if doc_data is None:
        await client.send_message(admin_id, f"❌ Tidak ada data riwayat yang ditemukan di Cache/Disk untuk ID `{user_id_str}`. Status Telegram: {tg_status_message}")
        return

    history_list = doc_data.get('history', [])

    if not history_list:
        await client.send_message(admin_id, f"❌ Dokumen ditemukan untuk ID `{user_id_str}`, tetapi riwayatnya kosong. Status Telegram: {tg_status_message}")
        return

    # --- FASE 3: KOMPARASI & HEADER ---

    last_db_entry = history_list[-1]
    last_db_full_name = last_db_entry.get('full_name')
    last_db_username = last_db_entry.get('username')

    comparison_status_message = ""
    header_name = f"User ID {user_id_str}"

    if live_entity:
        header_name = f"{current_tg_full_name} (@{current_tg_username})" if current_tg_username else current_tg_full_name

        name_changed = current_tg_full_name != last_db_full_name
        user_changed = current_tg_username != last_db_username

        if name_changed or user_changed:
            comparison_status_message = "\n🔴 **STATUS PERUBAHAN BELUM TERSIMPAN (STALE DATA):**\n"
            comparison_status_message += f" - Nama Terakhir Disimpan: `{last_db_full_name}`\n"
            comparison_status_message += f" - Username Terakhir Disimpan: `@{last_db_username}`\n"
            comparison_status_message += " *Data live ini akan disimpan saat update user berikutnya/perintah /saveme."
        else:
            comparison_status_message = "🟢 **STATUS SINKRON:** Data live saat ini sama dengan entri riwayat terakhir yang tersimpan."
    else:
        header_name = f"{last_db_full_name} (@{last_db_username})" if last_db_username else last_db_full_name
        header_name += " (Data Terakhir Disimpan)"
        comparison_status_message = "🔵 **STATUS DATA:** Menggunakan nama/username terakhir yang dicatat di Cache."

    # --- FASE 4: PEMFORMATAN RIWAYAT ---

    grouped_history = []

    # Iterasi terbalik dari data sejarah
    for i in range(len(history_list) - 1, -1, -1):
        current_entry = history_list[i]
        previous_entry = history_list[i - 1] if i > 0 else {}

        current_fn = current_entry.get('full_name', '-')
        current_un = current_entry.get('username', '')

        previous_fn = previous_entry.get('full_name', '-')
        previous_un = previous_entry.get('username', '')

        try:
            timestamp_dt = datetime.fromisoformat(current_entry['timestamp'])
            timestamp_formatted = timestamp_dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')
        except (ValueError, TypeError):
            timestamp_formatted = current_entry.get('timestamp', 'Timestamp Error')

        index = i + 1
        change_lines = []

        name_change_detected = False
        if i == 0 or current_fn != previous_fn:
            name_change_detected = True
            if i == 0:
                change_lines.append(f"Nama: `{current_fn}`")
            else:
                change_lines.append(f"Nama: `{previous_fn}` -> `{current_fn}`")

        user_change_detected = False
        if i == 0 or current_un != previous_un:
            user_change_detected = True
            if i == 0:
                if current_un:
                    change_lines.append(f"User: `@{current_un}`")
                else:
                    change_lines.append("User: (Kosong)")
            else:
                old_un_display = f"@{previous_un}" if previous_un else "(Kosong)"
                new_un_display = f"@{current_un}" if current_un else "(Kosong)"
                change_lines.append(f"User: `{old_un_display}` -> `{new_un_display}`")

        # --- Riwayat Grup ---
        chats_info_str = ""
        chats_list_shared = current_entry.get('shared_chats')
        chats_list_active = current_entry.get('active_chats_snapshot')

        if chats_list_shared is not None:
            # Data dari /scan_user
            chats_info_str += f"\n- **Grup Bersama (/scan_user) Ditemukan:** ({len(chats_list_shared)} Grup)"

        if chats_list_active is not None:
            # Data dari Passive Chat Tracking & /scan_group
            chats_info_str += f"\n- **Grup Aktif (Pesan Baru/Scan):** ({len(chats_list_active)} Grup)"

        # Tampilkan detail grup jika ada salah satu dari dua data tersebut
        if chats_list_shared or chats_list_active:

            # Gabungkan kedua daftar untuk ditampilkan
            combined_chats = {}
            for chat in (chats_list_shared or []):
                combined_chats[str(chat['id'])] = {'title': chat['title'], 'source': 'Shared'}
            for chat in (chats_list_active or []):
                # Prioritaskan Shared jika ada konflik ID (meskipun keduanya harus sama)
                if str(chat['id']) not in combined_chats:
                    combined_chats[str(chat['id'])] = {'title': chat['title'], 'source': 'Active'}

            sorted_chat_ids = sorted(combined_chats.keys(), key=lambda x: combined_chats[x]['title'])

            chat_detail_lines = []
            for chat_id in sorted_chat_ids:
                chat_data = combined_chats[chat_id]
                chat_detail_lines.append(f"  - **{chat_data['title']}** (`{chat_id}`)")

            if chat_detail_lines:
                chats_info_str += "\n" + "\n".join(chat_detail_lines)
            else:
                chats_info_str += "\n  - **(TIDAK ADA CHAT DITEMUKAN)**"

            # Tentukan sumber entri riwayat
            is_group_scan_entry = chats_list_shared is not None
            is_group_active_entry = chats_list_active is not None

            if is_group_scan_entry and not is_group_active_entry:
                source_info = "Pemindaian Manual"
            elif is_group_active_entry and not is_group_scan_entry:
                source_info = "Aktivitas Pesan Baru/Scan Grup"
            else:
                source_info = "Perubahan Data/Aktivitas"

            change_lines.append(f"**{source_info} Ditemukan ({len(combined_chats)} Grup):** {chats_info_str.strip()}")
        # --- AKHIR RIWAYAT GRUP ---

        if name_change_detected or user_change_detected or (chats_list_shared is not None) or (chats_list_active is not None):
            change_lines_str = "\n".join(change_lines)
            report_entry = (
                f"**[{index}]** ({timestamp_formatted})\n"
                f"{change_lines_str}\n"
            )
            grouped_history.append(report_entry)

    max_entries = 100
    report_text = "\n".join(reversed(grouped_history[:max_entries]))  # Balikkan urutan agar terbaru di atas

    omitted_count = len(grouped_history) - max_entries
    omitted_message = f"\n*...dan {omitted_count} entri lainnya (Terlama tidak ditampilkan).* \n" if omitted_count > 0 else ""

    # Ubah urutan report_text agar yang terbaru di atas (index tertinggi)
    final_report = (
        f"**RIWAYAT DATA USER: {header_name}** (`{user_id_str}`)\n\n"
        f"**STATUS PENGAMBILAN DATA**\n"
        f"{tg_status_message}\n"
        f"{comparison_status_message}\n"
        f"--- RIWAYAT CACHE ({len(history_list)} ENTRi) ---\n"
        f"*(Menampilkan {min(len(grouped_history), max_entries)} entri perubahan/aktivitas terbaru)*\n"
        f"{report_text}"
        f"{omitted_message}"
    )

    logger.info(f"✅ BERHASIL AMBIL: Mengirimkan riwayat {len(history_list)} entri secara privat ke Admin {admin_id}.")

    await client.send_message(admin_id, final_report, parse_mode='markdown')


# --- FUNGSI SCAN GRUP DAN SIMPAN (METODE ALFABETIS) ---
# ... (Fungsi scan_and_save_group_members dimodifikasi di sini) ...


async def scan_and_save_group_members(event):
    """Mengambil semua peserta grup/channel menggunakan iterasi alfabetis (search filter) dan menyimpan data mereka ke cache."""

    client = event.client
    sender_id = event.sender_id
    text = event.raw_text.strip()
    parts = text.split()

    # Inisialisasi variabel
    chat_id_str = None
    chat_title = None
    participant = None
    initial_reply_message = None

    # Penghitung Sesi
    total_processed_since_start = 0     # Total API hits (bisa duplikat)
    unique_user_ids_in_session = set()  # Set untuk melacak ID unik di sesi ini
    unique_users_scanned = 0            # Total user unik
    count_saved = 0                     # Total user yang datanya benar-benar berubah/baru

    # --- 1. RESOLVE IDENTIFIER ---
    target_identifier = None
    if len(parts) > 1:
        identifier_str = parts[1]
        try:
            target_identifier = int(identifier_str)
        except ValueError:
            target_identifier = identifier_str.lstrip('@')

    chat_entity = None

    display_identifier = f"ID `{target_identifier}`" if isinstance(target_identifier, int) else f"Username `{target_identifier}`"
    if target_identifier is None:
        display_identifier = "grup saat ini"

    try:
        if target_identifier:
            chat_entity = await client.get_entity(target_identifier)
        else:
            chat_entity = await event.get_chat()

        if not chat_entity or not hasattr(chat_entity, 'title'):
            await event.reply(f"❌ Target `{display_identifier}` bukan Grup atau Channel yang valid.")
            return

        chat_id_str = str(chat_entity.id)
        chat_title = chat_entity.title if chat_entity.title else f"Channel/Grup ID {chat_entity.id}"

        # <<< MODIFIKASI DIMULAI: Persiapan data grup untuk disimpan ke snapshot user >>>
        group_data_for_save = {
            'chat_id': chat_id_str,
            'title': chat_title
        }
        # Gunakan kunci yang sama dengan Passive Tracking agar save_user_data mengolahnya sebagai update grup
        extra_data_for_scan = {'active_group_info': group_data_for_save}
        # <<< MODIFIKASI SELESAI >>>

        # --- 2. CEK CHECKPOINT DAN TENTUKAN OFFSET BERDASARKAN FILTER INDEX ---

        # Mengambil index filter terakhir dan total user yang telah dipindai (total processed, bukan unik).
        start_filter_index, total_processed_since_start = await get_scan_checkpoint(chat_id_str)

        resume_message = ""
        if start_filter_index > 0 or total_processed_since_start > 0:
            current_filter_char = PARTICIPANT_FILTERS[start_filter_index]
            resume_message = (
                f"\n➡️ Melanjutkan pemindaian dari FILTER ALFABETIS: '{current_filter_char}'. "
                f"(Total API hits di sesi sebelumnya: `{total_processed_since_start}`)."
                f"\n*Catatan: Metode ini menghasilkan duplikasi hit API untuk user yang sama, lihat laporan akhir untuk jumlah unik.*"
            )
            logger.info(f"Scan akan dilanjutkan dari Filter Index: {start_filter_index} (Filter: '{current_filter_char}').")

        # Kirim notifikasi awal di chat yang sama
        initial_reply_message = await event.reply(
            f"🚀 Mulai memindai **{chat_title}** (ID: `{chat_entity.id}`) menggunakan Metode Alfabetis."
            f"{resume_message}"
        )

        # Kirim notifikasi privat ke Admin untuk status proses
        admin_private_message = await client.send_message(
            sender_id,
            "ℹ️ Pemindaian dimulai. Notifikasi status akan diperbarui di sini..."
        )

        # --- 3. LOOP ITERASI ALFABETIS ---

        # Kita mengiterasi dari filter_index yang tersimpan (jika ada)
        for filter_index, current_filter in enumerate(PARTICIPANT_FILTERS):

            # Lompati filter yang sudah selesai di sesi sebelumnya
            if filter_index < start_filter_index:
                continue

            count_processed_in_current_filter = 0

            logger.info(f"=== MEMULAI FILTER: '{current_filter}' (Index: {filter_index}) ===")

            # Kirim update ke chat privat admin untuk filter baru
            await client.edit_message(admin_private_message,
                                      f"**=== MEMULAI FILTER: '{current_filter}' ({filter_index+1}/{len(PARTICIPANT_FILTERS)}) ===**\n"
                                      f"Total API hits sejauh ini: `{total_processed_since_start}`\n"
                                      f"Total user unik ditemukan: `{unique_users_scanned}`\n"
                                      f"Memulai pemindaian..."
                                      )

            # Iterasi menggunakan filter pencarian
            async for participant in client.iter_participants(
                    chat_entity,
                    search=current_filter,  # Menggunakan filter pencarian
                    filter=None,
                    limit=None
            ):

                # --- MULAI PROSES USER BARU ---

                try:
                    user_id_str = str(participant.id)

                    # 1. Update penghitung duplikat (Total API Hits)
                    count_processed_in_current_filter += 1
                    total_processed_since_start += 1

                    # 2. Update penghitung unik (Jika ID belum pernah ditemukan di sesi ini)
                    if user_id_str not in unique_user_ids_in_session:
                        unique_user_ids_in_session.add(user_id_str)
                        unique_users_scanned += 1

                    # 3. Simpan data user (Termasuk data grup yang sedang dipindai)
                    # Catatan: extra_data_for_scan dikirim di sini
                    if await save_user_data(user_id_str, participant, extra_data=extra_data_for_scan):
                        if not participant.bot:
                            count_saved += 1

                    # Simpan checkpoint setiap 100 user yang berhasil diproses
                    if count_processed_in_current_filter % 100 == 0:
                        # Checkpoint sekarang menyimpan index filter dan total API hits
                        await set_scan_checkpoint(chat_id_str, filter_index, total_processed_since_start)

                except Exception as e:
                    # Menangkap error yang terjadi saat memproses user tunggal
                    pid = getattr(participant, 'id', 'unknown')
                    logger.error(f"🚨 GAGAL memproses user ID {pid}. Melewati user ini. Error: {type(e).__name__}")
                    # Lanjutkan ke user berikutnya

                # --- UPDATE STATUS GANDA (PRIVAT & GRUP) ---

                # Buat teks status untuk pesan privat (detail)
                private_status_text = (
                    f"🔄 Filter '{current_filter}' ({filter_index+1}/{len(PARTICIPANT_FILTERS)}):\n"
                    f" - API Hits (Filter ini): `{count_processed_in_current_filter}`\n"
                    f" - Total API Hits (Keseluruhan): `{total_processed_since_start}`\n"
                    f" - **Total User Unik Ditemukan:** `{unique_users_scanned}`\n"
                    f"`{count_saved}` user datanya diperbarui/disimpan (Sesi Ini).\n"
                    f"*(Data akan ditulis ke disk setiap {SAVE_INTERVAL_SECONDS} detik)*"
                )

                # Kirim update ke chat privat admin (setiap 500 user - frekuensi sedang)
                if count_processed_in_current_filter % 500 == 0:
                    await client.edit_message(admin_private_message, private_status_text)

                # Kirim update ke pesan balasan awal (di grup/chat) (setiap 5000 user - frekuensi sangat rendah)
                if total_processed_since_start % 5000 == 0 and initial_reply_message:
                    group_status_text = (
                        f"🚀 Memindai **{chat_title}**... (Total User Unik: `{unique_users_scanned}` | Total Proses API: `{total_processed_since_start}`)\n"
                        f"Status detail dikirim secara privat kepada admin."
                    )
                    await initial_reply_message.edit(group_status_text)
                    # TUNDA 10 DETIK setelah mengedit pesan di grup publik untuk menghindari rate limit.
                    await asyncio.sleep(MESSAGE_EDIT_DELAY_SECONDS)

                # Delay singkat untuk pemrosesan rutin, terutama setelah pembaruan checkpoint/private message.
                elif count_processed_in_current_filter % 500 == 0:
                    await asyncio.sleep(0.2)

            # --- SELESAI FILTER ---

            # Checkpoint terakhir di akhir filter (jika tidak ada error)
            await set_scan_checkpoint(chat_id_str, filter_index, total_processed_since_start)

            # Setelah filter selesai, update pesan privat untuk konfirmasi
            await client.edit_message(admin_private_message,
                                      f"✅ Filter '{current_filter}' SELESAI. Total User Unik: `{unique_users_scanned}`. Beralih ke filter berikutnya..."
                                      )

        # --- SELESAI SEMUA FILTER (ITERASI LENGKAP) ---

        # Jika pemindaian selesai tanpa error (semua filter dilalui), hapus checkpoint
        await delete_scan_status_local(chat_id_str)

        message_parts = [
            "✅ **Pemindaian Selesai!** (Checkpoint Dihapus dari Cache)",
            f"- Grup: **{chat_title}**",
            f"--- LAPORAN AKHIR ---",
            f"1. **Total User Unik Ditemukan:** `{unique_users_scanned}`",
            f"2. **Total API Hits / Entitas Diproses (termasuk duplikat):** `{total_processed_since_start}`",
            f"3. **Jumlah Data User Baru/Diperbarui (Sesi Ini):** `{count_saved}`",
            f"Data terbaru anggota grup ini telah dicatat di cache. Data akan disimpan ke file JSON dalam waktu dekat."
        ]

        reply_message = "\n".join(message_parts)

        # Update pesan di grup/chat dan pesan privat
        if initial_reply_message:
            await initial_reply_message.edit(f"✅ Pemindaian **{chat_title}** Selesai!\nTotal User Unik: `{unique_users_scanned}`")

        await client.edit_message(admin_private_message, reply_message, parse_mode='markdown')

    except UserNotParticipantError:
        error_msg = f"❌ GAGAL: Bot harus menjadi anggota Grup/Channel **{display_identifier}** untuk memindai anggotanya."
        if initial_reply_message:
            await initial_reply_message.edit(error_msg)
        else:
            await event.reply(error_msg)
    except (ValueError, PeerIdInvalidError):
        await event.reply(f"❌ GAGAL: Tidak dapat menemukan entitas untuk **{display_identifier}**. Pastikan ID/Username benar.")
    except FloodWaitError as e:
        # Simpan checkpoint terakhir sebelum FloodWait
        if 'filter_index' in locals() and chat_id_str is not None:
            await set_scan_checkpoint(chat_id_str, filter_index, total_processed_since_start)
        await event.reply(f"⚠️ Peringatan Flood: Telegram meminta bot menunggu selama {e.seconds} detik sebelum mencoba lagi. Checkpoint (Filter '{PARTICIPANT_FILTERS[filter_index]}') telah disimpan di cache, Anda dapat melanjutkan nanti.")
    except TimeoutError:
        # Tambahan penanganan Timeout, simpan checkpoint jika memungkinkan
        if 'filter_index' in locals() and chat_id_str is not None:
            await set_scan_checkpoint(chat_id_str, filter_index, total_processed_since_start)
        await event.reply(f"⚠️ Kesalahan Timeout: Permintaan API Telegram Time-out. Checkpoint telah disimpan di cache, coba ulangi perintah `/scan_group` setelah beberapa saat.")
    except Exception as e:
        # Penanganan error fatal, simpan checkpoint jika ada
        if 'filter_index' in locals() and chat_id_str is not None:
            # Simpan checkpoint terakhir sebelum error
            if 'total_processed_since_start' in locals():
                await set_scan_checkpoint(chat_id_str, filter_index, total_processed_since_start)

        error_msg = f"❌ Terjadi kesalahan fatal saat memindai **{display_identifier}**. Error: `{type(e).__name__}`. Cek log bot. (Pastikan bot adalah anggota grup target)."
        logger.exception(f"🚨 GAGAL saat memindai grup. Error: {e}")
        await event.reply(error_msg)


# --- PENULISAN DATA TERJADWAL ---


async def scheduled_save_to_disk():
    """Fungsi yang berjalan secara berkala untuk menyimpan data dari cache ke disk."""
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        if local_store.data_modified:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(local_store.executor, local_store.save_data)
        else:
            logger.info("⏳ Tidak ada perubahan data di cache. Melewati penyimpanan ke disk.")


# --- HANDLER EVENT TELETHON ---


async def event_handler(event):
    """Handler universal untuk semua event update dan pesan."""

    sender_id = None
    user_entity = None

    # 1. Tangani Perubahan Entitas User (Otomatis)
    if isinstance(event, events.UserUpdate):
        user_entity = event.entity
        user_id_str = str(user_entity.id)

        if user_entity.bot:
            return

        logger.info(f"🔔 UPDATE DITERIMA: Memproses perubahan user ID {user_id_str}")
        # Panggilan pasif: Tidak ada extra_data (is_passive_identity_update)
        await save_user_data(user_id_str, user_entity, extra_data=None)
        return

    # 2. Tangani Perintah dan Pesan Baru
    if isinstance(event, events.NewMessage.Event):

        try:
            sender_id = event.sender_id
            client = event.client
            user_entity = await event.get_sender()
        except Exception:
            # Gagal mendapatkan sender (mungkin pesan layanan)
            return

        if user_entity and user_entity.bot:
            return

        user_id_str = str(sender_id)
        text = event.raw_text.strip()

        is_admin_command = text.startswith(('/saveme', '/hisz', '/scan_group', '/clear_checkpoint', '/scan_user'))

        if is_admin_command:
            if sender_id not in ALLOWED_ADMIN_IDS:
                await event.reply("❌ Akses Ditolak. Perintah ini hanya untuk admin yang terdaftar.")
                return

            # --- Aksi Admin (/saveme, /hisz, /scan_user, /scan_group, /clear_checkpoint) ---

            if text == '/saveme':
                # Panggilan pasif: Tidak ada extra_data, hanya update identitas
                await save_user_data(user_id_str, user_entity, extra_data=None)
                await event.reply(f"💾 Data Anda telah diperiksa dan diperbarui di cache. Akan disimpan ke disk dalam {SAVE_INTERVAL_SECONDS} detik.")
                return

            elif text.startswith('/hisz'):
                # Pastikan data admin terbaru tersimpan
                await save_user_data(user_id_str, user_entity, extra_data=None)

                parts = text.split()
                user_to_check = sender_id

                if len(parts) > 1:
                    target_identifier_str = parts[1].lstrip('@')
                    final_user_id_to_check = None

                    # --- REVERSE LOOKUP UNTUK ID GRUP ---
                    # Coba identifikasi sebagai ID Grup
                    if target_identifier_str.startswith('-100') or target_identifier_str.startswith('-'):
                        # Ini mungkin ID grup. Cari user ID yang memiliki grup ini di riwayat mereka.
                        group_id_to_find = target_identifier_str
                        found_user_ids = []

                        # Memindai seluruh data user di cache
                        for uid, user_doc in local_store.user_data_cache.items():
                            history_list_scan = user_doc.get('history', [])
                            if history_list_scan:
                                last_entry_scan = history_list_scan[-1]

                                # Cek di active_chats_snapshot (Passive/Scan Group)
                                active_chats = last_entry_scan.get('active_chats_snapshot', [])
                                if any(str(chat['id']) == group_id_to_find for chat in active_chats):
                                    found_user_ids.append(uid)
                                    continue  # Lanjut ke user berikutnya setelah ditemukan

                                # Cek di shared_chats (/scan_user)
                                shared_chats = last_entry_scan.get('shared_chats', [])
                                if any(str(chat['id']) == group_id_to_find for chat in shared_chats):
                                    found_user_ids.append(uid)

                        if found_user_ids:
                            # Jika ID Grup ditemukan di riwayat user
                            found_user_ids_str = "\n".join([f"`{uid}`" for uid in found_user_ids[:10]])  # Batasi 10 hasil

                            response_msg = (
                                f"🔍 **Pencarian Grup Berhasil!** ID Grup `{group_id_to_find}` ditemukan di riwayat user berikut:\n"
                                f"{found_user_ids_str}\n"
                                f"*(Hanya menampilkan 10 user pertama)*\n\n"
                                f"➡️ **Silakan ulangi perintah `/hisz` menggunakan salah satu ID User di atas** (misalnya: `/hisz {found_user_ids[0]}`) untuk melihat riwayatnya."
                            )
                            await client.send_message(sender_id, response_msg, parse_mode='markdown')
                            return
                        else:
                            await event.reply(f"❌ Tidak ada ID User yang tercatat memiliki ID Grup `{group_id_to_find}` di riwayatnya.")
                            return
                    # --- AKHIR REVERSE LOOKUP ---

                    try:
                        target_identifier_int = int(target_identifier_str)
                        try:
                            target_entity = await client.get_entity(target_identifier_int)
                            final_user_id_to_check = target_entity.id
                        except Exception:
                            final_user_id_to_check = target_identifier_int
                            logger.warning(f"⚠️ Gagal resolve entitas untuk ID {target_identifier_int}, melanjutkan ke cek cache.")
                    except ValueError:
                        try:
                            target_entity = await client.get_entity(target_identifier_str)
                            final_user_id_to_check = target_entity.id
                        except Exception as e:
                            logger.warning(f"Gagal mendapatkan entitas target '{target_identifier_str}': {e}")
                            await event.reply(f"❌ Tidak dapat menemukan user dengan identifier `{target_identifier_str}`. Pastikan ID atau username benar.")
                            return

                    user_to_check = final_user_id_to_check

                await show_history(client, sender_id, user_to_check)
                return

            elif text.startswith('/scan_user'):
                parts = text.split()
                if len(parts) < 2:
                    await event.reply("⚠️ **Penggunaan:** `/scan_user <ID_User_atau_Username>` (Satu user saja)")
                    return

                target_identifier_str = parts[1].lstrip('@')
                target_identifier = target_identifier_str

                try:
                    target_identifier = int(target_identifier_str)
                except ValueError:
                    target_identifier = target_identifier_str

                try:
                    target_entity = await client.get_entity(target_identifier)

                    if not isinstance(target_entity, User):
                        await event.reply(f"❌ Identifikasi `{target_identifier_str}` bukan entitas User yang valid (mungkin Grup/Channel).")
                        return

                    user_id_str_target = str(target_entity.id)

                    # --- FIND SHARED CHATS (Panggilan API mahal) ---
                    chats_list_for_save = []
                    try:
                        common_chats = await client.get_common_chats(target_entity)
                        for chat in common_chats:
                            chat_title = chat.title if chat.title else f"Chat ID {chat.id}"
                            chats_list_for_save.append({'id': str(chat.id), 'title': chat_title})
                        logger.info(f"✅ Ditemukan {len(chats_list_for_save)} chat bersama dengan user {user_id_str_target}.")

                    except Exception as e:
                        logger.warning(f"⚠️ Gagal mendapatkan chat bersama untuk user {user_id_str_target}: {type(e).__name__}. Lanjutkan tanpa data grup.")
                        # chats_list_for_save remains []

                    # Panggilan manual: extra_data dengan kunci 'shared_chats'
                    extra_data = {'shared_chats': chats_list_for_save}

                    display_name = target_entity.first_name + (f" @{target_entity.username}" if target_entity.username else "")

                    if await save_user_data(user_id_str_target, target_entity, extra_data):
                        group_count = len(chats_list_for_save)
                        group_msg = f" Data grup bersama **({group_count} grup)** juga dicatat."

                        instruction_msg = ""
                        if group_count > 0:
                            instruction_msg = f"\n➡️ **GUNAKAN `/hisz {user_id_str_target}` (secara pribadi) untuk melihat daftar {group_count} grup yang ditemukan.**"

                        await event.reply(f"✅ Data user **{target_entity.first_name}** (`{user_id_str_target}`) berhasil diambil. **Entri riwayat baru dibuat** untuk mencatat status ini.{group_msg}{instruction_msg}")
                    else:
                        await event.reply(f"ℹ️ Pemindaian user **{target_entity.first_name}** (`{user_id_str_target}`) dilewati. Tidak ada perubahan identitas atau daftar grup bersama sejak entri riwayat terakhir. **(ANTI REDUNDANSI)**")

                except Exception as e:
                    logger.exception(f"🚨 Gagal memproses /scan_user untuk {target_identifier_str}")
                    await event.reply(f"❌ Gagal memindai user `{target_identifier_str}`. Error: {type(e).__name__}")
                return

            elif text.startswith('/scan_group'):
                await scan_and_save_group_members(event)
                return

            elif text.startswith('/clear_checkpoint'):
                parts = text.split()
                if len(parts) < 2:
                    await event.reply("⚠️ **Penggunaan:** `/clear_checkpoint <ID_Grup_atau_Username_Channel>`")
                    return

                identifier_to_clear = parts[1]
                chat_entity = None
                try:
                    chat_entity = await client.get_entity(identifier_to_clear)
                    chat_id_str = str(chat_entity.id)
                    chat_title = chat_entity.title

                    await delete_scan_status_local(chat_id_str)
                    await event.reply(f"✅ Checkpoint pemindaian untuk **{chat_title}** (`{chat_id_str}`) berhasil dihapus dari cache. Pemindaian berikutnya akan dimulai dari awal (Filter 'a').")
                except Exception as e:
                    await event.reply(f"❌ Gagal menemukan atau menghapus checkpoint untuk `{identifier_to_clear}`. Error: {type(e).__name__}")
                return

        # --- TANGANI PESAN REGULER DARI NON-ADMIN (Passive Group Tracking) ---
        else:
            is_in_group_or_channel = event.is_group or event.is_channel

            if is_in_group_or_channel:
                try:
                    chat = await event.get_chat()
                    chat_title = chat.title if chat.title else f"Channel ID {chat.id}"

                    group_data = {
                        'chat_id': str(chat.id),
                        'title': chat_title
                    }
                    # Panggilan pasif: extra_data dengan kunci 'active_group_info'
                    extra_data = {'active_group_info': group_data}
                    await save_user_data(user_id_str, user_entity, extra_data=extra_data)
                except Exception as e:
                    logger.warning(f"⚠️ Gagal mendapatkan chat entity untuk passive tracking: {type(e).__name__}")
                    # Fallback ke update identitas pasif biasa jika gagal mendapatkan chat (misal, pesan dari channel tertutup)
                    await save_user_data(user_id_str, user_entity, extra_data=None)
            else:
                # Pesan pribadi/obrolan biasa, hanya update identitas
                await save_user_data(user_id_str, user_entity, extra_data=None)


# --- MAIN ---


async def main():
    """Fungsi utama untuk menjalankan klien Telegram."""

    # 1. Muat data dari disk ke cache saat startup
    local_store.load_data()

    logger.info("🚀 Mencoba menghubungkan sebagai User Klien...")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.start()

        me = await client.get_me()
        logger.info(f"✅ Login berhasil sebagai: {me.first_name} (ID: {me.id})")

        print(f"\n--- INFORMASI BOT ---")
        print(f"**PELACAKAN AKTIF (Penyimpanan Lokal):**")
        print(f"1. Data disimpan di file **{USER_DATA_FILE}**.")
        print(f"2. Data di-cache di memori dan disimpan ke disk setiap **{SAVE_INTERVAL_SECONDS} detik**.")
        print(f"3. **FIXED:** Logika anti-redundansi kini mencegah entri baru hanya untuk penambahan snapshot grup pertama.")
        print(f"4. **ANTI-REDUNDANSI AKTIF:** Entri riwayat baru hanya dibuat jika ada perubahan identitas/grup aktif/shared chats.")
        print(f"\n**PERINTAH ADMIN BARU (untuk kelanjutan):**")
        print(f"1. `/scan_user <ID/Username>`: Memindai **satu** user, mencatat riwayat perubahannya, dan **melacak Grup/Channel bersama (via API mahal)**.")
        print(f"2. `/scan_group <ID/Username>`: Memindai anggota grup menggunakan **Metode Alfabetis**, dan **melacak grup tersebut** di riwayat user.")
        print(f"3. `/clear_checkpoint <ID/Username>`: Menghapus status kemajuan pemindaian grup dari cache/disk.")
        print(f"4. `/hisz <ID/Username/ID_Grup>`: Menampilkan riwayat user (dari cache) dan status live Telegram. **Mendukung Reverse Lookup untuk ID Grup**.")
        print(f"5. `/saveme`: Memaksa pembaruan data riwayat untuk diri sendiri.")
        print(f"Perintah Admin diizinkan: {ALLOWED_ADMIN_IDS}")

        # Mulai proses penyimpanan terjadwal
        asyncio.create_task(scheduled_save_to_disk())
        logger.info(f"✨ Proses penyimpanan terjadwal dimulai (interval {SAVE_INTERVAL_SECONDS} detik).")

        client.add_event_handler(event_handler, events.NewMessage)
        client.add_event_handler(event_handler, events.UserUpdate)

        logger.info("✨ Client sedang mendengarkan perubahan dan perintah...")
        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"🚨 ERROR FATAL DI CLIENT: {e}")
    finally:
        if client.is_connected():
            await client.disconnect()
            logger.info("\n👋 Client dihentikan.")

        # Pastikan data tersimpan dan executor ditutup
        local_store.shutdown()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Program dihentikan oleh pengguna (Ctrl+C).")
    except Exception as e:
        logger.error(f"🚨 GAGAL MENJALANKAN ASYNCIO: {e}")
