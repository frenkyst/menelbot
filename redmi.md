ubah saja database nya jika sudah ada 1 grup tambahkan saja grup ke2 
di bawahnya abaikan time stampnya jadi selama user tidak ganti nama dan username 
masukan saja ke timestam yang awal user dicatat kecuali user menganti username baru timestamp baru dicatat 
dan grup semua grup yang ada ditambahkan ke pencatatan usernamebaru

Perintah /scan_group dijalankan.
Identitas sama, tetapi grup baru ditambahkan ke snapshot.
Logika menganggap "penambahan grup baru" ini memerlukan entri baru, 
padahal sebenarnya Anda hanya ingin memperkaya Entri 1 dengan data snapshot yang baru didapat.
dan ini active_chats_snapshot juga data jadi redudan

ketika username dan nama tidak berubah overwrite aja data pada id user tersebut 
nah baru kalo ada perubahan usernama atau nama baru turunan grup nya semua di ikutkan ke username dan nama yang baru
Jika nama/username TIDAK berubah dan hanya ada penambahan grup aktif -> MERGE ke entri terakhir (tidak buat entri baru, tidak ubah timestamp).
Jika nama/username BERUBAH -> buat entri baru dengan timestamp baru dan sertakan union dari semua grup yang pernah tercatat (shared + active), termasuk grup baru.
Seluruh daftar grup di-normalisasi & dideduplikasi berdasarkan id.

Untuk /scan_group yang hanya menambahkan grup baru dan identitas TIDAK berubah: fungsi akan MERGE grup ke entri terakhir 
(tidak membuat entri baru; timestamp lama tetap).
Untuk perubahan nama/username: dibuat entri baru dengan timestamp baru, 
dan entri baru akan mengandung union dari semua grup yang diketahui agar riwayat baru melacak grup terkait perubahan identitas.
Semua daftar grup dideduplikasi berdasarkan id.

Merge grup aktif ke entri terakhir jika nama/username tidak berubah (tanpa ubah timestamp).
Buat entri baru jika nama/username berubah dan gabungkan semua grup (shared + active).
Deduplikasi/normalisasi ID grup.






