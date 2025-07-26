import sys
import sqlite3
import os
import time
import random
import main_request
import json
import datetime  # Pastikan impor ini ada di atas
from database_connect import *
# Konstanta konfigurasi retry


#fungsi utama transaksi nik
def nik(json_text_data):
    conn = connect_with_retry()
    cursor = conn.cursor()
    print(json_text_data)
    try:
        # Ambil id_user_aktif dari temp_status
        row = fetchone_with_retry(cursor, "SELECT id_user_aktif FROM temp_status LIMIT 1")
        if not row or row[0] is None:
            
            return

        id_user = row[0]

        # Parse JSON
        try:
            data_input = json.loads(json_text_data)
        except json.JSONDecodeError as e:
            
            return

        if not isinstance(data_input, list):
            
            return

        # Validasi isi JSON
        valid_data = json.loads(json_text_data)
        print(valid_data)
        if not valid_data:
            
            return

        # Bersihkan isi temp_transaksi
        execute_with_retry(cursor, "DELETE FROM temp_transaksi")

        # Masukkan data ke temp_transaksi
        data_to_insert = [
            (i, valid_data[i]['nama'], valid_data[i]['nik'], valid_data[i]['jumlah'], None, None)
            for i in range(len(valid_data))
        ]
        print(data_to_insert)

        for row in data_to_insert:
            execute_with_retry(cursor, """
                INSERT INTO temp_transaksi (id, nama, nik, proses, status, log)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row)

        conn.commit()

        # Update total_transaksi dan stok_produk
        update_data(conn, len(data_to_insert))

    finally:
        conn.close()
# Fungsi utama otomatis
def otomatis(jumlah):

    conn = connect_with_retry()
    cursor = conn.cursor()

    try:
        # Ambil id_user_aktif dari temp_status
        row = fetchone_with_retry(cursor, "SELECT id_user_aktif FROM temp_status LIMIT 1")
        if not row or row[0] is None:
            return

        id_user = row[0]

        # Ambil pembeli yang sesuai
        semua_pembeli = fetchall_with_retry(cursor, "SELECT nama, nik FROM pembeli WHERE id_users = ?", (id_user,))
        if not semua_pembeli:
            return

        random.shuffle(semua_pembeli)
        dipilih = semua_pembeli[:min(int(jumlah), len(semua_pembeli))]

        execute_with_retry(cursor, "DELETE FROM temp_transaksi")
        
        data = [
            (i, nama, nik, 1, None, None)
            for i, (nama, nik) in enumerate(dipilih)
        ]

        for row in data:
            execute_with_retry(cursor, """
                INSERT INTO temp_transaksi (id, nama, nik, proses, status, log)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row)

        conn.commit()
        update_data(conn, len(data))

    finally:
        conn.close()

# Update status tiap transaksi
def update_data(conn, jumlah):
    cursor = conn.cursor()
    
    # Ambil login_token dan id_user_aktif dari temp_status
    login_token = fetchone_with_retry(cursor, "SELECT login_token FROM temp_status LIMIT 1")[0]
    id_user_aktif = fetchone_with_retry(cursor, "SELECT id_user_aktif FROM temp_status LIMIT 1")[0]
    conn.commit()
    error_messages = [
        "Data tidak valid",
        "NIK tidak ditemukan",
        "Format NIK salah",
        "Terjadi kesalahan sistem",
        "NIK kosong",
        "Duplikat data"
    ]

    for i in range(int(jumlah)):
        try:
            row = fetchone_with_retry(cursor, "SELECT * FROM temp_transaksi WHERE id = ?", (i,))
            conn.commit()
            time.sleep(20)
            if row:
                id_transaksi, nama, nik, proses, status_lama, log_lama = row
                status_baru = ''
                log_baru = ''

                client_data = main_request.verify_nik(nik, login_token)
                if client_data['code'] in [200, 201]:
                    depo = main_request.products(login_token)
                    if depo['code'] in [200, 201]:
                        try:
                            product_id = depo["data"]["productId"]
                            client_token = client_data["data"]["token"]
                            family_id_encrypted = client_data["data"]["familyIdEncrypted"]

                            if len(client_data["data"]["customerTypes"]) != 1:
                                log_baru = f'untuk {nama} dengan nik {nik} sebaiknya diinputkan manual'
                                status_baru = 'failed'
                            else:
                                customer_type = client_data["data"]["customerTypes"][0]
                                category = customer_type["name"]
                                source_type_id = customer_type["sourceTypeId"]
                                name = client_data["data"]["name"]
                                channel_inject = client_data["data"]["channelInject"]

                                payload = main_request.build_subsidi_payload(
                                    product_id, int(proses), client_token, nik,
                                    family_id_encrypted, category, source_type_id,
                                    name, channel_inject
                                )
                                transaksi = main_request.post_transaction(payload, login_token)

                                if transaksi['code'] in [200, 201]:
                                    status_baru = 'success'
                                    log_baru = transaksi['message']
                                else:
                                    status_baru = 'failed'
                                    log_baru = transaksi['message']
                        except:
                            status_baru = 'failed'
                            log_baru = 'server pertamina gagal memproses'
                    else:
                        status_baru = 'failed'
                        log_baru = depo['message']
                else:
                    status_baru = 'failed'
                    log_baru = client_data['message']

                if '60000000000 menit' in log_baru:
                    log_baru = 'mohon matikan software dan nyalakan 15 menit lagi'

                #  Update ke temp_transaksi
                execute_with_retry(cursor, """
                    UPDATE temp_transaksi
                    SET status = ?, log = ?
                    WHERE id = ?
                """, (status_baru, log_baru, id_transaksi))
                conn.commit()

                #  Insert ke riwayat jika status success
                if status_baru == 'success':
                    waktu_hari_ini = datetime.date.today().isoformat()  # 'YYYY-MM-DD'
                    execute_with_retry(cursor, """
                        INSERT INTO riwayat (id_users, nik, nama, jumlah_beli, waktu)
                        VALUES (?, ?, ?, ?, ?)
                    """, (id_user_aktif, nik, nama, int(proses), waktu_hari_ini))
                    conn.commit()
            else:
                pass

        except Exception as e:
            print(e)

        time.sleep(0.5)

    #  Update status_proses_transaksi jadi 'tidak_aktif'
    try:
        execute_with_retry(cursor, """
            UPDATE temp_status
            SET status_proses_transaksi = 'tidak_aktif'
        """)
        conn.commit()

        
    except Exception as e:
        pass
# Entry point
if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "otomatis":
            otomatis(sys.argv[2])
        elif cmd == "nik":
            nik(sys.argv[2])
        else:
            print("ERROR: Unknown command:", cmd)
    else:
        print("ERROR: No command provided.")











































