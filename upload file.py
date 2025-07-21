import pandas as pd
import sqlite3
import os
import time

def insert_pembeli(nik, nama, db_path='scripts/data.db', max_retries=5):
    retry_count = 0
    id_user = 1  # hardcoded ID user

    while retry_count < max_retries:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Cek duplikasi
            cursor.execute("SELECT 1 FROM pembeli WHERE id_users = ? AND nik = ? LIMIT 1", (id_user, nik))
            if cursor.fetchone():
                print(f"NIK '{nik}' sudah ada untuk user ID {id_user}.")
                conn.close()
                return False

            # Insert data
            cursor.execute("INSERT INTO pembeli (id_users, nik, nama) VALUES (?, ?, ?)", (id_user, nik, nama))
            conn.commit()
            conn.close()
            print(f"NIK {nik} berhasil ditambahkan.")
            return True

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                retry_count += 1
                print(f"Database is locked. Retry {retry_count}/{max_retries}")
                time.sleep(0.3)
            else:
                raise

    raise Exception("Gagal menyisipkan data ke tabel pembeli: terlalu banyak retry.")


def import_excel_and_insert_pembeli(file_path, db_path='scripts/data.db'):
    if not os.path.exists(file_path):
        print("File tidak ditemukan.")
        return

    try:
        df = pd.read_excel(file_path, dtype=str)

        # Normalisasi kolom
        df.columns = [str(col).strip().lower() for col in df.columns]
        if 'nama' not in df.columns or 'nik' not in df.columns:
            print('Format template salah. Harus ada kolom "nama" dan "nik".')
            return

        inserted = 0
        for idx, row in df.iterrows():
            nama = str(row['nama']).strip()
            nik = str(row['nik']).strip()

            if not nik.isdigit():
                continue

            try:
                if insert_pembeli(nik, nama, db_path):
                    inserted += 1
            except Exception as e:
                print(f"Gagal insert {nik} - {nama}: {e}")

        if inserted == 0:
            print("Tidak ada data valid yang berhasil diinput.")
        else:
            print(f"Berhasil menginputkan {inserted} data dari Excel.")

    except Exception as e:
        print(f"Terjadi kesalahan saat membaca Excel: {e}")



import_excel_and_insert_pembeli('file.xlsx')
