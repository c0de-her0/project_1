import pandas as pd
import sqlite3
import os
import time
from io import BytesIO

def hapus_pembeli(nik, db_path='scripts/data.db', max_retries=5):
    retry_count = 0
    id_user = 2  # hardcoded ID user

    while retry_count < max_retries:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Cek apakah NIK tersebut ada untuk user yang dimaksud
            cursor.execute("SELECT 1 FROM pembeli WHERE id_users = ? AND nik = ? LIMIT 1", (id_user, nik))
            if not cursor.fetchone():
                print(f"NIK '{nik}' tidak ditemukan untuk user ID {id_user}.")
                conn.close()
                return False

            # Hapus data
            cursor.execute("DELETE FROM pembeli WHERE id_users = ? AND nik = ?", (id_user, nik))
            conn.commit()
            conn.close()
            print(f"NIK {nik} berhasil dihapus.")
            return True

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                retry_count += 1
                print(f"Database is locked. Retry {retry_count}/{max_retries}")
                time.sleep(0.3)
            else:
                raise

    raise Exception("Gagal menghapus data dari tabel pembeli: terlalu banyak retry.")


def import_excel_and_hapus_pembeli(file_path, db_path='scripts/data.db'):
    if not os.path.exists(file_path):
        print("File tidak ditemukan.")
        return

    try:
        df = pd.read_excel(file_path, dtype=str)

        # Normalisasi kolom
        df.columns = [str(col).strip().lower() for col in df.columns]
        if 'nik' not in df.columns:
            print('Format template salah. Harus ada kolom "nik".')
            return

        deleted = 0
        for idx, row in df.iterrows():
            nik = str(row['nik']).strip()

            if not nik.isdigit():
                continue

            try:
                if hapus_pembeli(nik, db_path):
                    deleted += 1
            except Exception as e:
                print(f"Gagal hapus {nik}: {e}")

        if deleted == 0:
            print("Tidak ada data yang berhasil dihapus.")
        else:
            print(f"Berhasil menghapus {deleted} data dari Excel.")

    except Exception as e:
        print(f"Terjadi kesalahan saat membaca Excel: {e}")


# Eksekusi penghapusan dari file Excel
import_excel_and_hapus_pembeli('file.xlsx')
