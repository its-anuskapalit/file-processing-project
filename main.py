
import os
import time
import shutil
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv() 

# ---------- CONFIGURATION ----------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROCESSING_FOLDER = os.path.join(BASE_DIR, "Processing")
QUEUE_FOLDER = os.path.join(BASE_DIR, "in-queue")
PROCESSED_FOLDER = os.path.join(BASE_DIR, "Processed")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": 3306
}

# Create folders
for folder in (
    PROCESSING_FOLDER,
    QUEUE_FOLDER,
    PROCESSED_FOLDER
):
    os.makedirs(folder, exist_ok=True)

# ---------- DATABASE CONNECTION ----------

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

# ---------- DATABASE INITIALIZATION ----------

def init_db():

    conn = get_conn()

    try:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_name VARCHAR(255) NOT NULL UNIQUE,
                status TINYINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL
            )
        """)

        conn.commit()
        print("[DATABASE] Table ready")

    finally:
        cursor.close()
        conn.close()

# ---------- STAGE 1: GENERATE FILE ----------

def generate_file():

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    filename = f"file_{timestamp}.txt"

    temp_path = os.path.join(
        PROCESSING_FOLDER, filename + ".tmp"
    )

    final_path = os.path.join(
        PROCESSING_FOLDER, filename
    )

    with open(temp_path, "w") as file:
        file.write(f"File created at: {datetime.now()}")

    os.replace(temp_path, final_path)

    print(f"[GENERATED] {filename}")

# ---------- STAGE 2: MOVE TO QUEUE ----------

def move_to_queue():

    queue_files = [
        f for f in os.listdir(QUEUE_FOLDER)
        if os.path.isfile(os.path.join(QUEUE_FOLDER, f))
    ]

    # Do not move if even one file exists
    if queue_files:
        print("[MOVER] Queue is not empty. Waiting...")
        return

    processing_files = [
        f for f in os.listdir(PROCESSING_FOLDER)
        if os.path.isfile(os.path.join(PROCESSING_FOLDER, f))
        and not f.endswith(".tmp")
    ]

    for filename in processing_files:

        source = os.path.join(PROCESSING_FOLDER, filename)
        destination = os.path.join(QUEUE_FOLDER, filename)

        shutil.move(source, destination)

        print(f"[MOVED TO QUEUE] {filename}")

# ---------- STAGE 3: DATABASE PROCESSING ----------

def process_queue():

    files = [
        f for f in os.listdir(QUEUE_FOLDER)
        if os.path.isfile(os.path.join(QUEUE_FOLDER, f))
    ]

    for filename in files:

        source = os.path.join(QUEUE_FOLDER, filename)
        destination = os.path.join(PROCESSED_FOLDER, filename)

        conn = None
        cursor = None

        try:
            conn = get_conn()
            cursor = conn.cursor()

            # Insert pending record
            cursor.execute("""
                INSERT INTO file_records (file_name, status)
                VALUES (%s, 0)
                ON DUPLICATE KEY UPDATE
                status = IF(status = 1, 1, 0)
            """, (filename,))

            conn.commit()

            # Update status to processed
            cursor.execute("""
                UPDATE file_records
                SET status = 1,
                    processed_at = CURRENT_TIMESTAMP
                WHERE file_name = %s
            """, (filename,))

            conn.commit()

            # Move only after successful DB update
            shutil.move(source, destination)

            print(f"[PROCESSED] {filename} | DB status = 1")

        except Exception as error:

            if conn and conn.is_connected():
                conn.rollback()

            print(f"[ERROR] {filename}: {error}")

        finally:

            if cursor:
                cursor.close()

            if conn and conn.is_connected():
                conn.close()

# ---------- MAIN SYNCHRONOUS EXECUTION ----------

if __name__ == "__main__":

    init_db()

    print("\nFile Processing System Started\n")

    try:

        while True:

            # Stage 1
            generate_file()

            time.sleep(1)

            # Stage 2
            move_to_queue()

            # Stage 3
            process_queue()

            # Remaining time before next 5-second queue check
            time.sleep(4)

    except KeyboardInterrupt:

        print("\nSystem stopped by user.")