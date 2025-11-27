import mysql.connector
from datetime import datetime, timedelta
import time
from threading import Thread

# ================== CONFIG ==================
db_config = {
    "host": "switchyard.proxy.rlwy.net",
    "user": "root",
    "port": 28085,
    "password": "NOtYUNawwodSrBfGubHhwKaFtWyGXQct",
    "database": "railway",
}

# ================== DB CONNECT ==================
def get_connection():
    return mysql.connector.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )

# ================== CLEANUP LOGIC ==================
def clean_old_readings():
    """
    Deletes all readings older than 7 days from device_reading_log.
    """
    print("🧹 Starting cleanup process...")
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # calculate cutoff datetime
        # cutoff_date = datetime.now() - timedelta(days=15)
        cutoff_date = datetime.now() - timedelta(days=7)
        print("📅 Deleting records older than:", cutoff_date.strftime("%Y-%m-%d %H:%M:%S"))

        # count old records before delete
        cursor.execute("""
            SELECT COUNT(*) AS cnt
            FROM device_reading_log
            WHERE TIMESTAMP(READING_DATE, READING_TIME) < %s
        """, (cutoff_date,))
        count = cursor.fetchone()["cnt"]

        if count > 0:
            cursor.execute("""
                DELETE FROM device_reading_log
                WHERE TIMESTAMP(READING_DATE, READING_TIME) < %s
            """, (cutoff_date,))
            conn.commit()
            print(f"✅ Deleted {count} old readings (older than 7 days)")
        else:
            print("✅ No old readings found to delete.")

    except Exception as e:
        print("❌ Cleanup failed:", e)
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# ================== SCHEDULER ==================
def start_cleanup_scheduler():
    """
    Run cleanup every 24 hours in a background thread.
    """
    def scheduler():
        while True:
            clean_old_readings()
            print("⏳ Next cleanup in 24 hours...")
            time.sleep(24 * 60 * 60)  # 24 hours

    t = Thread(target=scheduler, daemon=True)
    t.start()
    print("🚀 Cleanup scheduler started (every 24 hours)")

# ================== MAIN ==================
if __name__ == "__main__":
    print("🟢 Cleanup script started")
    clean_old_readings()
    start_cleanup_scheduler()

    # keep the script alive (in case running standalone)
    while True:
        time.sleep(60)




