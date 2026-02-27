import mysql.connector
from datetime import datetime, timedelta
import time
from threading import Thread
import os
import sys
import pandas as pd
import pytz
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
import base64



# ========== TEST FILTER ==========
REPORT_ONLY = True
TEST_USER_ID = None   # <-- sirf testing ke liye


# =============== TIMEZONE =============
IST = pytz.timezone("Asia/Kolkata")

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
    Deletes all readings older than 10 days from device_reading_log.
    """
    print("🧹 Starting cleanup process...")
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        
        cutoff_date = datetime.now(IST) - timedelta(days=10)
        print("📅 Deleting records older than:", cutoff_date.strftime("%Y-%m-%d %H:%M:%S"))

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
            print(f"✅ Deleted {count} old readings (older than 10 days)")
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
            # clean_old_readings()
            print("⏳ Next cleanup in 24 hours...")
            time.sleep(24 * 60 * 60)

    t = Thread(target=scheduler, daemon=True)
    t.start()
    print("🚀 Cleanup scheduler started (every 24 hours)")


  #==================Log insert / Update ===============
def log_email_report(
    user_id,
    record_selection_date,
    sent_status,
    sent_dt=None,
    sent_tm=None
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO email_report_log
        (USER_ID, RECORD_SELECTION_DATE, EMAIL_SENT_STATUS,
         EMAIL_SENT_DATE, EMAIL_SENT_TIME)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            EMAIL_SENT_STATUS = VALUES(EMAIL_SENT_STATUS),
            EMAIL_SENT_DATE = VALUES(EMAIL_SENT_DATE),
            EMAIL_SENT_TIME = VALUES(EMAIL_SENT_TIME)
    """, (
        user_id,
        record_selection_date,
        sent_status,
        sent_dt,
        sent_tm
    ))

    conn.commit()
    cursor.close()
    conn.close()



  # ================== fetching Users ================== 
def get_active_users():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT USER_ID, ACTUAL_NAME, EMAIL
        FROM master_user
        WHERE SEND_EMAIL = 1
          AND EMAIL IS NOT NULL
    """

    params = []

    if TEST_USER_ID is not None:
        query += " AND USER_ID = %s"
        params.append(TEST_USER_ID)

    cursor.execute(query, params)

    users = cursor.fetchall()
    conn.close()
    return users

 # ================== User Org & Centre Link ================== 
def get_user_org_centres(user_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT ORGANIZATION_ID_id AS organization_id,
               CENTRE_ID_id AS centre_id
        FROM userorganizationcentrelink
        WHERE USER_ID_id = %s
    """, (user_id,))

    rows = cursor.fetchall()
    conn.close()
    return rows

 # ================== User Link Devices ================== 
def get_devices_for_user(user_id):
    org_centres = get_user_org_centres(user_id)

    if not org_centres:
        return []

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    devices = set()

    for oc in org_centres:
        cursor.execute("""
            SELECT DISTINCT DEVICE_ID
            FROM device_reading_log
            WHERE ORGANIZATION_ID = %s
              AND CENTRE_ID = %s
        """, (oc["organization_id"], oc["centre_id"]))

        for row in cursor.fetchall():
            devices.add(row["DEVICE_ID"])

    conn.close()
    return list(devices)

 # ================== Excel Generation (24h) ================== 

def generate_user_excel(user):
    devices = get_devices_for_user(user["USER_ID"])

    if not devices:
        return None

    conn = get_connection()
    cursor = conn.cursor()

    # ✅ Python based exact 24 hours
    now = datetime.now(IST)
    start_time = now - timedelta(hours=24)

    # 🔥 STEP 1 — Check if ANY device has data in last 24h
    format_strings = ','.join(['%s'] * len(devices))

    check_query = f"""
        SELECT COUNT(*)
        FROM device_reading_log
        WHERE DEVICE_ID IN ({format_strings})
        AND TIMESTAMP(READING_DATE, READING_TIME)
            BETWEEN %s AND %s
    """

    cursor.execute(
        check_query,
        tuple(devices) + (start_time, now)
    )

    total_count = cursor.fetchone()[0]

    if total_count == 0:
        conn.close()
        return None   # ❌ No data in any device

    # 🔥 STEP 2 — Now generate Excel (only active devices)

    filename = (
        f"Reading_Report_{user['ACTUAL_NAME']}_" 
        f"{now.strftime('%Y-%m-%d_%H%M%S')}.xlsx"
    )

    sheet_created = False

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        for device_id in devices:

            query = """
                SELECT
                    d.DEVICE_NAME           AS Device,
                    o.ORGANIZATION_NAME     AS Organization,
                    c.CENTRE_NAME           AS Centre,
                    s.SENSOR_NAME           AS Sensor,
                    p.PARAMETER_NAME        AS Parameter,
                    r.READING               AS Reading,
                    r.READING_DATE          AS Date,
                    r.READING_TIME          AS Time
                FROM device_reading_log r
                JOIN iot_api_masterdevice d
                    ON d.DEVICE_ID = r.DEVICE_ID
                JOIN iot_api_masterorganization o
                    ON o.ORGANIZATION_ID = r.ORGANIZATION_ID
                JOIN iot_api_mastercentre c
                    ON c.CENTRE_ID = r.CENTRE_ID
                JOIN iot_api_mastersensor s
                    ON s.SENSOR_ID = r.SENSOR_ID
                JOIN iot_api_masterparameter p
                    ON p.PARAMETER_ID = r.PARAMETER_ID
                WHERE r.DEVICE_ID = %s
                AND TIMESTAMP(r.READING_DATE, r.READING_TIME)
                    BETWEEN %s AND %s
                ORDER BY r.READING_DATE, r.READING_TIME;
            """

            df = pd.read_sql(
                query,
                conn,
                params=(device_id, start_time, now)
            )

            if df.empty:
                continue

            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%d-%m-%Y")
            df["Time"] = pd.to_timedelta(df["Time"]).astype(str).str.split().str[-1]

            df.to_excel(
                writer,
                sheet_name=str(df["Device"].iloc[0])[:31],
                index=False
            )

            sheet_created = True

    conn.close()

    if not sheet_created:
        if os.path.exists(filename):
            os.remove(filename)
        return None

    return filename



 # ================== BREVO Mail Sender ================== 
def send_email_brevo(to_email, username, excel_file):

    print(f"📧 Sending email to {to_email}")

    # 🔎 DEBUG CHECK (temporary)
    print("MAIL_FROM:", os.getenv("MAIL_FROM"))
    print("BREVO KEY:", os.getenv("BREVO_API_KEY"))

    BREVO_API_KEY = os.getenv("BREVO_API_KEY")

    if not BREVO_API_KEY:
        print("❌ BREVO_API_KEY not found in environment variables!")
        return

    try:
        import sib_api_v3_sdk
        from sib_api_v3_sdk.rest import ApiException
        import base64

        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = BREVO_API_KEY

        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )

        # Excel file ko base64 me convert karo
        with open(excel_file, "rb") as f:
            encoded_file = base64.b64encode(f.read()).decode()

        attachment = [{
            "content": encoded_file,
            "name": excel_file
        }]

        email = sib_api_v3_sdk.SendSmtpEmail(
            to=[{"email": to_email, "name": username}],
            sender={
                "email": os.getenv("MAIL_FROM"),
                "name": "FertiSense IoT"
            },
            subject="📊 Device Reading Report (Last 24 Hours)",
            html_content=f"""
                <p>Hello {username},</p>
                <p>Please find attached your device reading report for the last 24 hours.</p>
                <p>Regards,<br>FertiSense IoT System</p>
            """,
            attachment=attachment
        )

        response = api_instance.send_transac_email(email)
        print("✅ Email sent:", response)

    except ApiException as e:
        print("❌ Brevo API Error:", e.body)
    except Exception as e:
        print("❌ General Email Error:", str(e))



 # ================== Cron  ================== 
def send_reports_to_all_users():
    print("📨 Sending 24h reports to users...")

    users = get_active_users()

    for user in users:
        excel = generate_user_excel(user)

        record_selection_date = (datetime.now(IST) - timedelta(days=1)).date()
        now_dt = datetime.now(IST)

        if excel:
            print("📄 Excel generated at:", excel)

            # 🔔 Yaha email bhejna hoga (jab enable karoge)
            # send_email_brevo(...)

              # 🔥 MULTIPLE EMAIL SUPPORT (YAHI ADD KIYA HAI)
            emails = [e.strip() for e in user["EMAIL"].split(",") if e.strip()]

            for email in emails:
                send_email_brevo(
                    to_email=email,
                    username=user["ACTUAL_NAME"],
                    excel_file=excel
                )

            # ✅ SUCCESS LOG
            log_email_report(
                user_id=user["USER_ID"],
                record_selection_date=record_selection_date,
                sent_status=True,
                sent_dt=now_dt.date(),
                sent_tm=now_dt.time().replace(microsecond=0)
            )

            # os.remove(excel)

        else:
            # ❌ FAIL LOG (no data / excel nahi bana)
            log_email_report(
                user_id=user["USER_ID"],
                record_selection_date=record_selection_date,
                sent_status=False
            )


# ================== MAIN ==================
if __name__ == "__main__":
    print("🟢 Cleanup script started")

    # Run cleanup once (cron-safe)
    send_reports_to_all_users()
    clean_old_readings()
   


    # If explicitly running in standalone/VM mode, enable scheduler
    if os.getenv("ENABLE_INTERNAL_SCHEDULER") == "true":
        start_cleanup_scheduler()

        # keep the script alive only in this mode
        while True:
            time.sleep(60)

    print("🔴 Cleanup finished, exiting process")
    sys.exit(0)


