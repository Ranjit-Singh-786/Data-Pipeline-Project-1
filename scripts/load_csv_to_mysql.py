import csv, os
from pathlib import Path
import mysql.connector
from dotenv import load_dotenv
load_dotenv()
csv_path = Path(os.getenv("CSV_PATH", "insurance.csv"))
db = mysql.connector.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")), database=os.getenv("MYSQL_DATABASE", "insurance"), user=os.getenv("MYSQL_USER", "insurance_user"), password=os.getenv("MYSQL_PASSWORD", "insurance_password"))
cur = db.cursor()
cur.execute(Path("sql/schema.sql").read_text())
with csv_path.open(newline="", encoding="utf-8") as source:
    rows = list(csv.reader(source)); cur.executemany("INSERT INTO insurance (age, sex, bmi, children, smoker, region, charges) VALUES (%s,%s,%s,%s,%s,%s,%s)", rows[1:])
db.commit(); print(f"Loaded {len(rows)-1} rows into insurance.insurance")
cur.close(); db.close()
