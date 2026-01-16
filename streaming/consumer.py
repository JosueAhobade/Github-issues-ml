import json
import os
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import OperationalError
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

# -----------------------------
# Kafka
# -----------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("KAFKA_API_KEY"),
    sasl_plain_password=os.getenv("KAFKA_API_SECRET"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mysql-consumer-replay-v2"
)


# -----------------------------
# MySQL
# -----------------------------
def connect_mysql():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        autocommit=True
    )

db = connect_mysql()
cursor = db.cursor()

# -----------------------------
# Query
# -----------------------------
INSERT_QUERY = """
INSERT INTO issues (
    issue_id, repo, number, title, body, state, labels,
    created_at, updated_at, ingested_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    ingested_at=VALUES(ingested_at);
"""

# -----------------------------
# Safe insert avec reconnexion automatique
# -----------------------------
def safe_insert(issue):
    global cursor  # <- une seule déclaration globale au début
    while True:
        try:
            cursor.execute(
                INSERT_QUERY,
                (
                    issue["id"],
                    issue["repo"],
                    issue["number"],
                    issue["title"],
                    issue.get("body", ""),
                    issue["state"],
                    json.dumps(issue.get("labels", [])),
                    issue["created_at"].replace("T", " ").replace("Z", ""),
                    issue["updated_at"].replace("T", " ").replace("Z", ""),
                    datetime.now(timezone.utc)
                )
            )
            db.commit()
            break  # insertion réussie
        except OperationalError as e:
            print(f"⚠️ Lost MySQL connection, reconnecting... {e}")
            db.reconnect(attempts=5, delay=5)
            # cursor = db.cursor()  # réinitialiser le curseur après reconnexion
        except mysql.connector.Error as e:
            print(f"❌ MySQL error: {e}, issue: {issue['id']}")
            break

# -----------------------------
# Boucle principale
# -----------------------------
for msg in consumer:
    issue = msg.value
    safe_insert(issue)
    print(f"✅ Issue {issue['id']} insérée / mise à jour")
