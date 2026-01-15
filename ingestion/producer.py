import os
import time
import requests
from kafka import KafkaProducer
import json
from dotenv import load_dotenv

# Charger variables d'environnement
load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_issues")

# Repo GitHub à surveiller
OWNER = "facebook"   # change par le repo voulu
REPO = "react"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Headers GitHub API
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def fetch_issues(page=1, per_page=30):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/issues"
    params = {"state": "all", "page": page, "per_page": per_page}
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API: {response.status_code}, {response.text}")
        return []

def produce_issues():
    page = 1
    while True:
        issues = fetch_issues(page)
        if not issues:
            print("Pas de nouvelles issues. Attente 30s...")
            time.sleep(30)
            continue

        for issue in issues:
            # filtrer pull requests
            if "pull_request" in issue:
                continue
            data = {
                "id": issue["id"],
                "number": issue["number"],
                "title": issue["title"],
                "body": issue.get("body", ""),
                "labels": [label["name"] for label in issue.get("labels", [])],
                "state": issue["state"],
                "created_at": issue["created_at"],
                "updated_at": issue["updated_at"],
                "repo": f"{OWNER}/{REPO}"
            }
            producer.send(KAFKA_TOPIC, value=data)
            print(f"Issue envoyée: {data['title']}")
        page += 1
        time.sleep(10)  # pause pour ne pas spammer l'API

if __name__ == "__main__":
    produce_issues()
