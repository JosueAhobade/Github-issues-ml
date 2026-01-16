import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_API_KEY,
    sasl_plain_password=KAFKA_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v4+json"
}

GRAPHQL_URL = "https://api.github.com/graphql"

# -----------------------------
# 1️⃣ Users
# -----------------------------
def fetch_users(after_cursor=None, first=100):
    query = """
    query ($first:Int, $after:String) {
      search(query: "type:user repos:>5 followers:>10", type: USER, first:$first, after:$after) {
        userCount
        pageInfo { hasNextPage endCursor }
        nodes { ... on User { login id } }
      }
    }
    """
    variables = {"first": first, "after": after_cursor}
    resp = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables})
    if resp.status_code == 200:
        return resp.json()["data"]["search"]
    else:
        print("Erreur fetch_users:", resp.text)
        return None

# -----------------------------
# 2️⃣ Repos
# -----------------------------
def fetch_repos(user_login, after_cursor=None, first=10):
    query = """
    query($login:String!, $first:Int, $after:String) {
      user(login: $login) {
        repositories(first: $first, after: $after, orderBy:{field:UPDATED_AT, direction:DESC}) {
          pageInfo { hasNextPage endCursor }
          nodes { name stargazerCount updatedAt }
        }
      }
    }
    """
    variables = {"login": user_login, "first": first, "after": after_cursor}
    resp = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables})
    if resp.status_code == 200:
        return resp.json()["data"]["user"]["repositories"]
    else:
        print("Erreur fetch_repos:", resp.text)
        return None

# -----------------------------
# 3️⃣ Issues
# -----------------------------
def fetch_issues(user, repo, after_cursor=None, first=20):
    query = """
    query($owner: String!, $name: String!, $after: String, $first: Int!) {
      repository(owner: $owner, name: $name) {
        issues(first: $first, after: $after, states: [OPEN, CLOSED]) {
          edges {
            cursor
            node {
              id
              number
              title
              body
              labels(first:10){ nodes { name } }
              state
              createdAt
              updatedAt
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    """
    variables = {"owner": user, "name": repo, "after": after_cursor, "first": first}
    resp = requests.post(
        "https://api.github.com/graphql",
        headers={"Authorization": f"bearer {GITHUB_TOKEN}"},
        json={"query": query, "variables": variables}
    )

    json_data = resp.json()
    
    # ⚠️ Gérer si repository est None
    if json_data.get("data") is None or json_data["data"].get("repository") is None:
        print(f"⚠️ Repository {user}/{repo} introuvable ou accès refusé")
        if "errors" in json_data:
            print("Erreurs:", json_data["errors"])
        return {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}

    return json_data["data"]["repository"]["issues"]

# -----------------------------
# Producer principal
# -----------------------------
def produce_graphql():
    user_cursor = None
    while True:
        search_users = fetch_users(after_cursor=user_cursor, first=50)
        if not search_users:
            time.sleep(60)
            continue

        for user in search_users["nodes"]:
            login = user["login"]
            print(f"➡️ Collecte repos pour user: {login}")

            repo_cursor = None
            while True:
                repos = fetch_repos(login, after_cursor=repo_cursor, first=10)
                if not repos or "nodes" not in repos:
                    break

                for repo in repos["nodes"]:
                    repo_name = repo["name"]
                    print(f"    📦 Repos: {repo_name}")

                    # -----------------------------
                    # Issues
                    # -----------------------------
                    issue_cursor = None
                    while True:
                        issues = fetch_issues(login, repo_name, after_cursor=issue_cursor, first=20)
                        if not issues or "edges" not in issues:
                            break

                        for edge in issues["edges"]:
                            issue = edge["node"]
                            data = {
                                "id": issue["id"],
                                "number": issue["number"],
                                "title": issue["title"],
                                "body": issue.get("body", ""),
                                "labels": [label["name"] for label in issue.get("labels", {}).get("nodes", [])],
                                "state": issue["state"],
                                "created_at": issue["createdAt"],
                                "updated_at": issue["updatedAt"],
                                "repo": f"{login}/{repo_name}",
                                "ingested_at": datetime.now(timezone.utc).isoformat()
                            }
                            producer.send(KAFKA_TOPIC, key=str(issue["id"]).encode("utf-8"), value=data)
                            print(f"        ✅ Issue envoyée: {data['title'][:30]}...")

                        # Pagination des issues
                        if not issues["pageInfo"]["hasNextPage"]:
                            break
                        issue_cursor = issues["pageInfo"]["endCursor"]

                # Pagination des repos
                if not repos["pageInfo"]["hasNextPage"]:
                    break
                repo_cursor = repos["pageInfo"]["endCursor"]

        # Pagination des users
        if not search_users["pageInfo"]["hasNextPage"]:
            break
        user_cursor = search_users["pageInfo"]["endCursor"]

    user_cursor = None
    while True:  # boucle continue
        search_users = fetch_users(after_cursor=user_cursor, first=50)
        if not search_users:
            time.sleep(60)
            continue
        
        for user in search_users["nodes"]:
            login = user["login"]
            print(f"➡️ Collecte repos pour user: {login}")
            
            repo_cursor = None
            while True:
                repos = fetch_repos(login, after_cursor=repo_cursor, first=10)
                if not repos or "nodes" not in repos:
                    break

                for repo in repos["nodes"]:
                    repo_name = repo["name"]
                    print(f"    📦 Repos: {repo_name}")

                    issue_cursor = None
                    while True:
                        issues = fetch_issues(login, repo_name, after_cursor=issue_cursor, first=20)
                        if not issues or "edges" not in issues:
                            break

                        for edge in issues["edges"]:
                            issue = edge["node"]
                            data = {
                                "id": issue["id"],
                                "number": issue["number"],
                                "title": issue["title"],
                                "body": issue.get("body", ""),
                                "labels": [label["name"] for label in issue.get("labels", {}).get("nodes", [])],
                                "state": issue["state"],
                                "created_at": issue["createdAt"],
                                "updated_at": issue["updatedAt"],
                                "repo": f"{login}/{repo_name}",
                                "ingested_at": datetime.now(timezone.utc).isoformat()
                            }
                            producer.send(KAFKA_TOPIC, key=str(issue["id"]).encode("utf-8"), value=data)
                            print(f"        ✅ Issue envoyée: {data['title'][:30]}...")

                        # pagination
                        if not issues["pageInfo"]["hasNextPage"]:
                            break
                        issue_cursor = issues["pageInfo"]["endCursor"]

                if not repos["pageInfo"]["hasNextPage"]:
                    break
                repo_cursor = repos["pageInfo"]["endCursor"]

                login = user["login"]
                print(f"➡️ Collecte repos pour user: {login}")
                repo_cursor = None
                while True:
                    repos = fetch_repos(login, after_cursor=repo_cursor, first=10)
                    if not repos:
                        break
                    if not issues or "edges" not in issues:
                        break
                    for edge in issues["edges"]:
                        issue = edge["node"]  # c’est ici que se trouve l’issue
                        data = {
                            "id": issue["id"],
                            "number": issue["number"],
                            "title": issue["title"],
                            "body": issue.get("body", ""),
                            "labels": [label["name"] for label in issue.get("labels", {}).get("nodes", [])],
                            "state": issue["state"],
                            "created_at": issue["createdAt"],
                            "updated_at": issue["updatedAt"],
                            "repo": f"{login}/{repo_name}",
                            "ingested_at": datetime.now(timezone.utc).isoformat()
                        }
                        producer.send(KAFKA_TOPIC, key=str(issue["id"]).encode("utf-8"), value=data)
                        print(f"        ✅ Issue envoyée: {data['title'][:30]}...")

                        repo_name = repo["name"]
                        print(f"    📦 Repos: {repo_name}")
                        # issues
                        issue_cursor = None
                        while True:
                            issues = fetch_issues(login, repo_name, after_cursor=issue_cursor, first=20)
                            if not issues:
                                break
                            for issue in issues["nodes"]:
                                data = {
                                    "id": issue["id"],
                                    "number": issue["number"],
                                    "title": issue["title"],
                                    "body": issue.get("body", ""),
                                    "labels": [label["name"] for label in issue.get("labels", {}).get("nodes", [])],
                                    "state": issue["state"],
                                    "created_at": issue["createdAt"],
                                    "updated_at": issue["updatedAt"],
                                    "repo": f"{login}/{repo_name}",
                                    "ingested_at": datetime.now(timezone.utc).isoformat()
                                }
                                producer.send(KAFKA_TOPIC, key=str(issue["id"]).encode("utf-8"), value=data)
                                print(f"        ✅ Issue envoyée: {data['title'][:30]}...")
                            if not issues["pageInfo"]["hasNextPage"]:
                                break
                            issue_cursor = issues["pageInfo"]["endCursor"]
                    if not repos["pageInfo"]["hasNextPage"]:
                        break
                    repo_cursor = repos["pageInfo"]["endCursor"]
                if not search_users["pageInfo"]["hasNextPage"]:
                    break
                user_cursor = search_users["pageInfo"]["endCursor"]

if __name__ == "__main__":
    produce_graphql()
