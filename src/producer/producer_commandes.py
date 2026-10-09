"""Produit une commande StreamVault toutes les trois secondes.

Chaque commande reference un client et un titre reels stockes dans MongoDB.
Le prix est derive de l'identifiant du titre, ce qui le rend stable entre
plusieurs commandes portant sur le meme titre.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from azure.eventhub import EventData, EventHubProducerClient
from pymongo import MongoClient
from pymongo.database import Database


SEND_INTERVAL_SECONDS = 3


def required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"La variable d'environnement {name} est absente.")
    return value


def deterministic_price(title_id: Any) -> float:
    """Retourne toujours le meme prix, entre 4,99 EUR et 24,99 EUR."""
    digest = hashlib.sha256(str(title_id).encode("utf-8")).digest()
    cents = 499 + (int.from_bytes(digest[:4], "big") % 2001)
    return cents / 100


def sample_one(database: Database[Any], collection: str, projection: dict[str, int]) -> dict[str, Any]:
    documents = list(
        database[collection].aggregate(
            [
                {"$sample": {"size": 1}},
                {"$project": projection},
            ]
        )
    )
    if not documents:
        raise RuntimeError(f"La collection {collection} est vide.")
    return documents[0]


def build_order(database: Database[Any]) -> dict[str, Any]:
    client = sample_one(
        database,
        "clients",
        {
            "_id": 1,
            "nom": 1,
            "prenom": 1,
            "code_postal": 1,
            "commune": 1,
        },
    )
    title = sample_one(
        database,
        "catalogue",
        {
            "_id": 1,
            "title": 1,
            "type": 1,
        },
    )

    price = deterministic_price(title["_id"])
    return {
        "_id": f"commande_{uuid.uuid4().hex}",
        "client_id": str(client["_id"]),
        "client_nom": client.get("nom"),
        "client_prenom": client.get("prenom"),
        "code_postal": client.get("code_postal"),
        "commune": client.get("commune"),
        "catalogue_id": str(title["_id"]),
        "titre": title.get("title"),
        "type_titre": title.get("type"),
        "prix": price,
        "quantite": 1,
        "montant": price,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "type": "commande",
    }


def main() -> None:
    eventhub_connection_string = required_environment_variable(
        "AZURE_EVENTHUB_CONNECTION_STRING"
    )
    eventhub_name = os.getenv("AZURE_EVENTHUB_NAME", "commandes")
    connection_fields = [
        part.partition("=") for part in eventhub_connection_string.split(";")
    ]
    entity_paths = [
        value.strip()
        for key, separator, value in connection_fields
        if separator and key.strip().casefold() == "entitypath"
    ]
    if len(entity_paths) != 1 or entity_paths[0].casefold() != eventhub_name.casefold():
        raise RuntimeError(
            f"La chaine Event Hub doit cibler exactement EntityPath={eventhub_name}."
        )

    mongodb_password = required_environment_variable("MONGODB_PASSWORD")
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017")
    mongodb_database = os.getenv("MONGODB_DATABASE", "streamvault")
    mongodb_username = os.getenv("MONGODB_USERNAME", "streamvault_adf")
    mongodb_auth_source = os.getenv("MONGODB_AUTH_SOURCE", "streamvault")

    mongo_client: MongoClient[Any] = MongoClient(
        mongodb_uri,
        username=mongodb_username,
        password=mongodb_password,
        authSource=mongodb_auth_source,
        serverSelectionTimeoutMS=5_000,
    )
    database = mongo_client[mongodb_database]
    database.command({"ping": 1})

    producer = EventHubProducerClient.from_connection_string(
        conn_str=eventhub_connection_string
    )

    print("Producteur demarre. Appuyez sur Ctrl+C pour l'arreter.")
    try:
        with producer:
            while True:
                order = build_order(database)
                body = json.dumps(order, ensure_ascii=False)
                batch = producer.create_batch()
                batch.add(EventData(body))
                producer.send_batch(batch)
                print(body)
                time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur.")
    finally:
        mongo_client.close()


if __name__ == "__main__":
    main()
