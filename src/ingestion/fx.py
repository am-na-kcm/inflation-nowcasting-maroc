import os

import pandas as pd
import requests
from dotenv import load_dotenv

from src.ingestion.common import get_logger, landing_path

load_dotenv()
log = get_logger("fx")

API_KEY = os.environ.get("BKAM_API_KEY")
# Vérifiez l'URL exacte de l'opération dans la documentation du portail BKAM
URL = "https://api.centralbankofmorocco.ma/cours/Version1/api/CoursBBE"


def run():
    headers = {"Ocp-Apim-Subscription-Key": API_KEY}
    log.info("Récupération des cours de change depuis Bank Al-Maghrib...")

    # Appel de l'API
    response = requests.get(URL, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()

    # Transformation des données en tableau (adapter selon le JSON exact renvoyé par l'API)
    # L'objectif est d'avoir au minimum les colonnes : date, eur, usd
    df = pd.DataFrame(payload)

    # Sauvegarde dans landing au format attendu par Auto Loader
    dest = landing_path("fx", "fx_rates.csv")
    df.to_csv(dest, index=False)
    log.info("Fichier sauvegardé dans : %s (%s lignes)", dest, len(df))


if __name__ == "__main__":
    run()
