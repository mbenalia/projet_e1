from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os
from typing import List, Optional
from datetime import datetime


load_dotenv()
# Récupérer les informations de connexion à partir du fichier .env
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
API_TOKEN = os.getenv("API_TOKEN")
# Créer l'URL de connexion pour SQLAlchemy
DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
# Initialiser SQLAlchemy
engine = create_engine(DATABASE_URL)
# Créer l'application FastAPI
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

app = FastAPI()
async def get_api_key(api_key: str = Depends(api_key_header)):
    if api_key != f"Bearer {API_TOKEN}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Token"
        )
    return api_key


@app.get("/offres")
async def get_all_offres():
    try:
        with engine.connect() as conn:
            # Exécuter la requête SELECT * FROM offres
            query = text("""SELECT offres.id_offre, offres.poste, offres.entreprise, offres.lieu,
            offres.description, offres.contrat, offres.date_publication, offres.siren,
           entreprises.nom_complet, entreprises.categorie_entreprise
    FROM offres
    LEFT JOIN entreprises ON offres.siren = entreprises.siren """)
            result = conn.execute(query)
            # Convertir le résultat en DataFrame puis en JSON
            df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df_result.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/offres_date")
async def get_all_offres(date_min: Optional[datetime] = None, api_key: str = Depends(get_api_key)):
    try:
        with engine.connect() as conn:
            # Requête de base
            base_query = """
            SELECT offres.id_offre, offres.poste, offres.entreprise, offres.lieu,
                   offres.description, offres.contrat, offres.date_publication, offres.siren,
                   entreprises.nom_complet, entreprises.categorie_entreprise
            FROM offres
            LEFT JOIN entreprises ON offres.siren = entreprises.siren
            """

            # Ajout dynamique du filtre sur la date
            if date_min:
                base_query += " WHERE offres.date_publication >= :date_min"

            # Exécution de la requête
            result = conn.execute(text(base_query), {"date_min": date_min} if date_min else {})

            # Transformation directe des résultats en JSON
            offres = [dict(row) for row in result]
            return offres

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
