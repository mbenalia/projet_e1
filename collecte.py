import re
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()


def get_db_connection():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")

    # Créer une connexion SQLAlchemy
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    return engine

def get_nombre_offres():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    url='https://candidat.francetravail.fr/offres/recherche?motsCles=data&offresPartenaires=true&rayon=10&tri=0'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        h1_element = soup.find('h1', class_='title')
        # Extraire le texte de la balise <h1>
        h1_text = h1_element.get_text()
        # Utiliser une expression régulière pour extraire le nombre
        nombre_offres = re.search(r'\d+', h1_text).group()
        # Retourner le nombre d'offres
        print (nombre_offres)
        return int(nombre_offres)
    else:
        print("Erreur lors de la récupération de la page : HTTP", response.status_code)
        return None

def build_url(limite_basse, limite_haute,recherche='data',partenaires="true",):


    base_url = "https://candidat.francetravail.fr/offres/recherche"
    params = {
      "motsCles": recherche,
      "offresPartenaires": partenaires,
      "range": f"{limite_basse}-{limite_haute}",
      "rayon": 10,
      "tri": 0
    }
    url = base_url + "?" + "&".join(f"{key}={value}" for key, value in params.items())
    return url
def fetch_job_offers(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error fetching job offers: HTTP {response.status_code}")
        return None

def parse_job_details(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    job_sections = soup.find_all('h2', class_='t4 media-heading')
    job_data = []
    for job_section in job_sections:
        job_details = extract_job_information(job_section)
        if job_details:
            job_data.append(job_details)
    return job_data

def extract_job_information(job_section):
    id_offre = job_section.get('data-intitule-offre', 'ID non disponible')
    title = job_section.find('span', class_='media-heading-title').get_text(strip=True)
    subtext = job_section.find_next_sibling('p', class_='subtext')
    company = subtext.get_text(strip=True) if subtext else 'Information non disponible'
    location = subtext.find('span').get_text(strip=True) if subtext and subtext.find('span') else 'Lieu non disponible'
    description = job_section.find_next_sibling('p', class_='description').get_text(strip=True)
    contract = job_section.find_next_sibling('p', class_='contrat').get_text(strip=True)
    date_posted = job_section.find_next_sibling('p', class_='date').get_text(strip=True)
    siren='empty'
    return {
        'id_offre': id_offre,
          'poste': title,
          'entreprise': company,
          'lieu': location,
          'description': description,
          'contrat': contract,
          'date_publication': date_posted,
          'siren':siren
      }
def get_offres(limite_basse, limite_haute, job_list):
    url = build_url(limite_basse, limite_haute)
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    html_content = fetch_job_offers(url, headers)
    if html_content:
        job_data = parse_job_details(html_content)
        job_list.extend(job_data)
    return job_list# Use extend to add elements to an existing list    return job_list
#liste=[]
#jobs=get_offres(0,20,liste)
#df=pd.DataFrame(jobs)
def scrap_FT():
    job_list=[]
    nb_max=get_nombre_offres()
    nb_max=41
    for i in range(0,nb_max,20):
        delai_attente = random.randint(1, 5)
        #time.sleep(delai_attente)
        get_offres(i, min(i+20, nb_max), job_list)

    if nb_max % 20 != 0:
        get_offres(nb_max-nb_max%20,nb_max,job_list)

    return pd.DataFrame(job_list)

df=scrap_FT()
def nettoyer_entreprise(df):
    # Utilise une expression régulière pour supprimer les caractères précédents et le tiret avant le nombre
    clean_df = df.str.split('\n').str[0]
    return clean_df

def nettoyer_contrat(df):
    # Divise la colonne "Entreprise" en fonction du caractère "\n" et ne conserve que la première partie
    clean_df = df.str.replace('\n','')
    return clean_df

def convert_to_date(duree):
    if "aujourd'hui" in duree:
        return datetime.now().strftime("%Y-%m-%d")
    elif "hier" in duree:
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif "il y a plus de" in duree:
        return (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    elif "il y a" in duree:
        days_ago = int(duree.split()[4])
        return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

def nettoyage_total(df):
    df['entreprise'] = nettoyer_entreprise(df['entreprise'])
    df['contrat'] = nettoyer_contrat(df['contrat'])
    df["date_publication"] = df["date_publication"].apply(convert_to_date)
    return df
print(nettoyage_total(df))

def export_csv(dataframe, index=True):
    date_str = datetime.today().strftime("%Y-%m-%d-%H_%M")
    nom_fichier=f"france_travail_{date_str}.csv"
    dataframe.to_csv(nom_fichier, index=index)
    return f"exported to {nom_fichier}"

# Exemple d'utilisation
export_csv(df)

def conversion_df_bdd(df, connection=None):
    # Utiliser SQLAlchemy pour gérer les connexions et l'insertion

        # Utiliser la connexion fournie
    engine = get_db_connection()

    # Vérifier si la table 'entreprises' existe
    with engine.connect() as conn:
        result = conn.execute(text("SHOW TABLES LIKE 'entreprises';"))
        table_entreprises_existe = result.fetchone() is not None

    if not table_entreprises_existe:
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE entreprises (
                siren VARCHAR(50) PRIMARY KEY,
                nom_complet VARCHAR(255),
                categorie_entreprise VARCHAR(255)
            )
            """))
            print("Table 'entreprises' créée.")

    # Vérifier si la table 'offres' existe
    with engine.connect() as conn:
        result = conn.execute(text("SHOW TABLES LIKE 'offres';"))
        table_existe = result.fetchone() is not None

    # Créer la table si elle n'existe pas
    if not table_existe:
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE offres (
                id_offre VARCHAR(255) PRIMARY KEY,
                poste VARCHAR(255) NOT NULL,
                entreprise VARCHAR(255) NOT NULL,
                lieu TEXT,
                description TEXT,
                contrat VARCHAR(50),
                date_publication DATE,
                siren VARCHAR(50)
            )
            """))
            print("Table 'offres' créée.")

    try:
        # Utilisation de pandas pour insérer directement les données avec to_sql
        data_to_insert = df[['id_offre', 'poste', 'entreprise', 'lieu', 'description', 'contrat', 'date_publication','siren']]

        # Insertion des données via pandas avec SQLAlchemy
        data_to_insert.to_sql('offres', con=engine, if_exists='replace', index=False)
        print("Données insérées avec succès.")

    except Exception as e:
        print(f"Une erreur est survenue lors de l'insertion des données : {e}")
    engine.dispose()

# Exemple d'utilisation avec un dataframe `df`
conversion_df_bdd(df)

def fetch_company_details(company_name):
    url = f"https://recherche-entreprises.api.gouv.fr/search?q={company_name}&categorie_entreprise=PME%2CETI&est_entrepreneur_individuel=false&est_entrepreneur_spectacle=false&limite_matching_etablissements=1&minimal=true&include=siege&page=1&per_page=1"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            company_info = data['results'][0]  # Prenons le premier résultat
            siren = company_info.get('siren')
            nom_complet = company_info.get('nom_complet')
            categorie_entreprise = company_info.get('categorie_entreprise')
            return siren, nom_complet, categorie_entreprise
        else:
            return 0000000, 'NO_INFO', 'NO_INFO'
    else:
        print(f"Erreur API: {response.status_code}")
        return None, None, None


def update_company_in_db(company_name, engine):
    # Récupérer les détails de l'entreprise
    siren, nom_complet, categorie_entreprise = fetch_company_details(company_name)
    print(f"SIREN: {siren}, type: {type(siren)}")
    print(f"Nom complet: {nom_complet}, type: {type(nom_complet)}")
    print(f"Catégorie d'entreprise: {categorie_entreprise}, type: {type(categorie_entreprise)}")


    # Concaténation si l'adresse est un tuple
    # Vérifier que toutes les informations nécessaires sont présentes
    if siren and nom_complet and categorie_entreprise:
        try:
            with engine.connect() as conn:
                # Utilisation de la requête SQL brute avec des paramètres
                insert_query = text("""
                    INSERT INTO entreprises (siren, nom_complet, categorie_entreprise)
                    VALUES (:siren, :nom_complet, :categorie_entreprise)
                    ON DUPLICATE KEY UPDATE
                        nom_complet = VALUES(nom_complet),
                        categorie_entreprise = VALUES(categorie_entreprise)

                """)

                conn.execute(insert_query, {
                    'siren': siren,
                    'nom_complet': nom_complet,
                    'categorie_entreprise': categorie_entreprise,

                })
                update_offres_query = text("""
                    UPDATE offres
                    SET siren = :siren
                    WHERE entreprise = :company_name
                """)

                conn.execute(update_offres_query, {
                    'siren': siren,
                    'company_name': company_name
                })
                conn.commit()  # Valider la transaction explicitement

            print(f"Mise à jour réussie pour l'entreprise {company_name}")
        except Exception as e:
            print(f"Erreur lors de la mise à jour de l'entreprise {company_name}: {e}")
    else:
        print(f"Informations manquantes pour l'entreprise {company_name}")


def batch_update_companies(df):
    entreprises = df['entreprise'].unique()  # Récupérer toutes les entreprises uniques du DataFrame
    engine = get_db_connection()  # Connexion ouverte une seule fois

    try:
        for i, entreprise in enumerate(entreprises):
            try:
                # Appeler la fonction d'update pour chaque entreprise
                update_company_in_db(entreprise, engine)
            except Exception as e:
                print(f"Erreur lors de la mise à jour de l'entreprise '{entreprise}': {e}")

            # Pause après chaque 6 requêtes pour limiter la fréquence
            if (i + 1) % 6 == 0:
                print("Pause pour respecter la limite de 6 requêtes par seconde...")
                time.sleep(1)
    finally:
        engine.dispose()  # Fermer proprement la connexion au moteur
        print('Mise à jour terminée.')

        #engine.dispose()  # Utiliser dispose pour fermer le pool de connexions
# Exemple d'appel de la fonction
batch_update_companies(df)

def obtenir_offres_avec_entreprises(nom_fichier_csv="offres_avec_entreprises.csv"):
    # Connexion à la base de données
    engine = get_db_connection()

    # Requête pour faire un INNER JOIN entre 'offres' et 'entreprises' sur 'siren'
    query = """
    SELECT offres.id_offre, offres.poste, offres.entreprise, offres.lieu, offres.description,
           offres.contrat, offres.date_publication, offres.siren,
           entreprises.nom_complet, entreprises.categorie_entreprise
    FROM offres
    LEFT JOIN entreprises ON offres.siren = entreprises.siren
    """

    # Exécuter la requête et charger les résultats dans un DataFrame
    with engine.connect() as conn:
        result = conn.execute(text(query))
        offres_avec_entreprises = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Fermer la connexion
    engine.dispose()
    offres_avec_entreprises.to_csv(nom_fichier_csv, index=False, encoding="utf-8")
    print(f"Fichier CSV '{nom_fichier_csv}' créé avec succès.")

    return offres_avec_entreprises

obtenir_offres_avec_entreprises()
