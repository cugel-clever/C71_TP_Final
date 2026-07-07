import requests
import os
from dotenv import load_dotenv
from pymongo import MongoClient

#  Imports internes
from config import settings

# Charge les variables depuis le fichier .env situé à la racine du projet
# Si une variable est déjà dans l'environnement système, elle n'est pas écrasée
load_dotenv()

def get_details_requete_films(param_recherche, cle_api):
    base_url = "https://omdbapi.com/"
    params = {
        **param_recherche,
        'apikey': cle_api
    }
    
    result_films = []

    page = 1
    while True:
        # Pour le développement limiter à ~20 occurrences
        if len(result_films) > 20:
            break

        # Mettre à jour les paramètres avec la page courante
        params['page'] = page

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status() # Soulever une exception erreurs HTTP

            response_json = response.json()

            # Vérifier si la recherche donne des résultats valides
            if response_json.get("response") == "False":
                print(f"Erreur: {response.get('Error')}")
                break
        except requests.exceptions.RequestException as e:
            print(f"La requête a échoué: {e}")
            break
        
        # Pour chaque film, aller chercher les informations
        for film in response_json.get("Search", []):
            imdb_id = film.get("imdbID")
            
            try:
                # Aller chercher les détails
                detail_url = f"http://omdbapi.com/?i={imdb_id}&apikey={cle_api}"
                film_details = requests.get(detail_url)
                film_details.raise_for_status() # Soulever une exception erreurs HTTP

                film_details_json = film_details.json()
                result_films.append(film_details_json)
            except requests.exceptions.RequestException as e:
                print(f"La requête pour les détails du film ID # {imdb_id} a échoué: {e}")
                # Pas besoin de break ici

            # Si aucun résultat, sortir de la boucle
            if not response_json.get("Search"):
                break
            
            # Incrémenter la page pour la prochaine série d'informations
            page += 1

    return result_films

def films_mongodb(movies_details):
    # Connexion à MongoDB
    # client = MongoClient(os.getenv("MONGODB_URI"))
    client = MongoClient(settings.MONGODB_URI)
    
    # Sélection de la base de données et de la collection
    db = client['c71_tp_final']
    collection = db['films_bronze']

    # Sauvegarde des films dans la collection
    for movie in movies_details:
        collection.insert_one(movie)

    # Fermeture de la connexion à MongoDB
    client.close()

API_KEY = settings.OMDB_API_KEY

def main():
    # De facto, le pipeline

    # Critère Compréhension des données effectuée (document word tp-final.docx)
    # Critère Extraction des données

    # On souhaite tous les films de 2003 ayant le libellé "love" dans le titre
    param_recherche = {
        "s": "love",
        "y": "2003"
    }

    films_details = get_details_requete_films(param_recherche, API_KEY)
    
    print(f"Details of films found with search string by keyword (s) and year (y): {len(films_details)}")

    # Sauvegarde des films dans MongoDb (Couche Bronze)
    films_mongodb(films_details)
    
    for film in films_details:
        print(film)


if __name__ == "__main__":
    main()


