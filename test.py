import requests

def get_details_requete_films(param_recherche, cle_api):
    base_url = "https://omdbapi.com/"
    params = {
        **param_recherche,
        'apikey': cle_api
    }
    
    result_films = []

    page = 1
    while True:
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


# Exécution du programme. À terme, mettre infos sensibles dans .env
cle_api = "ede1c94c" 

# On souhaite tous les films de 2003 ayant le libellé "love" dans le titre
param_recherche = {
    "s": "love",
    "y": "2003"
}

def main():
    # De facto, le pipeline

    # Critère compréhension des données effectuée (document word tp-final.docx)
    # Critère Extraction des données
    films_details = get_details_requete_films(param_recherche, cle_api)
    
    print(f"Details of films found with search string by keyword (s) and year (y): {len(films_details)}")


    for film in films_details:
        print(film)


if __name__ == "__main__":
    main()


