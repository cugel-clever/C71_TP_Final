from prometheus_client import start_http_server, Gauge
import time

# Initialiser une métrique gauge
total_films = Gauge('total_films', 'Nombre total de films récupérés')

def collection_metriques():
    # Cette fonction récupère les données et met à jour les métriques
    total_films.set(20)  # Nombre de test à modifier par la suite
if __name__ == '__main__':
    # Démarrer le serveur pour afficher les métriques
    start_http_server(8000)
    print("Le serveur des métriques a été lancé sur le port 8000")
    
    while True:
        collection_metriques()
        time.sleep(5)  # Obtenir les métriques chaque 5 secondes

