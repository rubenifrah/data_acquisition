import os
import spotipy
import webbrowser
import requests  # Importation importante
from dotenv import load_dotenv

def test_spotify_id_with_raw_request():
    """
    Teste l'accès à l'API en utilisant l'authentification manuelle (OAuth)
    MAIS en appelant l'endpoint avec `requests` (brut) au lieu de `spotipy`.
    """
    
    # --- 1. Load Credentials ---
    load_dotenv()
    
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    REDIRECT_URI = 'http://127.0.0.1:8080' # Doit correspondre au Dashboard

    if not client_id or not client_secret:
        print("ERROR: SPOTIPY_CLIENT_ID or SPOTIPY_CLIENT_SECRET not found.")
        return

    # --- 2. Authentification (comme avant) ---
    # Nous demandons les mêmes scopes que votre ancien code
    oauth = spotipy.SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=REDIRECT_URI,
        scope='user-library-read playlist-read-private' # Scopes de votre ancien code
    )

    try:
        # Supprimer l'ancien cache pour forcer une nouvelle connexion
        if os.path.exists('.cache'):
            os.remove('.cache')
            print("Ancien cache de token supprimé.")

        # --- Processus de connexion manuel ---
        auth_url = oauth.get_authorize_url()
        print("\n--- ACTION REQUISE ---")
        print("Veuillez ouvrir ce lien dans votre navigateur pour vous connecter :")
        print(auth_url)
        webbrowser.open(auth_url)
        
        print("\nAprès vous être connecté (Spotify va demander des permissions), vous serez redirigé.")
        print("Copiez l'adresse URL complète de cette page (elle commence par 'http://127.0.0.1:8080').")
        
        response_url = input("\nCollez l'URL de redirection (http://127.0.0.1:8080...) ici et appuyez sur Entrée :\n")

        if "http://127.0.0.1:8080" not in response_url:
            print("Erreur: L'URL collée doit commencer par 'http://127.0.0.1:8080'")
            return

        code = oauth.parse_response_code(response_url)
        token_info = oauth.get_access_token(code, as_dict=True)

        # Nous avons le token !
        access_token = token_info['access_token']
        print("\n--- Authentification Réussie (Mode Utilisateur) ---")
        print(f"Access Token: {access_token[:20]}...") # Affiche le début du token

        # --- 3. Test avec `requests` (comme votre ancien code) ---
        track_id = '3Dy4REq8O09IlgiwuHQ3sk' # ABBA - Waterloo
        
        print(f"\n--- Test: Appel `requests.get()` pour les audio features (ID: {track_id}) ---")

        # Construction de l'appel manuel
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        url = f'https://api.spotify.com/v1/audio-features/{track_id}'

        response = requests.get(url, headers=headers)

        # --- 4. Analyse du Résultat ---
        if response.status_code == 200:
            print(f"Code de statut : {response.status_code} (OK)")
            print("\n--- TEST RÉUSSI ! ---")
            print("Les données ont été récupérées avec succès en utilisant `requests` brut.")
            print("\nDonnées reçues :")
            import pprint
            pprint.pprint(response.json())
        else:
            print(f"\n--- !! TEST FAILED !! ---")
            print(f"Code de statut : {response.status_code}")
            print("Réponse de l'API :")
            print(response.json())


    except Exception as e:
        print(f"\nUne erreur inattendue est survenue: {e}")

if __name__ == "__main__":
    test_spotify_id_with_raw_request()