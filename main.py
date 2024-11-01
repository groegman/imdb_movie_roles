import requests
import sqlite3
from imdb import IMDb
from concurrent.futures import ThreadPoolExecutor
import csv
from config import movie as config
from imdb import Cinemagoer
import time

API_KEY = config.API_KEY

def get_movies(start_year, end_year, db_path):
    url = "https://imdb8.p.rapidapi.com/v2/search-advance"
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "imdb8.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    
    all_data = []
    
    for year in range(start_year, end_year + 1):
        payload = {
            "first": 1000,
            "after": "",
            "includeReleaseDates": False,
            "sort": {
                "sortBy": "USER_RATING_COUNT",
                "sortOrder": "DESC"
            },
            "releaseDateRange": {
                "end": f"{year}-12-31",
                "start": f"{year}-01-01"
            },
            "ratingsCountRange": {"min": 1000},
            #"anyPrimaryCountries": ["US"],
            "anyPrimaryLanguages": ["en"],
            "anyTitleTypeIds": ["movie"]
        }
        
        while True:
            response = requests.post(url, json=payload, headers=headers)
            data = response.json().get('data', {}).get('advancedTitleSearch', {}).get('edges', [])
            
            if not data:
                break
            
            all_data.extend(data)
            
            end_cursor = response.json().get('data', {}).get('advancedTitleSearch', {}).get('pageInfo', {}).get('endCursor')
            has_next_page = response.json().get('data', {}).get('advancedTitleSearch', {}).get('pageInfo', {}).get('hasNextPage')
            
            if not has_next_page:
                break
            
            payload['after'] = end_cursor
    
    if not all_data:
        print("Keine Daten gefunden.")
    else:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS "movies" (
            	"id"	TEXT,
	            "title"	TEXT,
	            "genre"	TEXT,
	            "year"	INTEGER,
	            "director"	TEXT,
	            "rating"	REAL,
	            "votes"	INTEGER,
	            "runtime"	REAL,
	            PRIMARY KEY("id")
            )
        ''')
        
        for item in all_data:
            movie = item['node']['title']
            c.execute('''
                INSERT OR REPLACE INTO movies (id, title)
                VALUES (?, ?)
            ''', (movie['id'], movie.get('text', '')))
        
        conn.commit()
        conn.close()
        
        print("Daten erfolgreich in SQLite-Datenbank gespeichert.")

def process_movies(db_path):
    # Verbindung zur SQLite-Datenbank herstellen
    conn = sqlite3.connect(db_path, check_same_thread=False)
    cursor = conn.cursor()

    # Tabellen erstellen, falls sie noch nicht existieren
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS actors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS roles (
        role_id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_id INTEGER,
        movie_id TEXT,
        role_name TEXT,
        role_pos INTEGER,
        FOREIGN KEY(actor_id) REFERENCES actors(id),
        FOREIGN KEY(movie_id) REFERENCES movies(id)
    )
    """)

    # Cinemagoer-Instanz erstellen
    ia = Cinemagoer()

    # Nur IDs abrufen, bei denen die Spalte title nicht gefüllt ist
    cursor.execute("""
    SELECT movies.id 
    FROM movies 
    LEFT JOIN roles ON movies.id = roles.movie_id 
    WHERE roles.movie_id IS NULL
    LIMIT 200
    """)
    movie_ids = cursor.fetchall()

    # Zähler initialisieren
    processed_movies = 0
    inserted_actors = 0
    inserted_roles = 0

    # Funktion zum Verarbeiten eines Films
    def process_movie(movie_id):
        nonlocal processed_movies, inserted_actors, inserted_roles
        movie_id = movie_id[0]
        imdb_id = movie_id.replace("tt", "")
        
        try:
            # Filmdaten von IMDb abrufen
            movie = ia.get_movie(imdb_id)
            title = movie.get('title', 'N/A')
            year = movie.get('year', 'N/A')
            genres = ', '.join(movie.get('genres', []))
            director = ', '.join([d['name'] for d in movie.get('directors', [])])
            rating = movie.get('rating', 'N/A')
            votes = movie.get('votes', 'N/A')
            runtime = movie.get('runtime', ['N/A'])[0]
            
            # Daten in die Datenbank eintragen
            cursor.execute("""
                UPDATE movies
                SET title = ?, year = ?, genre = ?, director = ?, rating = ?, votes = ?, runtime = ?
                WHERE id = ?
            """, (title, year, genres, director, rating, votes, runtime, movie_id))
            
            # Schauspieler und Rollen abrufen
            cast = movie.get('cast', [])
            actor_data = []
            role_data = []
            for role_index, actor in enumerate(cast, start=1):
                actor_id = actor.personID
                actor_name = actor['name']
                role_name = str(actor.currentRole)  # Sicherstellen, dass role_name ein String ist
                
                # Schauspieler in die actors-Tabelle einfügen, falls noch nicht vorhanden
                cursor.execute("SELECT id FROM actors WHERE id = ?", (actor_id,))
                if cursor.fetchone() is None:
                    actor_data.append((actor_id, actor_name))
                    inserted_actors += 1
                
                # Rolle in die roles-Tabelle einfügen
                role_data.append((actor_id, movie_id, role_name, role_index))
                inserted_roles += 1
            
            # Batch-Inserts durchführen
            if actor_data:
                cursor.executemany("INSERT INTO actors (id, name) VALUES (?, ?)", actor_data)
            if role_data:
                cursor.executemany("INSERT INTO roles (actor_id, movie_id, role_name, role_pos) VALUES (?, ?, ?, ?)", role_data)
            
            processed_movies += 1
        
        except Exception as e:
            print(f"Fehler bei {movie_id}: {e}")

    # Zeittracking starten
    start_time = time.time()

    # Multithreading verwenden, um Filme parallel zu verarbeiten
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_movie, movie_ids)

    # Zeittracking beenden
    end_time = time.time()
    elapsed_time = (end_time - start_time)/60

    print(f"Die Laufzeit der Funktion beträgt {elapsed_time:.2f} Minuten")
    print(f"Verarbeitete Filme: {processed_movies}")
    print(f"Eingefügte Schauspieler: {inserted_actors}")
    print(f"Eingefügte Rollen: {inserted_roles}")

    # Änderungen speichern und Verbindung schließen
    conn.commit()
    conn.close()

def export_join_to_csv(db_path, csv_path):
    # Verbindung zur SQLite-Datenbank herstellen
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # SQL-Abfrage für den Join
    query = """
    SELECT 
        roles.role_id, 
        actors.name AS actor_name, 
        movies.title AS movie_title, 
        movies.genre, 
        movies.year, 
        movies.director, 
        movies.rating, 
        movies.votes, 
        movies.runtime, 
        roles.role_name, 
        roles.role_pos
    FROM roles
    JOIN actors ON roles.actor_id = actors.id
    JOIN movies ON roles.movie_id = movies.id
    """

    # Abfrage ausführen und Ergebnisse abrufen
    cursor.execute(query)
    rows = cursor.fetchall()

    # Spaltennamen abrufen
    column_names = [description[0] for description in cursor.description]

    # CSV-Datei schreiben
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(column_names)  # Spaltennamen schreiben
        csvwriter.writerows(rows)  # Daten schreiben

    # Verbindung schließen
    conn.close()



# Aufruf der Funktion
#get_movies(1985, 2023, 'movies.db')
process_movies('movies.db')
#export_join_to_csv('movies.db', 'joined_data2.csv')
