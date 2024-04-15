import os
import json
import requests
import xmltodict

from airflow.decorators import dag, task
import pendulum
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from vosk import Model, KaldiRecognizer
from pydub import AudioSegment

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "episodes"
FRAME_RATE = 16000

@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 4, 15),
    catchup=False,
)
def podcast_summary():

    def create_table():
        hook = SqliteHook(sqlite_conn_id="podcasts")
        hook.run(r"""
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT PRIMARY KEY,
                title TEXT,
                filename TEXT,
                published TEXT,
                description TEXT,
                transcript TEXT
            );
        """)

    create_database = PythonOperator(
        task_id='create_table_sqlite',
        python_callable=create_table,
    )


    @task()
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored_episodes = hook.get_pandas_df("SELECT * from episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_episodes["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])
        return new_episodes

    new_episodes = load_episodes(podcast_episodes)

    
    # def download_episodes(episodes):
    #     audio_files = []
    #     for episode in episodes:
    #         name_end = episode["link"].split('/')[-1]
    #         filename = f"{name_end}.mp3"
    #         audio_path = os.path.join(EPISODE_FOLDER, filename)
    #         if not os.path.exists(audio_path):
    #             print(f"Downloading {filename}")
    #             audio = requests.get(episode["enclosure"]["@url"])
    #             with open(audio_path, "wb+") as f:
    #                 f.write(audio.content)
    #         audio_files.append({
    #             "link": episode["link"],
    #             "filename": filename
    #         })
    #     return audio_files
    
    @task()
    def download_episodes(episodes):
        audio_files = []
        if not os.path.exists(EPISODE_FOLDER):
            os.makedirs(EPISODE_FOLDER)

        for episode in episodes:
            name_end = episode["link"].split('/')[-1]
            filename = f"{name_end}.mp3"
            audio_path = os.path.join(EPISODE_FOLDER, filename)

            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                try:
                    audio = requests.get(episode["enclosure"]["@url"])
                    with open(audio_path, "wb+") as f:
                        f.write(audio.content)
                    audio_files.append({
                        "link": episode["link"],
                        "filename": filename
                    })
                except Exception as e:
                    print(f"Error downloading {filename}: {e}")
            else:
                audio_files.append({
                    "link": episode["link"],
                    "filename": filename
                })

        return audio_files

    audio_files = download_episodes(podcast_episodes)


summary = podcast_summary()