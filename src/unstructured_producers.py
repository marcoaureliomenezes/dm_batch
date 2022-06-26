from pytube import YouTube
from pytube import Playlist
import os
from os import path
import subprocess
from pymongo import MongoClient
import requests
from io import BytesIO
from scipy.io.wavfile import read, write



link = "https://www.youtube.com/watch?v=XIiu0JI3I5g&list=PLXRJ0SLXQeZrRxcRgkjPCffmHzPilLd4D"


yt = Playlist(link)

for url in yt.video_urls:
    ys = YouTube(url)
    print("O titulo =e igual a ", ys.title)
    v = ys.streams.get_audio_only()
    out_file = v.download('tmp/audios')
    print(out_file)
    base, ext = os.path.splitext(out_file)
    new_file = base + '.mp3'
    os.rename(out_file, new_file)
    subprocess.call(['ffmpeg', '-i', new_file, f"{base}.wav"])
    os.remove(new_file)
    # coloca no mongo

    cluster = MongoClient('localhost', 27017)

    # database
    db = cluster["aiproduct"]
    # collections
    collection = db["audiofile"]
    rate, data = read(BytesIO(f"{base}.wav"))
    print(rate)
    break