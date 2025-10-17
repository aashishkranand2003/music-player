import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, jsonify, Response
from flask_socketio import SocketIO, emit
from ytmusicapi import YTMusic
import yt_dlp
import requests
import time
import os
from urllib.parse import unquote
import socket

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key')
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

ytmusic = YTMusic()

stream_url_cache = {}
CACHE_TIMEOUT = 500

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/stream/<path:stream_url>')
def stream_audio(stream_url):
    try:
        decoded_url = unquote(stream_url)
        headers = {'Range': request.headers.get('Range', 'bytes=0-')}
        response = requests.get(decoded_url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()

        resp = Response(
            response.iter_content(chunk_size=8192),
            mimetype='audio/webm',
            status=response.status_code
        )
        resp.headers['Content-Range'] = response.headers.get('Content-Range', '')
        resp.headers['Accept-Ranges'] = 'bytes'
        resp.headers['Content-Length'] = response.headers.get('Content-Length', '')
        return resp
    except Exception as e:
        print(f"Streaming error: {e}")
        return jsonify({'error': 'Failed to stream audio'}), 500

@socketio.on('connect')
def handle_connect():
    print(f'User connected: {request.sid}')

@socketio.on('disconnect')
def handle_disconnect():
    print(f'User disconnected: {request.sid}')

@socketio.on('play_song')
def play_song(data):
    song_name = data.get('song_name')
    video_id = data.get('video_id')
    emit('loading_song', room=request.sid)

    if video_id:
        try:
            song_info = ytmusic.get_song(video_id)
            title = song_info['videoDetails']['title']
            artist_name = song_info['videoDetails']['author']
            thumbnail_url = song_info['videoDetails']['thumbnail']['thumbnails'][-1]['url']
            video_url = f"https://music.youtube.com/watch?v={video_id}"
            error = None
        except Exception as e:
            error = str(e)
            title = artist_name = thumbnail_url = video_url = None
    else:
        video_url, thumbnail_url, title, artist_name, error, video_id = search_youtube_music(song_name)

    if error:
        emit('song_error', {'error': error}, room=request.sid)
        return

    if video_url:
        stream_url = get_audio_stream_url(video_url)
        if stream_url:
            try:
                watch_playlist = ytmusic.get_watch_playlist(videoId=video_id, limit=11)
                playlist_tracks = []
                for track in watch_playlist['tracks'][1:]:
                    if 'videoId' in track:
                        p_title = track.get('title', 'Unknown Title')
                        p_artist = track['artists'][0]['name'] if track.get('artists') else 'Unknown Artist'
                        p_thumbnail = track['thumbnails'][-1]['url'] if track.get('thumbnails') else None
                        p_videoId = track['videoId']
                        playlist_tracks.append({
                            'title': p_title,
                            'artist': p_artist,
                            'videoId': p_videoId,
                            'thumbnailUrl': p_thumbnail
                        })
            except Exception as e:
                print(f"Error getting watch playlist: {e}")
                playlist_tracks = []

            song_data = {
                'stream_url': stream_url,
                'thumbnail_url': thumbnail_url,
                'song_title': title,
                'artist': artist_name,
                'videoId': video_id,
                'playlist': playlist_tracks
            }
            emit('song_played', song_data, room=request.sid)
        else:
            emit('song_error', {'error': "Failed to get audio stream URL"}, room=request.sid)
    else:
        emit('song_error', {'error': "Song not found"}, room=request.sid)

@app.route('/search-suggestions', methods=['POST'])
def search_suggestions():
    data = request.get_json()
    query = data.get('query', '')
    suggestions = ytmusic.search(query, filter='songs', limit=5)
    return jsonify([{
        'title': song['title'],
        'artist': song['artists'][0]['name'] if song['artists'] else 'Unknown Artist',
        'videoId': song['videoId'],
        'thumbnailUrl': song['thumbnails'][-1]['url'] if song['thumbnails'] else None
    } for song in suggestions])

def search_youtube_music(query):
    try:
        search_results = ytmusic.search(query, filter='songs', limit=1)
        if not search_results:
            return None, None, None, None, "No results found for the query", None
        result = search_results[0]
        video_id = result.get('videoId')
        if not video_id:
            return None, None, None, None, "Invalid search result format", None
        video_url = f"https://music.youtube.com/watch?v={video_id}"
        title = result.get('title', 'Unknown Title')
        artist_name = result.get('artists', [{}])[0].get('name', 'Unknown Artist')
        thumbnail_url = result['thumbnails'][-1]['url'] if 'thumbnails' in result else None
        return video_url, thumbnail_url, title, artist_name, None, video_id
    except Exception as e:
        error_msg = f"Error searching for song: {e}"
        print(error_msg)
        return None, None, None, None, error_msg, None

def get_audio_stream_url(youtube_url):
    try:
        current_time = time.time()
        if youtube_url in stream_url_cache:
            url, timestamp = stream_url_cache[youtube_url]
            if current_time - timestamp < CACHE_TIMEOUT:
                return url

        ydl_opts = {
            'format': '251/140/best',
            'quiet': True,
            'noplaylist': False,
            'http_chunk_size': 8192,
            'no_warnings': True,
            'extract_flat': True,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(youtube_url, download=False)
            stream_url = info_dict['url']

        stream_url_cache[youtube_url] = (stream_url, current_time)
        return stream_url
    except Exception as e:
        print(f"Error getting audio stream URL: {e}")
        return None

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=True) 