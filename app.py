import logging
import os
import socket
import threading
import time
from urllib.parse import unquote, urlparse

import eventlet

eventlet.monkey_patch()

import requests
from flask import Flask, Response, jsonify, render_template, request
from flask_socketio import SocketIO, emit
from ytmusicapi import YTMusic

try:
    import yt_dlp
except ImportError as exc:  # pragma: no cover - fail fast with a clear message
    raise RuntimeError("yt-dlp is required. Install it with: pip install yt-dlp") from exc

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("music_player")

# --------------------------------------------------------------------------
# App / config
# --------------------------------------------------------------------------
app = Flask(__name__)

_secret_key = os.environ.get("FLASK_SECRET_KEY")
app.config["SECRET_KEY"] = _secret_key

socketio = SocketIO(
    app,
    async_mode="eventlet",
    cors_allowed_origins=os.environ.get("CORS_ALLOWED_ORIGINS", "*"),
    manage_session=False,
    logger=False,
    engineio_logger=False,
)


REQUEST_TIMEOUT = 15  # seconds
CACHE_TIMEOUT = 500  # seconds
CACHE_MAX_ENTRIES = 500
CACHE_CLEAN_INTERVAL = 300  # seconds

stream_url_cache = {}
_cache_lock = threading.Lock()

# --------------------------------------------------------------------------
# YTMusic client
# --------------------------------------------------------------------------
ytmusic = YTMusic()


# --------------------------------------------------------------------------
# Cache maintenance
# --------------------------------------------------------------------------
def _clean_stream_cache():
    """Periodically evict expired / excess entries so the cache can't grow
    without bound over a long-running process."""
    while True:
        eventlet.sleep(CACHE_CLEAN_INTERVAL)
        try:
            now = time.time()
            with _cache_lock:
                expired = [k for k, (_, ts) in stream_url_cache.items() if now - ts >= CACHE_TIMEOUT]
                for k in expired:
                    del stream_url_cache[k]

                # Hard cap: drop oldest entries if still too large.
                if len(stream_url_cache) > CACHE_MAX_ENTRIES:
                    by_age = sorted(stream_url_cache.items(), key=lambda kv: kv[1][1])
                    overflow = len(stream_url_cache) - CACHE_MAX_ENTRIES
                    for k, _ in by_age[:overflow]:
                        del stream_url_cache[k]
            if expired:
                logger.debug("Cleaned %d expired stream cache entries", len(expired))
        except Exception:
            logger.exception("Error while cleaning stream cache")


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("inde.html")


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.errorhandler(404)
def not_found(_err):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(err):
    logger.exception("Unhandled server error: %s", err)
    return jsonify({"error": "Internal server error"}), 500




@app.route("/stream/<path:stream_url>")
def stream_audio(stream_url):
    try:
        decoded_url = unquote(stream_url)
    except Exception:
        return jsonify({"error": "Invalid stream url"}), 400

    try:
        headers = {"Range": request.headers.get("Range", "bytes=0-")}
        upstream = requests.get(
            decoded_url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT
        )
        upstream.raise_for_status()

        def generate():
            try:
                for chunk in upstream.iter_content(chunk_size=8192):
                    if chunk:
                        yield chunk
            except requests.exceptions.RequestException:
                logger.warning("Upstream stream interrupted mid-transfer")
            finally:
                upstream.close()

        resp = Response(generate(), mimetype="audio/webm", status=upstream.status_code)
        if upstream.headers.get("Content-Range"):
            resp.headers["Content-Range"] = upstream.headers["Content-Range"]
        resp.headers["Accept-Ranges"] = "bytes"
        if upstream.headers.get("Content-Length"):
            resp.headers["Content-Length"] = upstream.headers["Content-Length"]
        return resp
    except requests.exceptions.Timeout:
        logger.warning("Streaming timed out for %s", decoded_url)
        return jsonify({"error": "Streaming source timed out"}), 504
    except requests.exceptions.RequestException as e:
        logger.warning("Streaming request failed: %s", e)
        return jsonify({"error": "Failed to stream audio"}), 502
    except Exception:
        logger.exception("Unexpected streaming error")
        return jsonify({"error": "Failed to stream audio"}), 500


@app.route("/search-suggestions", methods=["POST"])
def search_suggestions():
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify([])
    if len(query) > 200:
        return jsonify({"error": "Query too long"}), 400

    try:
        results = ytmusic.search(query, filter="songs", limit=5) or []
    except Exception:
        logger.exception("search-suggestions failed for query=%r", query)
        return jsonify({"error": "Search is temporarily unavailable"}), 503

    suggestions = []
    for song in results:
        try:
            artists = song.get("artists") or []
            thumbnails = song.get("thumbnails") or []
            video_id = song.get("videoId")
            if not video_id:
                continue
            suggestions.append(
                {
                    "title": song.get("title") or "Unknown Title",
                    "artist": artists[0]["name"] if artists and artists[0].get("name") else "Unknown Artist",
                    "videoId": video_id,
                    "thumbnailUrl": thumbnails[-1]["url"] if thumbnails else None,
                }
            )
        except Exception:
            logger.exception("Skipping malformed search result: %r", song)
            continue

    return jsonify(suggestions)


# --------------------------------------------------------------------------
# Socket.IO handlers
# --------------------------------------------------------------------------
@socketio.on("connect")
def handle_connect():
    logger.info("Client connected: %s", request.sid)


@socketio.on("disconnect")
def handle_disconnect(reason=None):
    logger.info("Client disconnected: %s", request.sid)


@socketio.on_error_default
def default_error_handler(e):
    logger.exception("Unhandled Socket.IO error: %s", e)
    try:
        emit("song_error", {"error": "Something went wrong. Please try again."})
    except Exception:
        pass


@socketio.on("play_song")
def play_song(data):
    if not isinstance(data, dict):
        emit("song_error", {"error": "Invalid request"}, room=request.sid)
        return

    song_name = (data.get("song_name") or "").strip() or None
    video_id = (data.get("video_id") or "").strip() or None

    if not song_name and not video_id:
        emit("song_error", {"error": "No song specified"}, room=request.sid)
        return

    emit("loading_song", room=request.sid)

    error = None
    title = artist_name = thumbnail_url = video_url = None

    if video_id:
        try:
            song_info = ytmusic.get_song(video_id)
            details = song_info.get("videoDetails") or {}
            title = details.get("title") or "Unknown Title"
            artist_name = details.get("author") or "Unknown Artist"
            thumbnails = (details.get("thumbnail") or {}).get("thumbnails") or []
            thumbnail_url = thumbnails[-1]["url"] if thumbnails else None
            video_url = f"https://music.youtube.com/watch?v={video_id}"
        except Exception as e:
            logger.exception("get_song failed for video_id=%r", video_id)
            error = f"Could not load song: {e}"
    else:
        video_url, thumbnail_url, title, artist_name, error, video_id = search_youtube_music(song_name)

    if error or not video_url:
        emit("song_error", {"error": error or "Song not found"}, room=request.sid)
        return

    stream_url = get_audio_stream_url(video_url)
    if not stream_url:
        emit("song_error", {"error": "Failed to get audio stream URL"}, room=request.sid)
        return

    playlist_tracks = []
    try:
        watch_playlist = ytmusic.get_watch_playlist(videoId=video_id, limit=11)
        for track in (watch_playlist.get("tracks") or [])[1:]:
            t_video_id = track.get("videoId")
            if not t_video_id:
                continue
            p_artists = track.get("artists") or []
            p_thumbnails = track.get("thumbnails") or []
            playlist_tracks.append(
                {
                    "title": track.get("title") or "Unknown Title",
                    "artist": p_artists[0]["name"] if p_artists and p_artists[0].get("name") else "Unknown Artist",
                    "videoId": t_video_id,
                    "thumbnailUrl": p_thumbnails[-1]["url"] if p_thumbnails else None,
                }
            )
    except Exception:
        logger.exception("Error getting watch playlist for video_id=%r", video_id)
        playlist_tracks = []

    song_data = {
        "stream_url": stream_url,
        "thumbnail_url": thumbnail_url,
        "song_title": title,
        "artist": artist_name,
        "videoId": video_id,
        "playlist": playlist_tracks,
    }
    emit("song_played", song_data, room=request.sid)


def search_youtube_music(query):
    try:
        search_results = ytmusic.search(query, filter="songs", limit=1)
        if not search_results:
            return None, None, None, None, "No results found for the query", None
        result = search_results[0]
        video_id = result.get("videoId")
        if not video_id:
            return None, None, None, None, "Invalid search result format", None
        video_url = f"https://music.youtube.com/watch?v={video_id}"
        title = result.get("title") or "Unknown Title"
        artists = result.get("artists") or []
        artist_name = artists[0].get("name", "Unknown Artist") if artists else "Unknown Artist"
        thumbnails = result.get("thumbnails") or []
        thumbnail_url = thumbnails[-1]["url"] if thumbnails else None
        return video_url, thumbnail_url, title, artist_name, None, video_id
    except Exception as e:
        logger.exception("Error searching for song %r", query)
        return None, None, None, None, f"Error searching for song: {e}", None


def get_audio_stream_url(youtube_url):
    try:
        current_time = time.time()
        with _cache_lock:
            cached = stream_url_cache.get(youtube_url)
        if cached:
            url, timestamp = cached
            if current_time - timestamp < CACHE_TIMEOUT:
                return url

        ydl_opts = {
            "format": "251/140/best",
            "quiet": True,
            "noplaylist": False,
            "http_chunk_size": 8192,
            "no_warnings": True,
            "extract_flat": True,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            ),
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(youtube_url, download=False)

        stream_url = info_dict.get("url") if info_dict else None

        if not stream_url:
            logger.warning("No playable stream url found for %s", youtube_url)
            return None

        with _cache_lock:
            stream_url_cache[youtube_url] = (stream_url, current_time)
        return stream_url
    except Exception:
        logger.exception("Error getting audio stream URL for %s", youtube_url)
        return None


def get_local_ip():
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "localhost"
    finally:
        if s is not None:
            s.close()


if __name__ == "__main__":
    server_port = int(os.environ.get("PORT", 5000))
    server_ip = get_local_ip()

    threading.Thread(target=_clean_stream_cache, daemon=True).start()

    logger.info("Server running on http://%s:%s", server_ip, server_port)
    try:
        socketio.run(app, host="0.0.0.0", port=server_port)
    except Exception:
        logger.exception("Server crashed")
        raise
