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
CACHE_MAX_ENTRIES = 50
CACHE_CLEAN_INTERVAL = 300  # seconds
INITIAL_QUEUE_SIZE = 30  # up-next tracks fetched when a fresh radio starts
EXTEND_BATCH_SIZE = 10  # up-next tracks fetched per queue-extend call
MAX_RADIO_BATCH = 30  # hard cap on any single radio/extend request, client-requested or not
MAX_PREFETCH_PER_REQUEST = 4  # how many tracks a client can ask to prefetch at once

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

    # `radio` lets the client opt out of building an autoplay queue (e.g. when
    # it's just resuming a track that's already in its local queue/history).
    want_radio = data.get("radio", True) is not False
    playlist_tracks = (
        _get_radio_tracks(video_id, exclude_ids={video_id}, limit=INITIAL_QUEUE_SIZE)
        if want_radio
        else []
    )

    song_data = {
        "stream_url": stream_url,
        "thumbnail_url": thumbnail_url,
        "song_title": title,
        "artist": artist_name,
        "videoId": video_id,
        "playlist": playlist_tracks,
    }
    emit("song_played", song_data, room=request.sid)


@socketio.on("extend_queue")
def extend_queue(data):
    """Fetch more auto-play ("radio") tracks that continue on from a seed
    track, so the client's up-next queue can keep growing dynamically
    instead of ever running dry - mirrors YouTube Music's endless radio."""
    if not isinstance(data, dict):
        emit("queue_extend_error", {"error": "Invalid request"}, room=request.sid)
        return

    video_id = (data.get("video_id") or "").strip() or None
    if not video_id:
        emit("queue_extend_error", {"error": "No seed track specified"}, room=request.sid)
        return

    raw_exclude = data.get("exclude") or []
    if not isinstance(raw_exclude, list):
        raw_exclude = []
    exclude_ids = {video_id} | {str(x).strip() for x in raw_exclude if str(x).strip()}

    try:
        requested_limit = int(data.get("limit", EXTEND_BATCH_SIZE))
    except (TypeError, ValueError):
        requested_limit = EXTEND_BATCH_SIZE
    limit = max(1, min(requested_limit, MAX_RADIO_BATCH))

    tracks = _get_radio_tracks(video_id, exclude_ids=exclude_ids, limit=limit)
    if not tracks:
        emit("queue_extend_error", {"error": "Couldn't find more tracks for this radio"}, room=request.sid)
        return

    emit("queue_extended", {"seed_video_id": video_id, "tracks": tracks}, room=request.sid)


@socketio.on("prefetch_tracks")
def prefetch_tracks(data):
    """Warm the stream-url cache for a handful of upcoming queue tracks in
    the background, so playback is instant once the user actually reaches
    them. Fire-and-forget: no reply is sent, and failures are only logged -
    a failed prefetch just means that track resolves normally when played."""
    if not isinstance(data, dict):
        return

    raw_ids = data.get("video_ids") or []
    if not isinstance(raw_ids, list):
        return

    video_ids = []
    seen = set()
    for raw_id in raw_ids:
        video_id = str(raw_id).strip()
        if video_id and video_id not in seen:
            seen.add(video_id)
            video_ids.append(video_id)
        if len(video_ids) >= MAX_PREFETCH_PER_REQUEST:
            break

    for video_id in video_ids:
        socketio.start_background_task(_prefetch_stream_url, video_id)


def _prefetch_stream_url(video_id):
    """Resolve and cache a track's stream URL ahead of time. Runs in its own
    greenlet so a slow/failed extraction never blocks anything else."""
    try:
        video_url = f"https://music.youtube.com/watch?v={video_id}"
        with _cache_lock:
            cached = stream_url_cache.get(video_url)
        if cached and time.time() - cached[1] < CACHE_TIMEOUT:
            return  # already warm
        get_audio_stream_url(video_url)
    except Exception:
        logger.exception("Prefetch failed for video_id=%r", video_id)


def _format_playlist_track(track):
    """Normalize a ytmusicapi track dict into the shape the frontend expects.
    Returns None if the track is missing required fields."""
    t_video_id = track.get("videoId")
    if not t_video_id:
        return None
    p_artists = track.get("artists") or []
    p_thumbnails = track.get("thumbnails") or []
    return {
        "title": track.get("title") or "Unknown Title",
        "artist": p_artists[0]["name"] if p_artists and p_artists[0].get("name") else "Unknown Artist",
        "videoId": t_video_id,
        "thumbnailUrl": p_thumbnails[-1]["url"] if p_thumbnails else None,
    }


def _get_radio_tracks(video_id, exclude_ids=None, limit=EXTEND_BATCH_SIZE):
    """Ask ytmusicapi for a watch-playlist ("radio") seeded on video_id and
    return a deduplicated, cleaned list of up-next tracks. Never raises -
    on any failure it logs and returns an empty list so callers can degrade
    gracefully instead of breaking playback."""
    limit = max(1, min(int(limit or EXTEND_BATCH_SIZE), MAX_RADIO_BATCH))
    seen = set(exclude_ids or ())
    tracks = []
    try:
        watch_playlist = ytmusic.get_watch_playlist(videoId=video_id, limit=limit + len(seen) + 1)
        for track in (watch_playlist.get("tracks") or []):
            try:
                formatted = _format_playlist_track(track)
            except Exception:
                logger.exception("Skipping malformed watch-playlist track: %r", track)
                continue
            if not formatted or formatted["videoId"] in seen:
                continue
            seen.add(formatted["videoId"])
            tracks.append(formatted)
            if len(tracks) >= limit:
                break
    except Exception:
        logger.exception("Error getting watch playlist for video_id=%r", video_id)
        return []
    return tracks


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
            "format": "best",
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
