# Deployment

The repository includes a Docker deployment using Gunicorn and a single
gevent WebSocket worker. Keep one worker unless a shared Socket.IO message
queue and shared cache are added, because radio state and stream URL cache
are process-local.

Set these environment variables in the hosting provider:

- `FLASK_SECRET_KEY`: a stable random value.
- `CORS_ALLOWED_ORIGINS`: the deployed origin, or a comma-separated list of
  trusted origins when the frontend and API are hosted separately.
- `PORT`: supplied by most hosting providers.

Optional tuning variables are `LOG_LEVEL`, `REQUEST_TIMEOUT`,
`CACHE_TIMEOUT`, `CACHE_MAX_ENTRIES`, and `CACHE_CLEAN_INTERVAL`.

The health check endpoint is `GET /healthz`.
