# YAIIU Photo Server

This server lets YAIIU upload iPhone photos into a durable, self-hosted archive. Immich is optional. The server keeps the original upload on disk before it forwards a copy to Immich.

## Modes

- **Standalone:** leave `IMMICH_SERVER_URL` empty. YAIIU signs in against the configured standalone email and password, and uploads are stored in `DATA_DIR`.
- **With Immich:** set `IMMICH_SERVER_URL` to an Immich HTTP(S) origin. Login and normal Immich API calls pass through to Immich. Both YAIIU upload paths are archived first, then sent to Immich as a streamed multipart upload.

If Immich is unavailable, the server keeps the original and returns an upload error so YAIIU can retry. A token hash from a successful Immich login or upload is stored in the archive directory for up to 30 days; this lets the server accept retries during an Immich outage without saving the bearer token itself. Set `IMMICH_API_KEY` to let the server retry and backfill archived uploads in the background. Create an Immich API key with asset upload permission for this purpose. Without it, forwarding resumes when YAIIU retries an upload with a valid session.

Standalone mode implements the login, current-user, and empty asset-sync endpoints YAIIU needs for photo uploads. It does not implement Immich's gallery, album, sharing, or search features. The archive itself can be browsed or downloaded through the read-only archive API below.

## Data layout and archive API

Uploads are written under `DATA_DIR/originals/YYYY/MM/`. Each asset has a JSON sidecar with capture metadata, checksums, and Immich forwarding status. Staged uploads use `DATA_DIR/.staging/` and are removed after a successful commit. Mount `DATA_DIR` on persistent storage.

The read-only archive API requires `Authorization: Bearer <STANDALONE_API_KEY>`:

- `GET /api/archive/assets?limit=200` lists assets and their forwarding status.
- `GET /api/archive/assets/{id}` downloads an original.

The endpoint is disabled in Immich mode unless `STANDALONE_API_KEY` is set.

## Configuration

Copy `.env.example` to `.env` and set the standalone credentials and a random API key. Leave `IMMICH_SERVER_URL` empty for standalone mode.

| Variable | Default | Description |
| --- | --- | --- |
| `LISTEN_ADDR` | `:8080` | Address inside the container |
| `DATA_DIR` | `./data` | Persistent originals and upload state |
| `STANDALONE_API_KEY` | — | At least 32 characters; YAIIU's standalone access token and archive API token |
| `STANDALONE_EMAIL` | — | Login email required when Immich is not configured |
| `STANDALONE_PASSWORD` | — | Login password, at least 12 characters, required in standalone mode |
| `STANDALONE_NAME` | `Photo Backup` | Display name returned to YAIIU |
| `IMMICH_SERVER_URL` | empty | Immich origin; empty selects standalone mode |
| `IMMICH_API_KEY` | empty | Optional server-side key for background forwarding and backlog delivery |
| `MAX_UPLOAD_BYTES` | `107374182400` | Maximum file size, default 100 GiB |

The reverse proxy in front of this server must allow request bodies at least as large as `MAX_UPLOAD_BYTES`. For iPhone use, expose it over HTTPS. A local HTTP endpoint is useful for development on the Mac or Simulator.

## Run locally with Docker Compose

```sh
cp .env.example .env
# Edit .env with STANDALONE_API_KEY, STANDALONE_EMAIL, and STANDALONE_PASSWORD.
docker compose up -d --build
```

The server is available on `http://127.0.0.1:8090`. Compose stores the archive in `./data`.

To connect this proxy container to Immich running on the same Mac, set:

```dotenv
IMMICH_SERVER_URL=http://host.docker.internal:2284
IMMICH_API_KEY=your-immich-upload-key
```

Then rebuild/recreate the proxy with `docker compose up -d --build`. For two services on the same Compose network, use `http://immich-server:2283` instead. When Immich is configured, YAIIU should log in using the Immich account; the standalone email/password are ignored.

## Development build

Build the Go proxy with `go build ./...` from this directory. Build the YAIIU Debug app from the repository root with Xcode. The PhotoKit background upload extension requires a physical device for end-to-end upload debugging.

The current extension `Info.plist` has `BackgroundUploadURLBase` set to `https://github.com`. PhotoKit may reject a custom server URL until that extension setting is configured for the development server's HTTPS origin; the app's Swift client does not need to change.

The Helm chart also mounts the archive to a persistent volume claim by default (100 GiB, configurable). Provide standalone credentials and an optional `IMMICH_API_KEY` through `extraEnv` using Kubernetes Secrets rather than committing secrets in `values.yaml`.

## Existing proxy behavior

- `GET /health` returns the status and time YAIIU expects.
- `OPTIONS /api/assets/background` returns `501` so PhotoKit selects its non-resumable upload path.
- `POST /api/assets/background` accepts raw PhotoKit bytes and their metadata headers.
- `POST /api/assets` accepts YAIIU's multipart foreground upload.
- Other API requests are proxied to Immich when `IMMICH_SERVER_URL` is set.

The archive stores received bytes unchanged. If an incoming background image needs the timezone offset added for Immich, ExifTool writes a temporary forwarding copy; the archived original stays unchanged.
