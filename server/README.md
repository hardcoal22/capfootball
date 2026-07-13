# Slime Soccer (Cap Football) — Online Server

This replaces the server currently deployed at `capfootbale-server.onrender.com`.

## Why

The old server had a bug: once anyone joined a room — even if their connection
dropped a second later (page refresh, double-click on Join, phone screen lock) —
the seat was **never freed**. From then on, entering that room code always
returned **"Room not found or full"**, and the host was never told the opponent
left. This server fixes that:

- A joiner who disconnects before the game starts frees the seat again; the
  host sees "OPPONENT LEFT — WAITING FOR NEW OPPONENT..." and the same code
  keeps working.
- Dead connections are detected with ping/pong (30s) so stuck seats free up.
- Idle rooms expire after 20 minutes.

`public/index.html` is the game page (served at the site root). It is a copy of
`../CapFootball/index.html` with the WebSocket URL auto-detected from the page
host. If you edit the game, update both copies.

## Run locally

```
cd server
npm install
npm start        # http://localhost:2567  (WebSocket at ws://localhost:2567/ws)
```

`../CapFootball/index.html` connects to `ws://localhost:2567/ws` automatically
when opened from localhost.

## Deploy to Render

Replace the existing `capfootbale-server` service contents with this folder
(push it to the repo/branch the service deploys from):

- Build command: `npm install`
- Start command: `npm start` (Render's `PORT` env var is picked up automatically)

**Region matters:** the game shows live ping in the top-right corner during a
match. Measured WebSocket round-trip to the current deployment is ~220ms —
the service is hosted in a far region. In the Render dashboard, create the
service in the region closest to the players (e.g. **Frankfurt** for
Europe/Middle East); that alone should cut the ping to roughly a third.

## Player records & Hall of Fame

Players sign in through Firebase OnePass using email/password or Google, and
online wins/losses are tracked on their verified UID in the shared Loony Firebase
project:

`generaluserstation`

Hall of Fame data is stored in Firestore under:

`users/{uid}.slimeSoccer`

The server exposes a small REST API (`POST /api/register`,
`GET /api/record?name=`, `GET /api/halloffame`) and credits W/L when each online
match ends.

Lobby chat uses the same `generaluserstation` Firestore project. Messages are
stored in the `slimeSoccerChat` collection, while WebSockets deliver them
instantly to connected players. The latest 100 messages are restored when a
player enters the lobby.

### Storage backend

Records live in an in-memory cache loaded from Firestore and mirrored back to the
same OnePass user document. By default, SlimeSoccer does not write Hall of Fame
data to a local file or to another Firebase collection.

The legacy local `server/records.json` backend is disabled unless
`REQUIRE_ONEPASS_FIREBASE=false` is set explicitly for isolated development.

### Enable Firebase (Firestore)

1. Use the existing project:
   `https://console.firebase.google.com/project/generaluserstation/overview`
2. Make sure **Build -> Firestore Database** exists. Production mode is fine; the
   server uses the Admin SDK, which bypasses client security rules.
3. **Project settings -> Service accounts -> Generate new private key.** This
   downloads a JSON key file. Keep it secret and never commit it.
4. Install the dependency, already listed as an optional dependency:
   ```
   cd server && npm install
   ```
5. Point the server at the key with one of:
   - `GOOGLE_APPLICATION_CREDENTIALS` = absolute path to the JSON file
   - `FIREBASE_SERVICE_ACCOUNT` = the full JSON contents as a single-line string
6. Set these environment variables:
   ```
   FIREBASE_PROJECT_ID=generaluserstation
   ONEPASS_USERS_COLLECTION=users
   REQUIRE_ONEPASS_FIREBASE=true
   ```
7. Start the server. On boot it logs:
   `Records: Firebase generaluserstation/users (N loaded)`.

If the service-account key belongs to a different Firebase project, startup will
reject it instead of writing Hall of Fame data to the wrong project.

On Render: add the env vars under **Environment**, redeploy, and confirm the boot
log says `Firebase generaluserstation/users`.
## Latency features

- Client predicts its own slime locally, so your controls feel instant even
  on a high ping (the opponent and ball remain server-authoritative).
- `ping`/`pong` protocol for the in-game ping display.
- `setNoDelay` on sockets, compact rounded state packets, and state frames
  are skipped for a backlogged connection so slow links don't queue up lag.
