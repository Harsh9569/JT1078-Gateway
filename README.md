# JT1078 Gateway

Self-contained JT/T 1078 → HTTP-FLV media gateway for live camera video.
The camera pushes its 1078 stream here (after the platform sends `0x9101`); this
serves it as HTTP-FLV that a browser plays with mpegts.js.

It bundles the [SmallChi/JT1078](https://github.com/SmallChi/JT1078) libraries
(MIT) under `src/JT1078.Protocol` and `src/JT1078.Flv`, so it builds and runs
without any extra clone.

## Requirements
- **.NET 6 SDK** (`dotnet --version`)

## Run
```powershell
cd src/JT1078.Gateway
dotnet restore
dotnet run -c Release
```
On start you'll see:
```
[INGEST] JT1078 ingest listening on TCP 2272
Now listening on: http://0.0.0.0:8080
```

## Ports (configurable in `src/JT1078.Gateway/appsettings.json`)
| Port | Purpose |
|------|---------|
| 2272 | JT1078 **ingest** — the camera pushes video here (set as `JT1078_MEDIA_TCP_PORT` in the camera service) |
| 8080 | **HTTP-FLV** — browsers play from here (set `CAMERA_FLV_BASE=http://<server-ip>:8080`) |

Open both inbound in the Windows firewall **and** the cloud security group.

## Test
1. Trigger the stream from your platform (sends `0x9101` to the camera).
2. Watch the console for `[INGEST] LIVE stream key=<SIM>_<channel>`.
3. Open `http://<server-ip>:8080/`, enter that key, click **Play**.

Playback URL format: `http://<server-ip>:8080/live/{SIM}_{channel}.flv`

## Notes
- Video only for now (audio skipped).
- New viewers start at the next key-frame, so there can be a short delay before the picture appears.
