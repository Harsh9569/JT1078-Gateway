# JT1078 Gateway

Self-contained JT/T 1078 → HLS media gateway for live camera video.
The camera pushes its 1078 stream here (after the platform sends `0x9101`); this
**transcodes it with FFmpeg** and serves it as HLS that a browser plays with hls.js.

The Pictor camera streams **H.265/HEVC** on every stream (main + sub). The JT/T
808/1078 protocol has **no command to switch the codec to H.264** — the codec is a
read-only terminal attribute (reported via `0x1003`, queried via `0x9003`), not a
settable parameter. Browsers can't reliably play raw H.265, so the gateway pipes the
raw H.265 Annex-B elementary stream into FFmpeg (`-c:v libx264`) and outputs HLS.

It bundles the [SmallChi/JT1078](https://github.com/SmallChi/JT1078) libraries
(MIT) under `src/JT1078.Protocol` (used only to de-frame + merge the 1078 packets).

## Requirements
- **.NET 8 SDK** (`dotnet --version`)
- **FFmpeg** on the server. Download a Windows build, e.g.
  https://www.gyan.dev/ffmpeg/builds/ (the `ffmpeg-release-essentials.zip`),
  unzip, and either add the `bin` folder to PATH so `ffmpeg` resolves, **or** set
  the full path in `appsettings.json` → `"FfmpegPath": "D:\\ffmpeg\\bin\\ffmpeg.exe"`.
  Verify with `ffmpeg -version`.

## Run
```powershell
cd src/JT1078.Gateway
dotnet restore
dotnet run -c Release
```
On start you'll see:
```
[INGEST] JT1078 ingest listening on TCP 2272
Now listening on: http://0.0.0.0:8090
```

## Ports (configurable in `src/JT1078.Gateway/appsettings.json`)
| Port | Purpose |
|------|---------|
| 2272 | JT1078 **ingest** — the camera pushes video here (`JT1078_MEDIA_TCP_PORT` in the camera service) |
| 8090 | **HTTP/HLS** — browsers play from here (`CAMERA_FLV_BASE=http://<server-ip>:8090`) |

Open both inbound in the Windows firewall **and** the cloud security group.

## Test
1. Trigger the stream from your platform (sends `0x9101` to the camera).
2. Watch the console for `[INGEST] LIVE stream key=<SIM>_<channel>` and
   `[FFMPEG] <key> started`. `[FFMPEG:<key>] ...` lines surface any encode errors.
3. Open `http://<server-ip>:8090/`, enter that key, click **Play**.

Playback URL format: `http://<server-ip>:8090/live/{SIM}_{channel}.m3u8`

## Notes
- Video only (audio skipped).
- Transcoding uses real CPU — fine for a few test cameras; scaling to many needs
  more cores or hardware encoding.
- HLS adds ~3–4s latency. If you need lower latency later, switch the output to
  low-latency HTTP-FLV (mpegts.js) — the H.265→H.264 transcode stays the same.
