using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

namespace JT1078.Gateway;

/// <summary>
/// One FFmpeg process per live stream key. Video is H.265 (fixed in firmware),
/// transcoded to H.264 and segmented to HLS. When the camera also sends audio
/// (requested via dataType=0) we feed that audio to the SAME FFmpeg over a local
/// TCP connection and mux it as AAC, so the browser gets video WITH sound.
///
/// Audio transport is TCP (not UDP) on an OS-assigned loopback port: FFmpeg
/// connects OUT to us, so it never binds a fixed port — avoiding the Windows
/// reserved/excluded UDP port ranges (WSAEACCES/WSAEADDRINUSE).
///
/// Each job buffers briefly (DecideMs) to learn whether audio is present, then
/// starts FFmpeg with/without audio. If FFmpeg dies while running with audio, we
/// retry once VIDEO-ONLY so video never breaks. Stream key = "{SIM}_{channel}".
/// </summary>
public class FfmpegTranscoder
{
    private enum JobState { Buffering, Running, Dead }

    private class Job
    {
        public readonly object Gate = new();
        public JobState State = JobState.Buffering;
        public Process Proc;
        public Stream Stdin;
        public TcpListener AudioListener;
        public Stream AudioStream;     // accepted ffmpeg audio connection
        public Channel<byte[]> AudioQueue;   // ingest -> writer task; drops when full
        public bool WithAudio;
        public bool SawAudio;
        public bool ForceNoAudio;      // set when we fall back to video-only
        public bool TriedVideoOnly;
        public string Key;
        public long FramesIn;
        public long AudioIn;
        public bool? IsHevc;           // detected video codec: true=H.265, false=H.264, null=unknown
        public readonly List<byte[]> PendingVideo = new();
        public readonly List<byte[]> PendingAudio = new();
    }

    private readonly ConcurrentDictionary<string, Job> _jobs = new();
    private readonly string _ffmpeg;
    private readonly string _outDir;
    private readonly bool _audioEnabled;
    private readonly string _audioFmt;
    private readonly int _audioRate;
    private readonly ILogger _log;
    private const int DecideMs = 1500;
    private const int MaxBuffered = 600;

    public FfmpegTranscoder(string ffmpegPath, string outDir, ILogger log,
        bool audioEnabled = true, string audioFmt = "alaw", int audioRate = 8000)
    {
        _ffmpeg = ffmpegPath;
        _outDir = outDir;
        _log = log;
        _audioEnabled = audioEnabled;
        _audioFmt = audioFmt;
        _audioRate = audioRate;
        Directory.CreateDirectory(_outDir);
    }

    public IEnumerable<string> ActiveKeys() => _jobs.Keys;

    public void Feed(string key, byte[] annexB)
    {
        var job = GetOrCreate(key);
        lock (job.Gate)
        {
            if (job.State == JobState.Buffering)
            {
                if (job.PendingVideo.Count < MaxBuffered) job.PendingVideo.Add(annexB);
                return;
            }
            if (job.State != JobState.Running) return;
            try { job.Stdin.Write(annexB, 0, annexB.Length); job.Stdin.Flush(); job.FramesIn++; }
            catch (Exception ex) { _log.LogWarning("[FFMPEG] {k} video write failed ({m})", key, ex.Message); StopLocked(job); }
        }
    }

    public void FeedAudio(string key, byte[] audio)
    {
        if (!_audioEnabled) return;
        var job = GetOrCreate(key);
        lock (job.Gate)
        {
            job.SawAudio = true;
            if (job.State == JobState.Buffering)
            {
                if (job.PendingAudio.Count < MaxBuffered) job.PendingAudio.Add(audio);
                return;
            }
            if (job.State != JobState.Running || !job.WithAudio) return;
            // Non-blocking hand-off: a dedicated task drains this queue to ffmpeg's
            // audio socket. Audio must NEVER block here — this runs on the same
            // thread that feeds video, and a blocking audio write (when ffmpeg
            // stops draining) would stall video too. Drop oldest if the queue is
            // full (audio is best-effort; video must keep flowing).
            if (job.AudioQueue != null && job.AudioQueue.Writer.TryWrite(audio)) job.AudioIn++;
        }
    }

    public void Stop(string key)
    {
        if (_jobs.TryRemove(key, out var job))
            lock (job.Gate) StopLocked(job);
    }

    public void StopAll(IEnumerable<string> keys)
    {
        foreach (var k in keys.ToArray()) Stop(k);
    }

    // ── internals ─────────────────────────────────────────────────────────────

    // Detect the video codec from Annex-B NAL headers so FFmpeg is hinted correctly.
    // H.264: nal_unit_type = firstByte & 0x1F  (SPS = 7  -> bytes 0x67/0x47/0x27)
    // H.265: nal_unit_type = (firstByte>>1) & 0x3F (VPS=32/0x40, SPS=33/0x42)
    // Returns true = H.265, false = H.264, null = undetermined (caller defaults H.265).
    private static bool? DetectHevc(IEnumerable<byte[]> frames)
    {
        foreach (var f in frames)
        {
            if (f == null) continue;
            for (int i = 0; i + 4 < f.Length; i++)
            {
                if (f[i] != 0 || f[i + 1] != 0) continue;         // Annex-B start code?
                int nalPos;
                if (f[i + 2] == 1) nalPos = i + 3;                 // 00 00 01
                else if (f[i + 2] == 0 && f[i + 3] == 1) nalPos = i + 4; // 00 00 00 01
                else continue;
                if (nalPos >= f.Length) break;
                byte b = f[nalPos];
                if (b == 0x67 || b == 0x47 || b == 0x27) return false; // H.264 SPS
                if (b == 0x40 || b == 0x42) return true;               // H.265 VPS/SPS
            }
        }
        return null;
    }

    // Detect ADTS AAC audio: frames begin with a 12-bit sync word 0xFFF
    // (byte0 == 0xFF, high nibble of byte1 == 0xF). Raw G.711 A-law has no sync
    // pattern, so absence of the sync word means the configured default (alaw).
    private static bool DetectAac(IEnumerable<byte[]> audioFrames)
    {
        foreach (var a in audioFrames)
            if (a != null && a.Length >= 2 && a[0] == 0xFF && (a[1] & 0xF0) == 0xF0)
                return true;
        return false;
    }

    private Job GetOrCreate(string key)
    {
        return _jobs.GetOrAdd(key, k =>
        {
            var job = new Job { Key = k };
            _log.LogInformation("[FFMPEG] {k} buffering {ms}ms to detect audio (audioEnabled={ae})", k, DecideMs, _audioEnabled);
            _ = Task.Run(async () => { await Task.Delay(DecideMs); StartJob(job); });
            return job;
        });
    }

    private void StartJob(Job job)
    {
        lock (job.Gate)
        {
            if (job.State != JobState.Buffering) return;
            bool withAudio = _audioEnabled && job.SawAudio && !job.ForceNoAudio;
            job.WithAudio = withAudio;

            // Detect the actual video codec from the buffered Annex-B frames. Old
            // cameras are H.265 (firmware-fixed); some JT/T 808-2019 units (15-digit
            // IMEI) send H.264. Feeding H.264 into "-f hevc" makes FFmpeg decode
            // nothing -> no HLS output -> watchdog kills it -> even the video-only
            // retry fails. Pick the right demuxer; default H.265 so old cameras are
            // unchanged when detection is inconclusive.
            if (job.IsHevc == null) job.IsHevc = DetectHevc(job.PendingVideo);
            bool useHevc = job.IsHevc ?? true;
            string codecFmt = useHevc ? "hevc" : "h264";

            string safe = job.Key.Replace("/", "_").Replace("\\", "_").Replace("..", "_");
            string m3u8 = Path.Combine(_outDir, safe + ".m3u8");
            string seg = Path.Combine(_outDir, safe + "_%03d.ts");

            // clear stale HLS files for this key (leftover from a prior session)
            try
            {
                foreach (var f in Directory.GetFiles(_outDir, safe + "_*.ts")) File.Delete(f);
                if (File.Exists(m3u8)) File.Delete(m3u8);
            }
            catch { }

            // audio over TCP: we listen on an OS-assigned loopback port; ffmpeg
            // connects out to it (no fixed-port bind → no Windows excluded-range error)
            int audioPort = 0;
            if (withAudio)
            {
                try
                {
                    job.AudioListener = new TcpListener(IPAddress.Loopback, 0);
                    job.AudioListener.Start();
                    audioPort = ((IPEndPoint)job.AudioListener.LocalEndpoint).Port;
                    job.AudioQueue = Channel.CreateBounded<byte[]>(
                        new BoundedChannelOptions(300) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
                }
                catch (Exception ex)
                {
                    _log.LogWarning("[FFMPEG] {k} audio listener failed ({m}); video-only", job.Key, ex.Message);
                    withAudio = false; job.WithAudio = false;
                }
            }

            // Feed rate: a fixed -framerate 25 drifts badly when a camera sends fewer
            // fps (this 2019 H.264 unit sends ~12), which grows the latency AND, since
            // the audio then rides a faster/slower clock, stops the HLS muxer aligning
            // A/V within the 6s watchdog. Estimate the REAL rate from the frames
            // buffered during detection so the video clock tracks real time (and the
            // audio clock). Keep genpts+framerate (assigns clean per-frame timestamps,
            // so the burst-flushed startup buffer is fine — unlike wall-clock, which
            // gave every buffered frame the same stamp and produced no output at all).
            // Old H.265 cameras keep the proven fixed 25.
            int estFps = job.PendingVideo.Count >= 8
                ? Math.Clamp((int)Math.Round(job.PendingVideo.Count / (DecideMs / 1000.0)), 8, 30)
                : 15;
            string videoIn = useHevc
                ? $"-fflags +genpts -framerate 25 -f hevc -i pipe:0 "
                : $"-fflags +genpts -framerate {estFps} -f h264 -i pipe:0 ";

            // Detect the audio codec from the buffered frames. The default is G.711
            // A-law (raw samples), but some cameras — including this JT/T 808-2019
            // unit — send AAC in ADTS framing (0xFFFx sync word). Feeding AAC bytes
            // to "-f alaw" makes FFmpeg decode garbage -> broken audio timestamps ->
            // the HLS muxer stalls and never writes a segment (the "no HLS output in
            // 6s" fallback). For ADTS AAC the frame header carries sample-rate and
            // channels, so we must NOT force -ar/-ac on the input.
            bool audioIsAac = withAudio && DetectAac(job.PendingAudio);
            string audioFmt = audioIsAac ? "aac" : _audioFmt;
            // -analyzeduration 0 -probesize: FFmpeg otherwise spends up to its
            // default 5s analyzing an AAC (ADTS) input before it starts muxing —
            // which blows past the 6s watchdog and looks like an audio "stall".
            // Raw A-law needs no analysis (fully specified by -ar/-ac), which is
            // why alaw cameras never stalled. Force a tiny probe so the muxer starts
            // immediately and audio + video interleave from the first frames.
            // AAC audio: -fflags +genpts puts it on the same 0-based clock as the
            // genpts video above (now that video runs at the real frame rate, the two
            // clocks track together and interleave). -analyzeduration 0 -probesize
            // small so FFmpeg doesn't spend seconds probing the AAC stream before it
            // starts muxing. A-law audio (old cameras) is left exactly as before.
            string audioIn = withAudio
                ? (audioIsAac
                    ? $"-fflags +genpts -analyzeduration 0 -probesize 4096 -thread_queue_size 1024 -f aac -i tcp://127.0.0.1:{audioPort} "
                    : $"-thread_queue_size 1024 -f {_audioFmt} -ar {_audioRate} -ac 1 -i tcp://127.0.0.1:{audioPort} ")
                : "";
            string maps = withAudio
                ? "-map 0:v:0 -map 1:a:0 -c:a aac -b:a 64k -ar 44100 -filter:a aresample=async=1000 "
                : "-map 0:v:0 -an ";

            // -max_interleave_delta: max time the muxer buffers to interleave A/V.
            // 0 means "buffer indefinitely until every stream has a packet" — with
            // live audio that trickles in behind the burst-flushed video, that makes
            // the muxer wait forever and write NOTHING (the "no HLS output in 6s"
            // stall). A small positive value (0.1s) flushes video promptly and still
            // interleaves audio when it's on time. Harmless for the video-only path
            // (single stream never needs interleaving).
            string args =
                "-hide_banner -loglevel warning " + videoIn + audioIn +
                "-c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -g 25 " +
                "-max_interleave_delta 100000 -max_muxing_queue_size 1024 -muxpreload 0 -muxdelay 0 " + maps +
                "-f hls -hls_time 1 -hls_list_size 6 " +
                "-hls_flags delete_segments+omit_endlist+independent_segments " +
                $"-hls_segment_type mpegts -hls_segment_filename \"{seg}\" \"{m3u8}\"";

            var psi = new ProcessStartInfo
            {
                FileName = _ffmpeg,
                Arguments = args,
                RedirectStandardInput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            Process proc;
            try { proc = Process.Start(psi); }
            catch (Exception ex)
            {
                _log.LogError("[FFMPEG] failed to start '{f}' for {k}: {m}", _ffmpeg, job.Key, ex.Message);
                try { job.AudioListener?.Stop(); } catch { }
                job.State = JobState.Dead;
                _jobs.TryRemove(job.Key, out _);
                return;
            }

            job.Proc = proc;
            job.Stdin = proc.StandardInput.BaseStream;
            job.State = JobState.Running;

            proc.EnableRaisingEvents = true;
            proc.Exited += (_, __) => HandleExit(job);

            // Accept ffmpeg's audio connection, then run a DEDICATED writer task that
            // drains the audio queue to the socket. All blocking socket writes happen
            // here — never on the video ingest thread — so a stalled audio socket can
            // never freeze video. If the write blocks/fails we just stop audio; video
            // keeps running.
            if (withAudio && job.AudioListener != null)
            {
                var listener = job.AudioListener;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var client = await listener.AcceptTcpClientAsync();
                        client.NoDelay = true;
                        client.SendTimeout = 2000;
                        var stream = client.GetStream();

                        List<byte[]> pending;
                        lock (job.Gate)
                        {
                            if (job.State != JobState.Running) { try { stream.Dispose(); } catch { } return; }
                            job.AudioStream = stream;
                            pending = new List<byte[]>(job.PendingAudio);
                            job.PendingAudio.Clear();
                        }

                        foreach (var a in pending) await stream.WriteAsync(a, 0, a.Length);

                        var reader = job.AudioQueue.Reader;
                        while (await reader.WaitToReadAsync())
                            while (reader.TryRead(out var buf))
                                await stream.WriteAsync(buf, 0, buf.Length);
                    }
                    catch { /* ffmpeg never connected, socket closed, or write stalled — drop audio, keep video */ }
                });
            }

            // drain ffmpeg stderr to the log
            var key = job.Key;
            _ = Task.Run(async () =>
            {
                try
                {
                    string line;
                    while ((line = await proc.StandardError.ReadLineAsync()) != null)
                        _log.LogInformation("[FFMPEG:{k}] {line}", key, line);
                }
                catch { }
            });

            _log.LogInformation("[FFMPEG] {k} started codec={c} fps={fps} withAudio={a} audioFmt={af} (tcp:{p}) -> {m3u8}",
                job.Key, codecFmt, useHevc ? 25 : estFps, withAudio, withAudio ? audioFmt : "-", audioPort, m3u8);

            // Watchdog: some channels (e.g. higher-res streams) let ffmpeg sit
            // interleaving audio+video forever without ever flushing a segment.
            // If no HLS playlist appears within the window, kill ffmpeg — that
            // triggers HandleExit, which retries the channel VIDEO-ONLY so video
            // always comes through (audio is best-effort).
            if (withAudio)
            {
                string watchM3u8 = m3u8;
                var watchProc = proc;
                _ = Task.Run(async () =>
                {
                    await Task.Delay(6000);
                    bool kill;
                    lock (job.Gate)
                        kill = job.State == JobState.Running && job.WithAudio
                               && !job.TriedVideoOnly && !File.Exists(watchM3u8);
                    if (kill)
                    {
                        _log.LogWarning("[FFMPEG] {k} no HLS output in 6s with audio; falling back to VIDEO-ONLY", job.Key);
                        try { if (!watchProc.HasExited) watchProc.Kill(true); } catch { }
                    }
                });
            }

            // flush buffered video
            foreach (var v in job.PendingVideo) { try { job.Stdin.Write(v, 0, v.Length); } catch { } }
            try { job.Stdin.Flush(); } catch { }
            job.FramesIn += job.PendingVideo.Count;
            job.PendingVideo.Clear();
            if (!withAudio) job.PendingAudio.Clear();
        }
    }

    // ffmpeg exited unexpectedly: retry once video-only (audio is best-effort).
    private void HandleExit(Job job)
    {
        lock (job.Gate)
        {
            if (job.State != JobState.Running) return;

            int code = -1;
            try { code = job.Proc?.ExitCode ?? -1; } catch { }
            try { job.Stdin?.Close(); } catch { }
            try { job.AudioStream?.Close(); } catch { }
            try { job.AudioListener?.Stop(); } catch { }
            job.AudioStream = null; job.AudioListener = null; job.Proc = null; job.Stdin = null;

            if (job.WithAudio && !job.TriedVideoOnly)
            {
                job.TriedVideoOnly = true;
                job.ForceNoAudio = true;
                job.State = JobState.Buffering;
                _log.LogWarning("[FFMPEG] {k} exited (code {c}) with audio; retrying VIDEO-ONLY", job.Key, code);
                _ = Task.Run(async () => { await Task.Delay(700); StartJob(job); });
            }
            else
            {
                _log.LogWarning("[FFMPEG] {k} exited (code {c}); dropping", job.Key, code);
                StopLocked(job);
            }
        }
    }

    private void StopLocked(Job job)
    {
        if (job.State == JobState.Dead) return;
        job.State = JobState.Dead;
        try { job.AudioQueue?.Writer.TryComplete(); } catch { }
        try { job.Stdin?.Close(); } catch { }
        try { job.AudioStream?.Close(); } catch { }
        try { job.AudioListener?.Stop(); } catch { }
        try { if (job.Proc != null && !job.Proc.HasExited) job.Proc.Kill(true); } catch { }
        _jobs.TryRemove(job.Key, out _);
        _log.LogInformation("[FFMPEG] {k} stopped (video={v} audio={a})", job.Key, job.FramesIn, job.AudioIn);
    }
}
