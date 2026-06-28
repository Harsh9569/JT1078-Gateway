using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

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
        public bool WithAudio;
        public bool SawAudio;
        public bool ForceNoAudio;      // set when we fall back to video-only
        public bool TriedVideoOnly;
        public string Key;
        public long FramesIn;
        public long AudioIn;
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
            if (job.State != JobState.Running || !job.WithAudio || job.AudioStream == null) return;
            try { job.AudioStream.Write(audio, 0, audio.Length); job.AudioIn++; }
            catch { /* best-effort */ }
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
                }
                catch (Exception ex)
                {
                    _log.LogWarning("[FFMPEG] {k} audio listener failed ({m}); video-only", job.Key, ex.Message);
                    withAudio = false; job.WithAudio = false;
                }
            }

            string videoIn = "-fflags +genpts -framerate 25 -f hevc -i pipe:0 ";
            string audioIn = withAudio
                ? $"-thread_queue_size 1024 -f {_audioFmt} -ar {_audioRate} -ac 1 -i tcp://127.0.0.1:{audioPort} "
                : "";
            // The camera's real frame rate (~15) is below the -framerate 25 we feed,
            // so the video PTS clock runs slower than the real-time audio clock and
            // the streams diverge. Without help the HLS muxer holds packets waiting
            // to interleave and STOPS cutting segments (the classic "1 segment then
            // freeze" with audio). -max_interleave_delta 0 makes it flush instead of
            // wait; aresample async lets the audio absorb the drift so it stays in
            // sync without stalling.
            string maps = withAudio
                ? "-map 0:v:0 -map 1:a:0 -c:a aac -b:a 64k -ar 44100 -filter:a aresample=async=1:min_hard_comp=0.100:first_pts=0 "
                : "-map 0:v:0 -an ";

            string args =
                "-hide_banner -loglevel warning " + videoIn + audioIn +
                "-c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -g 25 " +
                "-max_interleave_delta 0 -muxpreload 0 -muxdelay 0 " + maps +
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

            // accept ffmpeg's audio connection, then flush pending + stream live
            if (withAudio && job.AudioListener != null)
            {
                var listener = job.AudioListener;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var client = await listener.AcceptTcpClientAsync();
                        client.NoDelay = true;
                        var stream = client.GetStream();
                        lock (job.Gate)
                        {
                            if (job.State != JobState.Running) { try { stream.Dispose(); } catch { } return; }
                            job.AudioStream = stream;
                            foreach (var a in job.PendingAudio) { try { stream.Write(a, 0, a.Length); } catch { } }
                            job.AudioIn += job.PendingAudio.Count;
                            job.PendingAudio.Clear();
                        }
                    }
                    catch { /* listener stopped / ffmpeg never connected */ }
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

            _log.LogInformation("[FFMPEG] {k} started withAudio={a} (in {f}/{r}Hz, tcp:{p}) -> {m3u8}",
                job.Key, withAudio, _audioFmt, _audioRate, audioPort, m3u8);

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
        try { job.Stdin?.Close(); } catch { }
        try { job.AudioStream?.Close(); } catch { }
        try { job.AudioListener?.Stop(); } catch { }
        try { if (job.Proc != null && !job.Proc.HasExited) job.Proc.Kill(true); } catch { }
        _jobs.TryRemove(job.Key, out _);
        _log.LogInformation("[FFMPEG] {k} stopped (video={v} audio={a})", job.Key, job.FramesIn, job.AudioIn);
    }
}
