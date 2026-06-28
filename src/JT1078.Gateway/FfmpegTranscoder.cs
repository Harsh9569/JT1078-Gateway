using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace JT1078.Gateway;

/// <summary>
/// One FFmpeg process per live stream key. The camera's video is H.265 (fixed in
/// firmware), which browsers can't reliably play, so we transcode H.265 -> H.264
/// and segment to HLS. When the camera also sends audio (requested via dataType=0)
/// we feed that audio to the SAME FFmpeg over a local UDP input and mux it as AAC,
/// so the browser gets video WITH sound.
///
/// To avoid stalling video when a camera sends no audio, each job buffers briefly
/// (DecideMs) to learn whether audio is present, then starts FFmpeg with or without
/// the audio input. Stream key = "{SIM}_{channel}".
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
        public UdpClient AudioOut;
        public int AudioPort;
        public bool WithAudio;
        public bool SawAudio;
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
    private readonly string _audioFmt;   // ffmpeg input demuxer for the camera audio: alaw|mulaw|...
    private readonly int _audioRate;     // input sample rate (G.711 = 8000)
    private readonly ILogger _log;
    private int _portCounter = 41000;
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

    /// <summary>Feed one complete H.265 Annex-B video frame.</summary>
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

    /// <summary>Feed one complete audio frame (raw codec bytes, e.g. G.711A).</summary>
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
            if (job.State != JobState.Running || !job.WithAudio || job.AudioOut == null) return;
            try { job.AudioOut.Send(audio, audio.Length); job.AudioIn++; }
            catch { /* udp best-effort */ }
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
            var job = new Job { Key = k, AudioPort = NextPort() };
            _log.LogInformation("[FFMPEG] {k} buffering {ms}ms to detect audio (audioEnabled={ae})", k, DecideMs, _audioEnabled);
            _ = Task.Run(async () => { await Task.Delay(DecideMs); StartJob(job); });
            return job;
        });
    }

    private int NextPort()
    {
        int p = Interlocked.Increment(ref _portCounter);
        if (p > 41999) { Interlocked.Exchange(ref _portCounter, 41000); p = 41000; }
        return p;
    }

    private void StartJob(Job job)
    {
        lock (job.Gate)
        {
            if (job.State != JobState.Buffering) return;   // stopped before we could start
            bool withAudio = _audioEnabled && job.SawAudio;
            job.WithAudio = withAudio;

            string safe = job.Key.Replace("/", "_").Replace("\\", "_").Replace("..", "_");
            string m3u8 = Path.Combine(_outDir, safe + ".m3u8");
            string seg = Path.Combine(_outDir, safe + "_%03d.ts");

            string videoIn = "-fflags +genpts -framerate 25 -f hevc -i pipe:0 ";
            string audioIn = withAudio
                ? $"-thread_queue_size 1024 -f {_audioFmt} -ar {_audioRate} -ac 1 -i \"udp://127.0.0.1:{job.AudioPort}?overrun_nonfatal=1&fifo_size=1000000\" "
                : "";
            string maps = withAudio
                ? "-map 0:v:0 -map 1:a:0 -c:a aac -b:a 64k -ar 44100 "
                : "-an ";

            string args =
                "-hide_banner -loglevel warning " + videoIn + audioIn +
                "-c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -g 25 " + maps +
                "-f hls -hls_time 1 -hls_list_size 4 " +
                "-hls_flags delete_segments+append_list+omit_endlist+independent_segments " +
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
                job.State = JobState.Dead;
                _jobs.TryRemove(job.Key, out _);
                return;
            }

            job.Proc = proc;
            job.Stdin = proc.StandardInput.BaseStream;
            job.State = JobState.Running;

            if (withAudio)
            {
                try { job.AudioOut = new UdpClient(); job.AudioOut.Connect(IPAddress.Loopback, job.AudioPort); }
                catch (Exception ex) { _log.LogWarning("[FFMPEG] {k} audio udp init failed: {m}", job.Key, ex.Message); }
            }

            // drain ffmpeg stderr to the log (codec / encode errors land here)
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

            _log.LogInformation("[FFMPEG] {k} started withAudio={a} (in {f}/{r}Hz) -> {m3u8}",
                job.Key, withAudio, _audioFmt, _audioRate, m3u8);

            // flush what we buffered during the detect window
            foreach (var v in job.PendingVideo) { try { job.Stdin.Write(v, 0, v.Length); } catch { } }
            try { job.Stdin.Flush(); } catch { }
            job.FramesIn += job.PendingVideo.Count;
            job.PendingVideo.Clear();

            if (withAudio && job.AudioOut != null)
            {
                foreach (var a in job.PendingAudio) { try { job.AudioOut.Send(a, a.Length); } catch { } }
                job.AudioIn += job.PendingAudio.Count;
            }
            job.PendingAudio.Clear();
        }
    }

    private void StopLocked(Job job)
    {
        if (job.State == JobState.Dead) return;
        job.State = JobState.Dead;
        try { job.Stdin?.Close(); } catch { }
        try { job.AudioOut?.Close(); } catch { }
        try { if (job.Proc != null && !job.Proc.HasExited) job.Proc.Kill(true); } catch { }
        _jobs.TryRemove(job.Key, out _);
        _log.LogInformation("[FFMPEG] {k} stopped (video={v} audio={a})", job.Key, job.FramesIn, job.AudioIn);
    }
}
