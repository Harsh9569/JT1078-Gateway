using System.Collections.Concurrent;
using System.Diagnostics;

namespace JT1078.Gateway;

/// <summary>
/// Manages one FFmpeg process per live stream key. The camera's video codec is
/// H.265 (a fixed terminal attribute — the JT/T 808/1078 protocol has no command
/// to switch it to H.264), and browsers can't reliably play raw H.265, so we feed
/// the raw H.265 Annex-B elementary stream into FFmpeg, transcode to H.264, and
/// segment it as HLS into wwwroot/live/{key}.m3u8 (+ .ts). The browser plays it
/// with hls.js. One "stream" = one camera channel, keyed by "{SIM}_{channel}".
/// </summary>
public class FfmpegTranscoder
{
    private class Job
    {
        public Process Proc;
        public Stream Stdin;
        public string Key;
        public long FramesIn;
    }

    private readonly ConcurrentDictionary<string, Job> _jobs = new();
    private readonly string _ffmpeg;
    private readonly string _outDir;
    private readonly ILogger _log;

    public FfmpegTranscoder(string ffmpegPath, string outDir, ILogger log)
    {
        _ffmpeg = ffmpegPath;
        _outDir = outDir;
        _log = log;
        Directory.CreateDirectory(_outDir);
    }

    public IEnumerable<string> ActiveKeys() => _jobs.Keys;

    /// <summary>Feed one complete H.265 Annex-B video frame for the given stream.</summary>
    public void Feed(string key, byte[] annexB)
    {
        var job = _jobs.GetOrAdd(key, StartJob);
        if (job == null) return;
        try
        {
            job.Stdin.Write(annexB, 0, annexB.Length);
            job.Stdin.Flush();
            job.FramesIn++;
        }
        catch (Exception ex)
        {
            _log.LogWarning("[FFMPEG] {k} write failed ({m}); restarting", key, ex.Message);
            Stop(key);
        }
    }

    public void Stop(string key)
    {
        if (_jobs.TryRemove(key, out var job))
        {
            try { job.Stdin?.Close(); } catch { }
            try { if (!job.Proc.HasExited) job.Proc.Kill(true); } catch { }
            _log.LogInformation("[FFMPEG] {k} stopped (framesIn={f})", key, job.FramesIn);
        }
    }

    public void StopAll(IEnumerable<string> keys)
    {
        foreach (var k in keys.ToArray()) Stop(k);
    }

    private Job StartJob(string key)
    {
        // sanitise key for a filename
        string safe = key.Replace("/", "_").Replace("\\", "_").Replace("..", "_");
        string m3u8 = Path.Combine(_outDir, safe + ".m3u8");
        string seg = Path.Combine(_outDir, safe + "_%03d.ts");

        // -f hevc      : input is a raw H.265 Annex-B elementary stream (00 00 00 01 NALUs)
        // -framerate   : raw stream carries no timestamps; assume 25 fps (refine later)
        // libx264 ultrafast/zerolatency : cheap, low-latency transcode
        // HLS: 1s segments, short rolling window, delete old segments
        string args =
            "-hide_banner -loglevel warning -fflags +genpts " +
            "-framerate 25 -f hevc -i pipe:0 " +
            "-an -c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -g 25 " +
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
            _log.LogError("[FFMPEG] failed to start '{f}' for {k}: {m}. Is FFmpeg installed / on PATH?",
                _ffmpeg, key, ex.Message);
            return null;
        }

        var job = new Job { Proc = proc, Stdin = proc.StandardInput.BaseStream, Key = key };

        // drain ffmpeg stderr to the log so we can see codec/encode errors
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

        _log.LogInformation("[FFMPEG] {k} started -> {m3u8}", key, m3u8);
        return job;
    }
}
