using System.Net;
using System.Net.Sockets;
using JT1078.Protocol;
using JT1078.Protocol.Enums;

namespace JT1078.Gateway;

/// <summary>
/// TCP listener the camera pushes its JT/T 1078 stream to (after the platform
/// sends it 0x9101). Frames packets on the 0x30 0x31 0x63 0x64 marker, parses +
/// merges them into complete video frames, and feeds the raw H.265 Annex-B
/// elementary stream into FFmpeg (per stream key) to transcode -> H.264 HLS.
/// </summary>
public class TcpIngest
{
    private static readonly byte[] Magic = { 0x30, 0x31, 0x63, 0x64 };
    private readonly FfmpegTranscoder _ff;
    private readonly int _port;
    private readonly ILogger _log;

    public TcpIngest(FfmpegTranscoder ff, int port, ILogger log)
    {
        _ff = ff; _port = port; _log = log;
    }

    public void Start()
    {
        var listener = new TcpListener(IPAddress.Any, _port);
        listener.Start();
        _log.LogInformation("[INGEST] JT1078 ingest listening on TCP {p}", _port);
        _ = Task.Run(async () =>
        {
            while (true)
            {
                try
                {
                    var client = await listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClient(client));
                }
                catch (Exception ex) { _log.LogError("[INGEST] accept error: {m}", ex.Message); }
            }
        });
    }

    private async Task HandleClient(TcpClient client)
    {
        var remote = client.Client.RemoteEndPoint?.ToString();
        _log.LogInformation("[INGEST] camera connected {r}", remote);
        var streamKeys = new HashSet<string>();   // keys this connection produced (to stop on disconnect)
        var buffer = new List<byte>();
        var readBuf = new byte[65536];

        // ── diagnostics ──────────────────────────────────────────────────────
        long totalBytes = 0; int totalPkts = 0, mergeNull = 0, videoPkts = 0,
            audioPkts = 0, errors = 0; bool firstChunkLogged = false;
        string lastErr = ""; var dataTypes = new HashSet<string>(); var subTypes = new HashSet<string>();
        var stats = new IngestStats(() =>
            _log.LogInformation("[DIAG] {r} bytes={b} pkts={p} video={v} audio={a} mergeNull={m} errors={e} dataTypes=[{dt}] subTypes=[{st}] keys=[{k}] lastErr={le}",
                remote, totalBytes, totalPkts, videoPkts, audioPkts, mergeNull, errors,
                string.Join(",", dataTypes), string.Join(",", subTypes), string.Join(",", streamKeys), lastErr));

        try
        {
            using var ns = client.GetStream();
            int n;
            while ((n = await ns.ReadAsync(readBuf, 0, readBuf.Length)) > 0)
            {
                totalBytes += n;
                if (!firstChunkLogged)
                {
                    int show = Math.Min(n, 64);
                    _log.LogInformation("[DIAG] {r} FIRST {s} bytes: {hex}", remote, show, Convert.ToHexString(readBuf, 0, show));
                    firstChunkLogged = true;
                }
                for (int i = 0; i < n; i++) buffer.Add(readBuf[i]);
                Process(buffer, streamKeys, remote,
                    ref totalPkts, ref mergeNull, ref videoPkts, ref audioPkts, ref errors, ref lastErr, dataTypes, subTypes);
                stats.MaybeLog(totalPkts, totalBytes);
            }
        }
        catch (Exception ex) { _log.LogWarning("[INGEST] {r} error: {m}", remote, ex.Message); }
        finally
        {
            client.Close();
            _ff.StopAll(streamKeys);
            _log.LogInformation("[INGEST] camera disconnected {r} | bytes={b} pkts={p} video={v} audio={a} mergeNull={m} errors={e} dataTypes=[{dt}] keys=[{k}] lastErr={le}",
                remote, totalBytes, totalPkts, videoPkts, audioPkts, mergeNull, errors, string.Join(",", dataTypes), string.Join(",", streamKeys), lastErr);
        }
    }

    private void Process(List<byte> buf, HashSet<string> streamKeys, string remote,
        ref int totalPkts, ref int mergeNull, ref int videoPkts, ref int audioPkts, ref int errors, ref string lastErr,
        HashSet<string> dataTypes, HashSet<string> subTypes)
    {
        while (true)
        {
            int start = IndexOf(buf, 0);
            if (start < 0) { if (buf.Count > 2_000_000) buf.Clear(); return; }
            int next = IndexOf(buf, start + 4);
            if (next < 0)
            {
                if (start > 0) buf.RemoveRange(0, start);
                return;
            }
            var pkt = buf.GetRange(start, next - start).ToArray();
            buf.RemoveRange(0, next);
            totalPkts++;
            Handle(pkt, streamKeys, remote, ref mergeNull, ref videoPkts, ref audioPkts, ref errors, ref lastErr, dataTypes, subTypes);
        }
    }

    private void Handle(byte[] bytes, HashSet<string> streamKeys, string remote,
        ref int mergeNull, ref int videoPkts, ref int audioPkts, ref int errors, ref string lastErr,
        HashSet<string> dataTypes, HashSet<string> subTypes)
    {
        try
        {
            var package = JT1078Serializer.Deserialize(bytes);
            if (dataTypes.Count < 12) dataTypes.Add(package.Label3.DataType.ToString());
            if (subTypes.Count < 12) subTypes.Add(package.Label3.SubpackageType.ToString());

            var full = JT1078Serializer.Merge(package, JT808ChannelType.Live);
            if (full == null) { mergeNull++; return; }

            if (full.Bodies == null || full.Bodies.Length == 0) return;
            string key = full.GetAVKey();

            // Audio frame -> feed FFmpeg's audio input (muxed as AAC into the HLS).
            if (full.Label3.DataType == JT1078DataType.音频帧)
            {
                audioPkts++;
                if (audioPkts == 1)
                    _log.LogInformation("[DIAG] {r} FIRST audio frame {n}B: {hex}", remote,
                        Math.Min(full.Bodies.Length, 32), Convert.ToHexString(full.Bodies, 0, Math.Min(full.Bodies.Length, 32)));
                _ff.FeedAudio(key, full.Bodies);
                return;
            }

            videoPkts++;
            if (streamKeys.Add(key))
                _log.LogInformation("[INGEST] LIVE stream key={k} from {r} -> transcoding H.265->H.264 HLS", key, remote);

            _ff.Feed(key, full.Bodies);
        }
        catch (Exception ex) { errors++; lastErr = ex.Message; }
    }

    private static int IndexOf(List<byte> buf, int from)
    {
        for (int i = from; i <= buf.Count - 4; i++)
            if (buf[i] == Magic[0] && buf[i + 1] == Magic[1] && buf[i + 2] == Magic[2] && buf[i + 3] == Magic[3])
                return i;
        return -1;
    }
}

/// <summary>Logs a diagnostic summary at most once every ~100 packets.</summary>
public class IngestStats
{
    private readonly Action _log;
    private int _lastPkts;
    public IngestStats(Action log) { _log = log; }
    public void MaybeLog(int totalPkts, long totalBytes)
    {
        if (totalPkts - _lastPkts >= 100) { _lastPkts = totalPkts; _log(); }
    }
}
