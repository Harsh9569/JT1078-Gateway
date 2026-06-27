using System.Net;
using System.Net.Sockets;
using JT1078.Protocol;
using JT1078.Protocol.Enums;
using JT1078.Flv;

namespace JT1078.Gateway;

/// <summary>
/// TCP listener the camera pushes its JT/T 1078 stream to (after the platform
/// sends it 0x9101). Frames packets on the 0x30 0x31 0x63 0x64 marker, parses +
/// merges them, encodes to FLV, and hands the FLV tags to the StreamManager.
/// </summary>
public class TcpIngest
{
    private static readonly byte[] Magic = { 0x30, 0x31, 0x63, 0x64 };
    private readonly StreamManager _mgr;
    private readonly int _port;
    private readonly ILogger _log;

    public TcpIngest(StreamManager mgr, int port, ILogger log)
    {
        _mgr = mgr; _port = port; _log = log;
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
        var encoders = new Dictionary<string, FlvEncoder>();
        var headered = new HashSet<string>();
        var buffer = new List<byte>();
        var readBuf = new byte[65536];
        try
        {
            using var ns = client.GetStream();
            int n;
            while ((n = await ns.ReadAsync(readBuf, 0, readBuf.Length)) > 0)
            {
                for (int i = 0; i < n; i++) buffer.Add(readBuf[i]);
                Process(buffer, encoders, headered, remote);
            }
        }
        catch (Exception ex) { _log.LogWarning("[INGEST] {r} error: {m}", remote, ex.Message); }
        finally { client.Close(); _log.LogInformation("[INGEST] camera disconnected {r}", remote); }
    }

    private void Process(List<byte> buf, Dictionary<string, FlvEncoder> encoders, HashSet<string> headered, string remote)
    {
        while (true)
        {
            int start = IndexOf(buf, 0);
            if (start < 0) { if (buf.Count > 2_000_000) buf.Clear(); return; }
            int next = IndexOf(buf, start + 4);
            if (next < 0)
            {
                if (start > 0) buf.RemoveRange(0, start); // drop junk before first marker
                return;                                   // wait for the next packet's marker
            }
            var pkt = buf.GetRange(start, next - start).ToArray();
            buf.RemoveRange(0, next);
            Handle(pkt, encoders, headered, remote);
        }
    }

    private void Handle(byte[] bytes, Dictionary<string, FlvEncoder> encoders, HashSet<string> headered, string remote)
    {
        try
        {
            var package = JT1078Serializer.Deserialize(bytes);
            var full = JT1078Serializer.Merge(package, JT808ChannelType.Live);
            if (full == null) return;                              // still accumulating sub-packages
            if (full.Label3.DataType == JT1078DataType.音频帧) return; // skip audio for v1
            string key = full.GetAVKey();                          // "{SIM}_{channel}"
            if (!encoders.TryGetValue(key, out var enc)) { enc = new FlvEncoder(); encoders[key] = enc; }

            if (!headered.Contains(key))
            {
                var header = enc.EncoderVideoTag(full, true);      // null until an I-frame (SPS) arrives
                if (header != null)
                {
                    _mgr.SetHeader(key, header);
                    headered.Add(key);
                    _log.LogInformation("[INGEST] LIVE stream key={k} from {r}", key, remote);
                }
            }
            else
            {
                var tag = enc.EncoderVideoTag(full, false);
                if (tag != null) _mgr.Push(key, tag);
            }
        }
        catch (Exception ex) { _log.LogDebug("[INGEST] packet parse error: {m}", ex.Message); }
    }

    private static int IndexOf(List<byte> buf, int from)
    {
        for (int i = from; i <= buf.Count - 4; i++)
            if (buf[i] == Magic[0] && buf[i + 1] == Magic[1] && buf[i + 2] == Magic[2] && buf[i + 3] == Magic[3])
                return i;
        return -1;
    }
}
