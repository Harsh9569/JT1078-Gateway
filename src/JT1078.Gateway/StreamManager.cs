using System.Collections.Concurrent;
using System.Threading.Channels;

namespace JT1078.Gateway;

/// <summary>
/// Holds per-stream subscriber lists and the cached FLV header so new browser
/// viewers can join an in-progress live stream. One "stream" = one camera
/// channel, keyed by "{SIM}_{channel}".
/// </summary>
public class StreamManager
{
    private class Subscriber
    {
        public readonly Channel<byte[]> Queue =
            Channel.CreateBounded<byte[]>(new BoundedChannelOptions(2048)
            { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
    }

    private class LiveStream
    {
        public byte[] Header;   // flv header + script tag + first video tag (SPS/PPS)
        public readonly ConcurrentDictionary<Guid, Subscriber> Subs = new();
    }

    private readonly ConcurrentDictionary<string, LiveStream> _streams = new();

    public void SetHeader(string key, byte[] header)
    {
        var s = _streams.GetOrAdd(key, _ => new LiveStream());
        s.Header = header;
        foreach (var sub in s.Subs.Values) sub.Queue.Writer.TryWrite(header);
    }

    public void Push(string key, byte[] data)
    {
        var s = _streams.GetOrAdd(key, _ => new LiveStream());
        foreach (var sub in s.Subs.Values) sub.Queue.Writer.TryWrite(data);
    }

    public IEnumerable<string> ListKeys() => _streams.Keys;

    public async Task Subscribe(string key, Stream output, CancellationToken ct)
    {
        var s = _streams.GetOrAdd(key, _ => new LiveStream());
        var sub = new Subscriber();
        var id = Guid.NewGuid();
        s.Subs[id] = sub;
        try
        {
            if (s.Header != null)
            {
                await output.WriteAsync(s.Header, ct);
                await output.FlushAsync(ct);
            }
            while (!ct.IsCancellationRequested)
            {
                var data = await sub.Queue.Reader.ReadAsync(ct);
                await output.WriteAsync(data, ct);
                await output.FlushAsync(ct);
            }
        }
        catch { /* client disconnected */ }
        finally { s.Subs.TryRemove(id, out _); }
    }
}
