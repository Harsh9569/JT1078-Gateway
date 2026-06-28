using System.Text.RegularExpressions;
using JT1078.Gateway;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

int ingestPort = int.Parse(builder.Configuration["IngestPort"] ?? "2272");
string ffmpegPath = builder.Configuration["FfmpegPath"] ?? "ffmpeg";

// audio: the camera's audio codec fed to ffmpeg. Most MDVRs use G.711A (alaw,
// 8 kHz mono). If the [DIAG] FIRST audio frame shows a different codec, change
// AudioFormat (e.g. "mulaw") / AudioRate in appsettings.json.
bool audioEnabled = !string.Equals(builder.Configuration["Audio"], "false", StringComparison.OrdinalIgnoreCase);
string audioFmt = builder.Configuration["AudioFormat"] ?? "alaw";
int audioRate = int.Parse(builder.Configuration["AudioRate"] ?? "8000");

// HLS output lives OUTSIDE wwwroot and is served by the custom /live route below
// with shared read, so the static-file middleware never collides with ffmpeg
// writing the playlist/segments (Windows sharing violation).
string hlsDir = Path.Combine(builder.Environment.ContentRootPath, "hls");
Directory.CreateDirectory(hlsDir);

var transcoder = new FfmpegTranscoder(ffmpegPath, hlsDir, app.Logger, audioEnabled, audioFmt, audioRate);
new TcpIngest(transcoder, ingestPort, app.Logger).Start();

app.UseDefaultFiles();   // serve wwwroot/index.html at /
app.UseStaticFiles();

// HLS playlist + segments — read with FileShare so ffmpeg can keep writing.
app.MapGet("/live/{file}", async (string file, HttpContext ctx) =>
{
    if (!Regex.IsMatch(file, @"^[A-Za-z0-9_]+\.(m3u8|ts)$")) return Results.BadRequest();
    string path = Path.Combine(hlsDir, file);
    if (!File.Exists(path)) return Results.NotFound();

    ctx.Response.ContentType = file.EndsWith(".m3u8") ? "application/vnd.apple.mpegurl" : "video/mp2t";
    ctx.Response.Headers["Cache-Control"] = "no-cache, no-store";
    ctx.Response.Headers["Access-Control-Allow-Origin"] = "*";

    for (int i = 0; i < 8; i++)
    {
        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            await fs.CopyToAsync(ctx.Response.Body);
            return Results.Empty;
        }
        catch (IOException) { await Task.Delay(25); }  // ffmpeg mid-write; retry briefly
    }
    return Results.StatusCode(503);
});

// list active (transcoding) stream keys
app.MapGet("/streams", () => Results.Json(transcoder.ActiveKeys()));

app.Run();
