using JT1078.Gateway;
using Microsoft.AspNetCore.StaticFiles;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

int ingestPort = int.Parse(builder.Configuration["IngestPort"] ?? "2272");
string ffmpegPath = builder.Configuration["FfmpegPath"] ?? "ffmpeg";

// HLS output lives under wwwroot/live so the static file middleware serves it.
string webRoot = app.Environment.WebRootPath
                 ?? Path.Combine(AppContext.BaseDirectory, "wwwroot");
string hlsDir = Path.Combine(webRoot, "live");

// audio: the camera's audio codec fed to ffmpeg. Most MDVRs use G.711A (alaw,
// 8 kHz mono). If the [DIAG] FIRST audio frame shows a different codec, change
// AudioFormat (e.g. "mulaw") / AudioRate in appsettings.json.
bool audioEnabled = !string.Equals(builder.Configuration["Audio"], "false", StringComparison.OrdinalIgnoreCase);
string audioFmt = builder.Configuration["AudioFormat"] ?? "alaw";
int audioRate = int.Parse(builder.Configuration["AudioRate"] ?? "8000");

var transcoder = new FfmpegTranscoder(ffmpegPath, hlsDir, app.Logger, audioEnabled, audioFmt, audioRate);
new TcpIngest(transcoder, ingestPort, app.Logger).Start();

// serve .m3u8 / .ts with the right content types (not in the default map)
var contentTypes = new FileExtensionContentTypeProvider();
contentTypes.Mappings[".m3u8"] = "application/vnd.apple.mpegurl";
contentTypes.Mappings[".ts"] = "video/mp2t";

app.UseDefaultFiles();   // serve wwwroot/index.html at /
app.UseStaticFiles(new StaticFileOptions
{
    ContentTypeProvider = contentTypes,
    OnPrepareResponse = ctx =>
    {
        // playlists must never be cached; segments are fine to cache briefly
        if (ctx.File.Name.EndsWith(".m3u8"))
            ctx.Context.Response.Headers["Cache-Control"] = "no-cache, no-store";
        ctx.Context.Response.Headers["Access-Control-Allow-Origin"] = "*";
    }
});

// list active (transcoding) stream keys — handy for finding the exact key
app.MapGet("/streams", () => Results.Json(transcoder.ActiveKeys()));

app.Run();
