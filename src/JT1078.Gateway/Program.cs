using JT1078.Gateway;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<StreamManager>();
var app = builder.Build();

int ingestPort = int.Parse(builder.Configuration["IngestPort"] ?? "2272");
var mgr = app.Services.GetRequiredService<StreamManager>();
new TcpIngest(mgr, ingestPort, app.Logger).Start();

app.UseDefaultFiles();   // serve wwwroot/index.html at /
app.UseStaticFiles();

// HTTP-FLV playback:  GET /live/{SIM}_{channel}.flv
app.MapGet("/live/{key}.flv", async (string key, HttpContext ctx, StreamManager m) =>
{
    ctx.Response.Headers["Content-Type"] = "video/x-flv";
    ctx.Response.Headers["Cache-Control"] = "no-cache";
    await m.Subscribe(key, ctx.Response.Body, ctx.RequestAborted);
});

// list active streams (handy for debugging the exact key)
app.MapGet("/streams", (StreamManager m) => Results.Json(m.ListKeys()));

app.Run();
