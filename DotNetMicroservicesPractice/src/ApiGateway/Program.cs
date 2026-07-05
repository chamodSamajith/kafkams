using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddReverseProxy()
    .LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"));

var app = builder.Build();

app.UseHttpMetrics();

app.MapReverseProxy();

app.MapMetrics();

app.Run();