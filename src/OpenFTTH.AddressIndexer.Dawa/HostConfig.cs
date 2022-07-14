using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Text.Json;

namespace OpenFTTH.AddressIndexer.Dawa;

internal static class HostConfig
{
    public static IHost Configure()
    {
        var hostBuilder = new HostBuilder();
        ConfigureServices(hostBuilder);
        ConfigureLogging(hostBuilder);
        return hostBuilder.Build();
    }

    private static void ConfigureServices(IHostBuilder hostBuilder)
    {
        var settingsJson = JsonDocument.Parse(File.ReadAllText("appsettings.json"))
            .RootElement.GetProperty("settings").ToString();

        var settings = JsonSerializer.Deserialize<Settings>(settingsJson) ??
            throw new ArgumentException("Could not deserialize appsettings into settings.");

        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            services.AddHostedService<AddressIndexerHost>();
            services.AddSingleton<Settings>();
            services.AddHttpClient();
        });
    }

    private static void ConfigureLogging(IHostBuilder hostBuilder)
    {
        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            var loggingConfiguration = new ConfigurationBuilder()
               .AddEnvironmentVariables().Build();

            services.AddLogging(loggingBuilder =>
            {
                var logger = new LoggerConfiguration()
                    .ReadFrom.Configuration(loggingConfiguration)
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("System", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console(new RenderedCompactJsonFormatter())
                    .CreateLogger();

                loggingBuilder.AddSerilog(logger, true);
            });
        });
    }
}
