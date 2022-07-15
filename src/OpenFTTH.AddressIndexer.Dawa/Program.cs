using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Text.Json;

namespace OpenFTTH.AddressIndexer.Dawa;

public sealed class Program
{
    public static async Task Main(string[] _)
    {
        using var token = new CancellationTokenSource();
        var serviceProvider = BuildServiceProvider();
        var startup = serviceProvider.GetService<Startup>();
        var logger = serviceProvider.GetService<ILogger<Program>>();

        if (startup is null)
        {
            throw new InvalidOperationException($"{nameof(Startup)} has not been configured.");
        }

        if (logger is null)
        {
            throw new InvalidOperationException("Logger has not been configured.");
        }

        try
        {
            await startup.Start().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError("{}", ex.ToString());
            throw;
        }
    }

    private static ServiceProvider BuildServiceProvider()
    {
        var logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
            .MinimumLevel.Override("System", LogEventLevel.Warning)
            .Enrich.FromLogContext()
            .WriteTo.Console(new CompactJsonFormatter())
            .CreateLogger();

        var settingsJson = JsonDocument.Parse(File.ReadAllText("appsettings.json"))
            .RootElement.GetProperty("settings").ToString();

        var settings = JsonSerializer.Deserialize<Settings>(settingsJson) ??
            throw new ArgumentException("Could not deserialize appsettings into settings.");

        return new ServiceCollection()
            .AddSingleton<Settings>(settings)
            .AddSingleton<IAddressImport, AddressImportDawa>()
            .AddLogging(logging =>
            {
                logging.AddSerilog(logger, true);
            })
            .AddSingleton<Startup>()
            .AddHttpClient()
            .BuildServiceProvider();
    }
}
