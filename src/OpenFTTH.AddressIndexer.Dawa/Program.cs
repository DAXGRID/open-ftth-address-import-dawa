using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using OpenFTTH.EventSourcing.Postgres;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Reflection;
using System.Text.Json;

namespace OpenFTTH.AddressIndexer.Dawa;

public sealed class Program
{
    public static async Task Main(string[] _)
    {
        using var token = new CancellationTokenSource();
        var serviceProvider = BuildServiceProvider();
        var startup = serviceProvider.GetService<ImportStarter>();
        var logger = serviceProvider.GetService<ILogger<Program>>();

        if (startup is null)
        {
            throw new InvalidOperationException(
                $"{nameof(ImportStarter)} has not been configured.");
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
            .AddSingleton<ImportStarter>()
            .AddSingleton<Settings>(settings)
            .AddSingleton<IAddressImport, AddressImportDawa>()
            .AddSingleton<ITransactionStore, PostgresTransactionStore>()
            .AddSingleton<IEventStore>(
                e =>
                new PostgresEventStore(
                    serviceProvider: e.GetRequiredService<IServiceProvider>(),
                    connectionString: settings.EventStoreConnectionString,
                    databaseSchemaName: "events"))
            .AddProjections(new Assembly[]
            {
                AppDomain.CurrentDomain.Load("OpenFTTH.Core.Address")
            })
            .AddHttpClient()
            .AddLogging(logging =>
            {
                logging.AddSerilog(logger, true);
            })
            .BuildServiceProvider();
    }
}
