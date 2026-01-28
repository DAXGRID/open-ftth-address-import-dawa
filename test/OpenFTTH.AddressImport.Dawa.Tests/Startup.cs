using Microsoft.Extensions.DependencyInjection;
using OpenFTTH.EventSourcing;
using OpenFTTH.EventSourcing.Postgres;
using Serilog;
using Serilog.Events;
using System.Reflection;

namespace OpenFTTH.AddressImport.Dawa.Tests;

public static class Startup
{
    public static void ConfigureServices(IServiceCollection services)
    {
        services.AddLogging(logging =>
        {
            var logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("System", LogEventLevel.Warning)
                .Enrich.FromLogContext()
                .WriteTo.Console(formatProvider: null)
                .CreateLogger();

            logging.AddSerilog(logger, true);
        });

        services.AddHttpClient();
        services.AddSingleton(new AddressImportSettings(DatabaseFixture.TestDbConnectionString, "", false));
        services.AddSingleton<IAddressFullImport, AddressFullImportDawa>();
        services.AddSingleton<IAddressChangesImport, AddressChangesImportDawa>();
        services.AddSingleton<IEventStore>(
            e =>
            new PostgresEventStore(
                serviceProvider: e.GetRequiredService<IServiceProvider>(),
                connectionString: DatabaseFixture.TestDbConnectionString,
                databaseSchemaName: "events"));

        services.AddSingleton<ITransactionStore, PostgresTransactionStore>();

        services.AddProjections(new Assembly[]
        {
            AppDomain.CurrentDomain.Load("OpenFTTH.Core.Address")
        });
    }
}
