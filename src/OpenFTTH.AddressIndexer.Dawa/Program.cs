using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressIndexer.Dawa;

public sealed class Program
{
    public static async Task Main(string[] _)
    {
        using var host = HostConfig.Configure();
        var logger = host.Services.GetService<ILogger<Program>>();

        if (logger is null)
        {
            throw new InvalidOperationException("Logger has not been configured.");
        }

        try
        {
            await host.StartAsync().ConfigureAwait(false);
            await host.WaitForShutdownAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError("{}", ex.ToString());
            throw;
        }
    }
}
