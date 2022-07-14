using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressIndexer.Dawa;

internal sealed class AddressIndexerHost : BackgroundService
{
    private readonly ILogger<AddressIndexerHost> _logger;
    private readonly Settings _settings;

    public AddressIndexerHost(
        ILogger<AddressIndexerHost> logger,
        Settings settings)
    {
        _logger = logger;
        _settings = settings;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(AddressIndexerHost)}.");
        return Task.CompletedTask;
    }
}
