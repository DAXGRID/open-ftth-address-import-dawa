using DawaAddress;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressIndexer.Dawa;

internal sealed class AddressIndexerHost : BackgroundService
{
    private readonly ILogger<AddressIndexerHost> _logger;
    private readonly Settings _settings;
    private readonly HttpClient _httpClient;

    public AddressIndexerHost(
        ILogger<AddressIndexerHost> logger,
        Settings settings,
        HttpClient httpClinet)
    {
        _logger = logger;
        _settings = settings;
        _httpClient = httpClinet;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting {}.", nameof(AddressIndexerHost));

        var dawaClient = new DawaClient(_httpClient);

        var latestTransaction = await dawaClient
            .GetLatestTransactionAsync(stoppingToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Received latest transaction with id: '{LatestTransactionId}'",
            latestTransaction.Id);

        var postCodesAsyncEnumerable = dawaClient
            .GetAllPostCodesAsync(latestTransaction.Id, stoppingToken)
            .ConfigureAwait(false);

        await foreach (var x in postCodesAsyncEnumerable)
        {
            _logger.LogInformation("{Number}:{Name},", x.Number, x.Name);
        }
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Stopping {}", nameof(AddressIndexerHost));
        await base.StopAsync(stoppingToken).ConfigureAwait(false);
    }
}
