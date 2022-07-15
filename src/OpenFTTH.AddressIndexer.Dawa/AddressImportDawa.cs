using DawaAddress;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressIndexer.Dawa;

internal sealed class AddressImportDawa : IAddressImport
{
    private readonly DawaClient _dawaClient;
    private readonly ILogger<AddressImportDawa> _logger;

    public AddressImportDawa(HttpClient httpClient, ILogger<AddressImportDawa> logger)
    {
        _dawaClient = new(httpClient);
        _logger = logger;
    }

    public async Task Full(CancellationToken cancellationToken = default)
    {
        var latestTransaction = await _dawaClient
            .GetLatestTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        var postCodesAsyncEnumerable = _dawaClient
            .GetAllPostCodesAsync(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        await foreach (var postCode in postCodesAsyncEnumerable)
        {
            _logger.LogInformation("{Number}:{Name},", postCode.Number, postCode.Name);
        }
    }
}
