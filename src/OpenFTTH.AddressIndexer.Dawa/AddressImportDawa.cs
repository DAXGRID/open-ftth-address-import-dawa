using DawaAddress;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa;

internal sealed class AddressImportDawa : IAddressImport
{
    private readonly DawaClient _dawaClient;
    private readonly ILogger<AddressImportDawa> _logger;
    private readonly IEventStore _eventStore;

    public AddressImportDawa(
        HttpClient httpClient,
        ILogger<AddressImportDawa> logger,
        IEventStore eventStore)
    {
        _dawaClient = new(httpClient);
        _logger = logger;
        _eventStore = eventStore;
    }

    public Task Changes(ulong lastTransactionId, CancellationToken cancellation = default)
    {
        throw new NotImplementedException();
    }

    public async Task Full(CancellationToken cancellationToken = default)
    {
        var latestTransaction = await _dawaClient
            .GetLatestTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Starting full import of post codes.");
        await FullImportPostCodes(
            latestTransaction, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Starting full import of roads.");
        await FullImportRoads(latestTransaction, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task FullImportRoads(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var roadsAsyncEnumerable = _dawaClient
            .GetAllRoadsAsync(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        await foreach (var road in roadsAsyncEnumerable)
        {
            var roadAR = new RoadAR();
            var create = roadAR.Create(
                id: Guid.NewGuid(),
                externalId: road.Id,
                name: road.Name);

            if (create.IsSuccess)
            {
                _eventStore.Aggregates.Store(roadAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }
    }

    private async Task FullImportPostCodes(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var postCodesAsyncEnumerable = _dawaClient
            .GetAllPostCodesAsync(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        await foreach (var postCode in postCodesAsyncEnumerable)
        {
            var postCodeAR = new PostCodeAR();
            var create = postCodeAR.Create(
                id: Guid.NewGuid(),
                number: postCode.Number,
                name: postCode.Name);

            if (create.IsSuccess)
            {
                _eventStore.Aggregates.Store(postCodeAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }
    }
}
