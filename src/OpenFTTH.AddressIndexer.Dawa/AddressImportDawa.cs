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
                var error = ((RoadError)create.Errors.First());

                if (error.Code == RoadErrorCode.NAME_CANNOT_BE_WHITE_SPACE_OR_NULL)
                {
                    _logger.LogWarning(
                        "ExternalId: '{ExternalId}', {ErrorMessage}",
                        road.Id, error.Message);
                }
                else
                {
                    throw new InvalidOperationException(
                        create.Errors.FirstOrDefault()?.Message);
                }
            }
        }
    }
}
