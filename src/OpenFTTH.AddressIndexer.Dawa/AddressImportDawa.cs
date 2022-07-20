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

        _logger.LogInformation("Starting full import of access addresses.");
        await FullImportAccessAdress(latestTransaction, cancellationToken)
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
                externalId: road.Id.ToString(),
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

    private async Task FullImportAccessAdress(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var accessAddressesAsyncEnumerable = _dawaClient
            .GetAllAccessAddresses(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        var existingPostCodeIds = addressProjection.PostCodeIds;
        var existingRoadIds = addressProjection.RoadIds;

        await foreach (var accessAddress in accessAddressesAsyncEnumerable)
        {
            var accessAddressAR = new AccessAddressAR();
            if (!addressProjection.PostCodeNumberToId.TryGetValue(
                    accessAddress.PostDistrictCode, out var postCodeId))
            {
                _logger.LogWarning(
                    "Could not find id using external post district code: '{PostDistrictCode}'.",
                    accessAddress.PostDistrictCode);
                continue;
            }

            if (!addressProjection.RoadExternalIdToId.TryGetValue(
                    accessAddress.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using external roadId code: '{RoadId}'.",
                    accessAddress.RoadId);
                continue;
            }

            var create = accessAddressAR.Create(
                id: Guid.NewGuid(),
                officialId: accessAddress.Id,
                created: accessAddress.Created,
                updated: accessAddress.Updated,
                municipalCode: accessAddress.MunicipalCode,
                status: MapDawaStatus(accessAddress.Status),
                roadCode: accessAddress.RoadCode,
                houseNumber: accessAddress.HouseNumber,
                postCodeId: postCodeId,
                eastCoordinate: accessAddress.EastCoordinate,
                northCoordinate: accessAddress.NorthCoordinate,
                locationUpdated: accessAddress.LocationUpdated,
                supplementaryTownName: accessAddress.SupplementaryTownName,
                plotId: accessAddress.PlotId,
                roadId: roadId,
                existingRoadIds: existingRoadIds,
                existingPostCodeIds: existingPostCodeIds);

            if (create.IsSuccess)
            {
                _eventStore.Aggregates.Store(accessAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }
    }

    private static Status MapDawaStatus(DawaStatus status)
        => status switch
        {
            DawaStatus.Active => Status.Active,
            DawaStatus.Canceled => Status.Canceled,
            DawaStatus.Discontinued => Status.Discontinued,
            DawaStatus.Pending => Status.Pending,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };
}
