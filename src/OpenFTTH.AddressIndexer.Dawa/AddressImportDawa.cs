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
        var dawaRoadsAsyncEnumerable = _dawaClient
            .GetAllRoadsAsync(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        var count = 0;
        await foreach (var dawaRoad in dawaRoadsAsyncEnumerable)
        {
            var roadAR = new RoadAR();
            var create = roadAR.Create(
                id: Guid.NewGuid(),
                officialId: dawaRoad.Id.ToString(),
                name: dawaRoad.Name);

            if (create.IsSuccess)
            {
                count++;
                _eventStore.Aggregates.Store(roadAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }

        _logger.LogInformation("Finished importing '{Count}' roads.", count);
    }

    private async Task FullImportPostCodes(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var dawaPostCodesAsyncEnumerable = _dawaClient
            .GetAllPostCodesAsync(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        var count = 0;
        await foreach (var dawaPostCode in dawaPostCodesAsyncEnumerable)
        {
            var postCodeAR = new PostCodeAR();
            var create = postCodeAR.Create(
                id: Guid.NewGuid(),
                number: dawaPostCode.Number,
                name: dawaPostCode.Name);

            if (create.IsSuccess)
            {
                count++;
                _eventStore.Aggregates.Store(postCodeAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }

        _logger.LogInformation("Finished importing '{Count}' post codes.", count);
    }

    private async Task FullImportAccessAdress(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var dawaAccessAddressesAsyncEnumerable = _dawaClient
            .GetAllAccessAddresses(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        var existingPostCodeIds = addressProjection.PostCodeIds;
        var existingRoadIds = addressProjection.RoadIds;

        int count = 0;
        await foreach (var dawaAccessAddress in dawaAccessAddressesAsyncEnumerable)
        {
            var accessAddressAR = new AccessAddressAR();
            if (!addressProjection.PostCodeNumberToId.TryGetValue(
                    dawaAccessAddress.PostDistrictCode, out var postCodeId))
            {
                _logger.LogWarning(
                    "Could not find id using external post district code: '{PostDistrictCode}'.",
                    dawaAccessAddress.PostDistrictCode);
                continue;
            }

            if (!addressProjection.RoadOfficialIdIdToId.TryGetValue(
                    dawaAccessAddress.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using external roadId code: '{RoadId}'.",
                    dawaAccessAddress.RoadId);
                continue;
            }

            var create = accessAddressAR.Create(
                id: Guid.NewGuid(),
                officialId: dawaAccessAddress.Id.ToString(),
                created: dawaAccessAddress.Created,
                updated: dawaAccessAddress.Updated,
                municipalCode: dawaAccessAddress.MunicipalCode,
                status: MapDawaStatus(dawaAccessAddress.Status),
                roadCode: dawaAccessAddress.RoadCode,
                houseNumber: dawaAccessAddress.HouseNumber,
                postCodeId: postCodeId,
                eastCoordinate: dawaAccessAddress.EastCoordinate,
                northCoordinate: dawaAccessAddress.NorthCoordinate,
                locationUpdated: dawaAccessAddress.LocationUpdated,
                supplementaryTownName: dawaAccessAddress.SupplementaryTownName,
                plotId: dawaAccessAddress.PlotId,
                roadId: roadId,
                existingRoadIds: existingRoadIds,
                existingPostCodeIds: existingPostCodeIds);

            if (create.IsSuccess)
            {
                count++;
                _eventStore.Aggregates.Store(accessAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }

        _logger.LogInformation("Finished importing {Count} access addresses.", count);
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
