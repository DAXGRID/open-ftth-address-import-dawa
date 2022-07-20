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

        _logger.LogInformation(
            "Starting full import of post codes using transaction id '{TransactionId}'.",
            latestTransaction.Id);
        var insertedPostCodesCount = await FullImportPostCodes(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' post codes.", insertedPostCodesCount);

        _logger.LogInformation(
            "Starting full import of roads using transaction id '{TransactionId}'.",
            latestTransaction.Id);
        var insertedRoadsCount = await FullImportRoads(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' roads.", insertedRoadsCount);

        _logger.LogInformation(
            "Starting full import of access addresses using transaction id '{TransactionId}'.",
            latestTransaction.Id);
        var insertedAccessAddressesCount = await FullImportAccessAdress(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' access addresses.", insertedAccessAddressesCount);
    }

    private async Task<int> FullImportRoads(
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

        return count;
    }

    private async Task<int> FullImportPostCodes(
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

        return count;
    }

    private async Task<int> FullImportAccessAdress(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var dawaAccessAddressesAsyncEnumerable = _dawaClient
            .GetAllAccessAddresses(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        // Its import that these are computed up here, since they're expensive computed properties.
        var existingRoadIds = addressProjection.RoadIds;
        var existingPostCodeIds = addressProjection.PostCodeIds;

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

        return count;
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
