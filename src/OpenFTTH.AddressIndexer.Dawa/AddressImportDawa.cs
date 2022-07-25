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
            "Starting full import of post codes using tid '{TransactionId}'.",
            latestTransaction.Id);
        var insertedPostCodesCount = await FullImportPostCodes(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' post codes.", insertedPostCodesCount);

        _logger.LogInformation(
            "Starting full import of roads using tid '{TransactionId}'.",
            latestTransaction.Id);
        var insertedRoadsCount = await FullImportRoads(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' roads.", insertedRoadsCount);

        _logger.LogInformation(
            "Starting full import of access addresses using tid '{TransactionId}'.",
            latestTransaction.Id);
        var insertedAccessAddressesCount = await FullImportAccessAdress(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' access addresses.", insertedAccessAddressesCount);

        _logger.LogInformation(
            "Starting full import of unit addresses using tid '{TransactionId}'.",
            latestTransaction.Id);
        var insertedUnitAddressesCount = await FullImportUnitAddresses(
            latestTransaction, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' unit-addresses.", insertedUnitAddressesCount);
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
                name: dawaRoad.Name,
                status: MapRoadStatus(dawaRoad.Status));

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

        // Important to be computed outside the loop, the computation is expensive.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodeIds = addressProjection.GetPostCodeIds();

        var count = 0;
        await foreach (var dawaAccessAddress in dawaAccessAddressesAsyncEnumerable)
        {
            var accessAddressAR = new AccessAddressAR();
            if (!addressProjection.PostCodeNumberToId.TryGetValue(
                    dawaAccessAddress.PostDistrictCode, out var postCodeId))
            {
                _logger.LogWarning(
                    @"Could not find id using official
post district code: '{PostDistrictCode}'.",
                    dawaAccessAddress.PostDistrictCode);
                continue;
            }

            if (!addressProjection.RoadOfficialIdIdToId.TryGetValue(
                    dawaAccessAddress.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using official roadId code: '{RoadId}'.",
                    dawaAccessAddress.RoadId);
                continue;
            }

            var createResult = accessAddressAR.Create(
                id: Guid.NewGuid(),
                officialId: dawaAccessAddress.Id.ToString(),
                created: dawaAccessAddress.Created,
                updated: dawaAccessAddress.Updated,
                municipalCode: dawaAccessAddress.MunicipalCode,
                status: MapAccessAddressStatus(dawaAccessAddress.Status),
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

            if (createResult.IsSuccess)
            {
                count++;
                _eventStore.Aggregates.Store(accessAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    createResult.Errors.FirstOrDefault()?.Message);
            }
        }

        return count;
    }

    private async Task<int> FullImportUnitAddresses(
        DawaTransaction latestTransaction, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var dawaUnitAddresssesAsyncEnumerable = _dawaClient
            .GetAllUnitAddresses(latestTransaction.Id, cancellationToken)
            .ConfigureAwait(false);

        // Important to be computed outside the loop, the computation is expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;

        var count = 0;
        await foreach (var dawaUnitAddress in dawaUnitAddresssesAsyncEnumerable)
        {
            var unitAddressAR = new UnitAddressAR();

            if (!addressProjection.AccessAddressOfficialIdToId.TryGetValue(
                    dawaUnitAddress.AccessAddressId.ToString(), out var accessAddressId))
            {
                _logger.LogWarning(
                    "Could not find accessAddress using official accessAddressId: '{AccessAddressId}'.",
                    dawaUnitAddress.AccessAddressId);
                continue;
            }

            var createResult = unitAddressAR.Create(
                id: Guid.NewGuid(),
                officialId: dawaUnitAddress.Id.ToString(),
                accessAddressId: accessAddressId,
                status: MapUnitAddressStatus(dawaUnitAddress.Status),
                floorName: dawaUnitAddress.FloorName,
                suitName: dawaUnitAddress.SuitName,
                created: dawaUnitAddress.Created,
                updated: dawaUnitAddress.Updated,
                existingAccessAddressIds: existingAccessAddressIds);

            if (createResult.IsSuccess)
            {
                count++;
                _eventStore.Aggregates.Store(unitAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    createResult.Errors.FirstOrDefault()?.Message);
            }
        }

        return count;
    }

    private static AccessAddressStatus MapAccessAddressStatus(DawaStatus status)
        => status switch
        {
            DawaStatus.Active => AccessAddressStatus.Active,
            DawaStatus.Canceled => AccessAddressStatus.Canceled,
            DawaStatus.Discontinued => AccessAddressStatus.Discontinued,
            DawaStatus.Pending => AccessAddressStatus.Pending,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };

    private static UnitAddressStatus MapUnitAddressStatus(DawaStatus status)
        => status switch
        {
            DawaStatus.Active => UnitAddressStatus.Active,
            DawaStatus.Canceled => UnitAddressStatus.Canceled,
            DawaStatus.Discontinued => UnitAddressStatus.Discontinued,
            DawaStatus.Pending => UnitAddressStatus.Pending,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };

    private static RoadStatus MapRoadStatus(DawaRoadStatus status)
        => status switch
        {
            DawaRoadStatus.Effective => RoadStatus.Effective,
            DawaRoadStatus.Temporary => RoadStatus.Temporary,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };

}
