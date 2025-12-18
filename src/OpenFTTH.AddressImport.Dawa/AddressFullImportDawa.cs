using DawaAddress;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressImport.Dawa;

internal sealed class AddressFullImportDawa : IAddressFullImport
{
    private readonly DatafordelerClient _dawaClient;
    private readonly ILogger<AddressFullImportDawa> _logger;
    private readonly IEventStore _eventStore;
    private const int _bulkCount = 5000;

    public AddressFullImportDawa(
        HttpClient httpClient,
        ILogger<AddressFullImportDawa> logger,
        IEventStore eventStore)
    {
        _dawaClient = new(httpClient);
        _logger = logger;
        _eventStore = eventStore;
    }

    public async Task Start(
        DateTime dateTime,
        CancellationToken cancellationToken = default)
    {
        // Post codes
        _logger.LogInformation(
            "Starting full import of post codes using timestamp: '{TimeStamp}'.",
            dateTime);
        var insertedPostCodesCount = await FullImportPostCodes(dateTime, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' post codes.", insertedPostCodesCount);

        // Active roads
        _logger.LogInformation(
            "Starting full import of Active roads using timestamp: '{TimeStamp}'.",
            dateTime);
        var insertedActiveRoadsCount = await FullImportRoads(
            dateTime, DatafordelerRoadStatus.Active, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' roads.",
            insertedActiveRoadsCount);

        // Temporary roads
        _logger.LogInformation(
            "Starting full import of Temporary roads using timestamp: '{TimeStamp}'.",
            dateTime);
        var insertedTemporaryRoadsCount = await FullImportRoads(
            dateTime, DatafordelerRoadStatus.Temporary, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' roads.", insertedTemporaryRoadsCount);

        // Active access addresses
        _logger.LogInformation("Starting full import of Active access addresses using timestamp: '{TimeStamp}'.", dateTime);
        var insertedActiveAccessAddressesCount = await FullImportAccessAdress(
            dateTime, DatafordelerAccessAddressStatus.Active, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' of Active access addresses.",
            insertedActiveAccessAddressesCount);

        // Pending access addresses
        _logger.LogInformation("Starting full import of Pending access addresses using timestamp: '{TimeStamp}'.", dateTime);
        var insertedPendingAccessAddressesCount = await FullImportAccessAdress(
            dateTime, DatafordelerAccessAddressStatus.Pending, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' of Pending access addresses.",
            insertedPendingAccessAddressesCount);

        // Active unit addresses
        _logger.LogInformation(
            "Starting full import of Active unit addresses using timestamp: '{TimeStamp}'.",
            dateTime);
        var insertedActiveUnitAddressesCount = await FullImportUnitAddresses(
            dateTime, DatafordelerUnitAddressStatus.Active, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting a total '{Count}' of Active unit-addresses.", insertedActiveUnitAddressesCount);

        // Pending unit addresses
        _logger.LogInformation(
            "Starting full import of pending unit addresses using timestamp: '{TimeStamp}'.",
            dateTime);
        var insertedPendingUnitAddressesCount = await FullImportUnitAddresses(
            dateTime, DatafordelerUnitAddressStatus.Pending, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting a total '{Count}' of Pending unit-addresses.", insertedPendingUnitAddressesCount);
    }

    private async Task<int> FullImportRoads(
        DateTime dateTime, DatafordelerRoadStatus roadStatus, CancellationToken cancellationToken)
    {
        var dawaRoadsAsyncEnumerable = _dawaClient
            .GetAllRoadsAsync(DateTime.MinValue, dateTime, roadStatus, cancellationToken)
            .ConfigureAwait(false);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();
        var existingRoadOfficialIds = addressProjection.RoadExternalIdIdToId;

        var count = 0;
        var aggregates = new List<RoadAR>();
        await foreach (var dawaRoad in dawaRoadsAsyncEnumerable)
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (existingRoadOfficialIds.ContainsKey(dawaRoad.Id.ToString()))
            {
                _logger.LogDebug(
                    "Road with official id: '{OfficialId}' has already been created.",
                    dawaRoad.Id);
                continue;
            }

            var roadAR = new RoadAR();
            var create = roadAR.Create(
                id: Guid.NewGuid(),
                externalId: dawaRoad.Id.ToString(),
                name: dawaRoad.Name,
                status: DawaStatusMapper.MapRoadStatus(dawaRoad.Status),
                externalCreatedDate: dawaRoad.Created,
                externalUpdatedDate: dawaRoad.Updated);

            if (create.IsSuccess)
            {
                count++;
                aggregates.Add(roadAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }

        // Store the remaining.
        await _eventStore.Aggregates
            .StoreManyAsync(aggregates)
            .ConfigureAwait(false);

        return count;
    }

    private async Task<int> FullImportPostCodes(
        DateTime dateTime, CancellationToken cancellationToken)
    {
        var dawaPostCodesAsyncEnumerable = _dawaClient
            .GetAllPostCodesAsync(DateTime.MinValue, dateTime, null, cancellationToken)
            .ConfigureAwait(false);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();
        var existingOfficialPostCodeNumbers = addressProjection.PostCodeNumberToId;

        var count = 0;
        var aggregates = new List<PostCodeAR>();
        await foreach (var dawaPostCode in dawaPostCodesAsyncEnumerable)
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (existingOfficialPostCodeNumbers.ContainsKey(dawaPostCode.Number))
            {
                _logger.LogDebug(
                    "Post code with number: '{PostCodeNumber}' has already been created.",
                    dawaPostCode.Number);
                continue;
            }

            var postCodeAR = new PostCodeAR();
            var create = postCodeAR.Create(
                id: Guid.NewGuid(),
                number: dawaPostCode.Number,
                name: dawaPostCode.Name,
                externalCreatedDate: null,
                externalUpdatedDate: null);

            if (create.IsSuccess)
            {
                count++;
                aggregates.Add(postCodeAR);
            }
            else
            {
                throw new InvalidOperationException(
                    create.Errors.FirstOrDefault()?.Message);
            }
        }

        // Store remaining
        await _eventStore.Aggregates
            .StoreManyAsync(aggregates)
            .ConfigureAwait(false);

        return count;
    }

    private async Task<int> FullImportAccessAdress(DateTime dateTime, DatafordelerAccessAddressStatus status, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var dawaAccessAddressesAsyncEnumerable = _dawaClient
            .GetAllAccessAddresses(DateTime.MinValue, dateTime, status, cancellationToken)
            .ConfigureAwait(false);

        // Important to be computed outside the loop, the computation is expensive.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodeIds = addressProjection.GetPostCodeIds();
        var officialAccessAddressIds = addressProjection.AccessAddressExternalIdToId;

        var count = 0;
        var aggregates = new List<AccessAddressAR>();
        await foreach (var dawaAccessAddress in dawaAccessAddressesAsyncEnumerable)
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (officialAccessAddressIds.ContainsKey(dawaAccessAddress.Id.ToString()))
            {
                _logger.LogDebug(
                    @"Access address with official id: '{DawaAccessAddressOfficialId}'
has already been created.",
                    dawaAccessAddress.Id);
                continue;
            }

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

            if (!addressProjection.RoadExternalIdIdToId.TryGetValue(
                    dawaAccessAddress.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using official roadId code: '{RoadId}'.",
                    dawaAccessAddress.RoadId);
                continue;
            }

            var createResult = accessAddressAR.Create(
                id: Guid.NewGuid(),
                externalId: dawaAccessAddress.Id.ToString(),
                externalCreatedDate: dawaAccessAddress.Created,
                externalUpdatedDate: dawaAccessAddress.Updated,
                municipalCode: dawaAccessAddress.MunicipalCode,
                status: DawaStatusMapper.MapAccessAddressStatus(dawaAccessAddress.Status),
                roadCode: dawaAccessAddress.RoadCode,
                houseNumber: dawaAccessAddress.HouseNumber,
                postCodeId: postCodeId,
                eastCoordinate: dawaAccessAddress.EastCoordinate,
                northCoordinate: dawaAccessAddress.NorthCoordinate,
                supplementaryTownName: dawaAccessAddress.SupplementaryTownName,
                plotId: dawaAccessAddress.PlotId,
                roadId: roadId,
                existingRoadIds: existingRoadIds,
                existingPostCodeIds: existingPostCodeIds,
                pendingOfficial: false);

            if (createResult.IsSuccess)
            {
                count++;
                aggregates.Add(accessAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    createResult.Errors.FirstOrDefault()?.Message);
            }
        }

        // Store the remaining
        await _eventStore.Aggregates
            .StoreManyAsync(aggregates)
            .ConfigureAwait(false);

        return count;
    }

    private async Task<int> FullImportUnitAddresses(DateTime dateTime, DatafordelerUnitAddressStatus status, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var dawaUnitAddresssesAsyncEnumerable = _dawaClient
            .GetAllUnitAddresses(DateTime.MinValue, dateTime, status, cancellationToken)
            .ConfigureAwait(false);

        // Important to be computed outside the loop, the computation is expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;
        var unitAddressOfficialIds = addressProjection.UnitAddressExternalIdToId;

        var count = 0;
        var aggregates = new List<UnitAddressAR>();
        await foreach (var dawaUnitAddress in dawaUnitAddresssesAsyncEnumerable)
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (unitAddressOfficialIds.ContainsKey(dawaUnitAddress.Id.ToString()))
            {
                _logger.LogDebug(
                    @"Unit address with official id: '{DawaUnitAddressOfficialId}'
has already been created.",
                    dawaUnitAddress.Id);
                continue;
            }

            var unitAddressAR = new UnitAddressAR();

            if (!addressProjection.AccessAddressExternalIdToId.TryGetValue(
                    dawaUnitAddress.AccessAddressId.ToString(), out var accessAddressId))
            {
                _logger.LogWarning(
                    "Could not find accessAddress using official accessAddressId: '{AccessAddressId}', unit address id being '{UnitAddressId}'.",
                    dawaUnitAddress.AccessAddressId,
                    dawaUnitAddress.Id);
                continue;
            }

            var createResult = unitAddressAR.Create(
                id: Guid.NewGuid(),
                externalId: dawaUnitAddress.Id.ToString(),
                accessAddressId: accessAddressId,
                status: DawaStatusMapper.MapUnitAddressStatus(dawaUnitAddress.Status),
                floorName: dawaUnitAddress.FloorName,
                suiteName: dawaUnitAddress.SuitName,
                externalCreatedDate: dawaUnitAddress.Created,
                externalUpdatedDate: dawaUnitAddress.Updated,
                existingAccessAddressIds: existingAccessAddressIds,
                pendingOfficial: false);

            if (createResult.IsSuccess)
            {
                count++;
                aggregates.Add(unitAddressAR);
            }
            else
            {
                throw new InvalidOperationException(
                    createResult.Errors.FirstOrDefault()?.Message);
            }
        }

        // Store the remaining
        await _eventStore.Aggregates
            .StoreManyAsync(aggregates)
            .ConfigureAwait(false);

        return count;
    }
}
