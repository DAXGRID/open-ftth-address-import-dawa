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
        IEventStore eventStore,
        Settings settings)
    {
        _dawaClient = new(httpClient, settings.DatafordelerApiKey);
        _logger = logger;
        _eventStore = eventStore;
    }

    public async Task<DateTime> Start(
        CancellationToken cancellationToken = default)
    {
        var latestGeneration = await _dawaClient.LatestGenerationNumberCurrentTotalDownloadAsync(cancellationToken).ConfigureAwait(false);
        if (latestGeneration is null)
        {
            throw new InvalidOperationException("Not all generation numbers are equal, cannot do full import.");
        }

        var latestGenerationNumber = latestGeneration.Value.generationNumber;
        var latestGenerationTimeStamp = latestGeneration.Value.dateTime;

        _logger.LogInformation(
            "Found latest {GenerationId} from current generation total download with {LatestGenerationNumberTimeStamp}",
            latestGenerationNumber,
            latestGenerationTimeStamp);

        // Post codes
        _logger.LogInformation(
            "Starting full import of post codes using timestamp: '{TimeStamp}'.",
            latestGenerationTimeStamp);

        var insertedPostCodesCount = await FullImportPostCodes(cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' post codes.", insertedPostCodesCount);

        // Active and temporary roads
        _logger.LogInformation(
            "Starting full import of Active and temporary roads using timestamp: '{TimeStamp}'.",
            latestGenerationTimeStamp);
        var insertedActiveRoadsCount = await FullImportRoads(new () { DawaRoadStatus.Effective,  DawaRoadStatus.Temporary }, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' active and temporary roads.",
            insertedActiveRoadsCount);

        // Active and pending access addresses
        _logger.LogInformation("Starting full import of active and pending access addresses using timestamp: '{TimeStamp}'.", latestGenerationTimeStamp);
        var insertedPendingAccessAddressesCount = await FullImportAccessAdress(
            new() { DawaStatus.Active, DawaStatus.Pending }, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting '{Count}' of active and pending access addresses.",
            insertedPendingAccessAddressesCount);

        // Active unit addresses
        _logger.LogInformation(
            "Starting full import of Active unit addresses using timestamp: '{TimeStamp}'.",
            latestGenerationTimeStamp);
        var insertedActiveUnitAddressesCount = await FullImportUnitAddresses(
            new() { DawaStatus.Active, DawaStatus.Pending }, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished inserting a total '{Count}' of active and pending unit-addresses.", insertedActiveUnitAddressesCount);

        return latestGenerationTimeStamp;
    }

    private async Task<int> FullImportRoads(
        HashSet<DawaRoadStatus> includedStatuses, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();
        var existingRoadOfficialIds = addressProjection.RoadExternalIdIdToId;

        var count = 0;
        var aggregates = new List<RoadAR>();
        await foreach (var dawaRoad in _dawaClient.GetAllRoadsAsync(includedStatuses, cancellationToken).ConfigureAwait(false))
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

    private async Task<int> FullImportPostCodes(CancellationToken cancellationToken)
    {
;

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();
        var existingOfficialPostCodeNumbers = addressProjection.PostCodeNumberToId;

        var count = 0;
        var aggregates = new List<PostCodeAR>();
        await foreach (var dawaPostCode in _dawaClient .GetAllPostCodesAsync(cancellationToken).ConfigureAwait(false))
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

    private async Task<int> FullImportAccessAdress(HashSet<DawaStatus> includedStatuses, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        // Important to be computed outside the loop, the computation is expensive.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodeIds = addressProjection.GetPostCodeIds();
        var officialAccessAddressIds = addressProjection.AccessAddressExternalIdToId;

        var insertedIds = new HashSet<Guid>();
        var count = 0;
        var aggregates = new List<AccessAddressAR>();
        await foreach (var dawaAccessAddress in _dawaClient.GetAllAccessAddressesAsync(includedStatuses, cancellationToken).ConfigureAwait(false))
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (insertedIds.Contains(dawaAccessAddress.Id))
            {
                _logger.LogWarning(
                    "Access address with official id: '{DawaAccessAddressOfficialId}' has already been created.",
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
                insertedIds.Add(dawaAccessAddress.Id);
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

    private async Task<int> FullImportUnitAddresses(HashSet<DawaStatus> includedStatuses, CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        // Important to be computed outside the loop, the computation is expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;
        var unitAddressOfficialIds = addressProjection.UnitAddressExternalIdToId;

        var insertedIds = new HashSet<Guid>();
        var count = 0;
        var aggregates = new List<UnitAddressAR>();
        await foreach (var dawaUnitAddress in _dawaClient.GetAllUnitAddressesAsync(includedStatuses, cancellationToken).ConfigureAwait(false))
        {
            if (aggregates.Count == _bulkCount)
            {
                await _eventStore.Aggregates
                    .StoreManyAsync(aggregates)
                    .ConfigureAwait(false);
                aggregates.Clear();
            }

            if (insertedIds.Contains(dawaUnitAddress.Id))
            {
                _logger.LogWarning(
                    "Unit address with official id: '{DawaUnitAddressOfficialId}' has already been created.",
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
                insertedIds.Add(dawaUnitAddress.Id);
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
