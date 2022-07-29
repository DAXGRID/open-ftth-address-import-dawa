using DawaAddress;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa;

internal sealed class AddressChangesImportDawa : IAddressChangesImport
{
    private readonly DawaClient _dawaClient;
    private readonly ILogger<AddressFullImportDawa> _logger;
    private readonly IEventStore _eventStore;

    public AddressChangesImportDawa(
        HttpClient httpClient,
        ILogger<AddressFullImportDawa> logger,
        IEventStore eventStore)
    {
        _dawaClient = new DawaClient(httpClient);
        _logger = logger;
        _eventStore = eventStore;
    }

    public async Task Start(
        ulong fromTransactionId,
        ulong toTransactionId,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Getting changes from '{LastTransactionId} to {LastestTransactionId}'.",
            fromTransactionId,
            toTransactionId);

        var changesPostCodesCount = await ImportPostCodeChanges(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished handling '{Count}' post code changes.", changesPostCodesCount);

        var changesRoadsCount = await ImportRoadChanges(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished handling '{Count}' road changes.", changesRoadsCount);

        var changesAccessAddressesCount = await ImportAccessAddressChanges(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished handling '{Count}' access address changes.",
            changesAccessAddressesCount);

        var changesUnitAddressCount = await ImportUnitAddressChanges(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Finished handling '{Count}' unit address changes.",
            changesUnitAddressCount);
    }

    private async Task<int> ImportPostCodeChanges(
        ulong fromTransactionId,
        ulong toTransactionId,
        CancellationToken cancellationToken)
    {
        var changesPostCodesAsyncEnumerable = _dawaClient.GetChangesPostCodesAsync(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var count = 0;
        await foreach (var postCodeChange in changesPostCodesAsyncEnumerable)
        {
            if (postCodeChange.Operation == DawaEntityChangeOperation.Insert)
            {
                if (addressProjection.PostCodeNumberToId
                    .ContainsKey(postCodeChange.Data.Number))
                {
                    _logger.LogWarning(
                        "Post code with number '{Number}' has already been created.",
                        postCodeChange.Data.Number);
                    continue;
                }

                var postCodeAR = new PostCodeAR();
                var createResult = postCodeAR.Create(
                    id: Guid.NewGuid(),
                    number: postCodeChange.Data.Number,
                    name: postCodeChange.Data.Name);

                if (createResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(postCodeAR);
                }
                else
                {
                    // There will always only be one error.
                    var error = (PostCodeError)createResult.Errors.First();
                    throw new InvalidOperationException(error.Message);
                }
            }
            else if (postCodeChange.Operation == DawaEntityChangeOperation.Update)
            {
                if (!addressProjection.PostCodeNumberToId.TryGetValue(
                        postCodeChange.Data.Number, out var postCodeId))
                {
                    _logger.LogWarning(
                        "Could not find id on '{PostNumber}'",
                        postCodeChange.Data.Number);
                }

                var postCodeAR = _eventStore.Aggregates.Load<PostCodeAR>(postCodeId);
                if (postCodeAR is not null)
                {
                    var updateResult = postCodeAR.Update(postCodeChange.Data.Name);

                    if (updateResult.IsSuccess)
                    {
                        _eventStore.Aggregates.Store(postCodeAR);
                    }
                    else
                    {
                        // There will always only be one error.
                        var error = (PostCodeError)updateResult.Errors.First();
                        if (error.Code == PostCodeErrorCodes.NO_CHANGES)
                        {
                            // No changes is okay, we just log it.
                            _logger.LogInformation("{ErrorMessage}", error.Message);
                            continue;
                        }
                        else
                        {
                            throw new InvalidOperationException(error.Message);
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException(
                        @$"Could not load {nameof(postCodeAR)}
on {nameof(postCodeId)}: '{postCodeId}'");
                }
            }
            else if (postCodeChange.Operation == DawaEntityChangeOperation.Delete)
            {
                if (!addressProjection.PostCodeNumberToId.TryGetValue(
                        postCodeChange.Data.Number, out var postCodeId))
                {
                    _logger.LogWarning(
                        "Could not find id on '{PostNumber}'",
                        postCodeChange.Data.Number);
                }

                var postCodeAR = _eventStore.Aggregates.Load<PostCodeAR>(postCodeId);
                if (postCodeAR is not null)
                {
                    var deleteResult = postCodeAR.Delete();

                    if (deleteResult.IsSuccess)
                    {
                        _eventStore.Aggregates.Store(postCodeAR);
                    }
                    else
                    {
                        // There will always only be one error.
                        var error = (PostCodeError)deleteResult.Errors.First();
                        throw new InvalidOperationException(error.Message);
                    }
                }
                else
                {
                    throw new InvalidOperationException(
                        @$"Could not load {nameof(postCodeAR)}
on {nameof(postCodeId)}: '{postCodeId}'");
                }
            }
            else
            {
                throw new InvalidOperationException(
                    "No valid handling of post code change.");
            }

            count++;
        }

        return count;
    }

    private async Task<int> ImportRoadChanges(
        ulong fromTransactionId,
        ulong toTransactionId,
        CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var roadChangesAsyncEnumerable = _dawaClient.GetChangesRoadsAsync(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);

        var count = 0;
        await foreach (var change in roadChangesAsyncEnumerable)
        {
            if (change.Operation == DawaEntityChangeOperation.Insert)
            {
                if (addressProjection.RoadOfficialIdIdToId
                    .ContainsKey(change.Data.Id.ToString()))
                {
                    _logger.LogWarning(
                        @"Road with official id '{OfficialId}' has already been created.",
                        change.Data.Id);
                    continue;
                }

                var roadAR = new RoadAR();

                var createResult = roadAR.Create(
                    id: Guid.NewGuid(),
                    officialId: change.Data.Id.ToString(),
                    name: change.Data.Name,
                    status: DawaStatusMapper.MapRoadStatus(change.Data.Status));

                if (createResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(roadAR);
                }
                else
                {
                    // There will always only be a single error
                    throw new InvalidOperationException(
                        createResult.Errors.First().Message);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Update)
            {
                if (!addressProjection.RoadOfficialIdIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var roadId))
                {
                    throw new InvalidOperationException(
                        $"Could not lookup road on id '{change.Data.Id}'.");
                }

                var roadAR = _eventStore.Aggregates.Load<RoadAR>(roadId);
                if (roadAR is null)
                {
                    throw new InvalidOperationException(
                        $"Could not load {nameof(RoadAR)} on id '{roadId}'.");
                }

                var updateResult = roadAR.Update(
                    name: change.Data.Name,
                    officialId: change.Data.Id.ToString(),
                    status: DawaStatusMapper.MapRoadStatus(change.Data.Status));

                if (updateResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(roadAR);
                }
                else
                {
                    // There will always only be a single error.
                    throw new InvalidOperationException(
                       updateResult.Errors.First().Message);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Delete)
            {
                if (!addressProjection.RoadOfficialIdIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var roadId))
                {
                    throw new InvalidOperationException(
                        $"Could not lookup road on id '{change.Data.Id}'.");
                }

                var roadAR = _eventStore.Aggregates.Load<RoadAR>(roadId);
                if (roadAR is null)
                {
                    throw new InvalidOperationException(
                        $"Could not load {nameof(RoadAR)} on id '{roadId}'.");
                }

                var deleteResult = roadAR.Delete();
                if (deleteResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(roadAR);
                }
                else
                {
                    throw new InvalidOperationException(
                        deleteResult.Errors.First().Message);
                }
            }
            else
            {
                throw new InvalidOperationException(
                    "No valid handling of post code change.");
            }

            count++;
        }

        return count;
    }

    private async Task<int> ImportAccessAddressChanges(
        ulong fromTransactionId,
        ulong toTransactionId,
        CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var accessAddressChangesAsyncEnumerable = _dawaClient.GetChangesAccessAddressAsync(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);

        // Important to do this outside since they're expensive computations.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodes = addressProjection.GetPostCodeIds();

        var count = 0;
        await foreach (var change in accessAddressChangesAsyncEnumerable)
        {
            if (change.Operation == DawaEntityChangeOperation.Insert)
            {
                if (!addressProjection.AccessAddressOfficialIdToId.ContainsKey(change.Data.Id.ToString()))
                {
                    var accessAddressAR = new AccessAddressAR();
                    if (!addressProjection.PostCodeNumberToId.TryGetValue(
                            change.Data.PostDistrictCode, out var postCodeId))
                    {
                        _logger.LogWarning(
                            @"Could not find id using official
post district code: '{PostDistrictCode}'.",
                            change.Data.PostDistrictCode);
                        continue;
                    }

                    if (!addressProjection.RoadOfficialIdIdToId.TryGetValue(
                            change.Data.RoadId.ToString(), out var roadId))
                    {
                        _logger.LogWarning(
                            "Could not find roadId using official roadId code: '{RoadId}'.",
                            change.Data.RoadId);
                        continue;
                    }

                    var createResult = accessAddressAR.Create(
                        id: Guid.NewGuid(),
                        officialId: change.Data.Id.ToString(),
                        created: change.Data.Created,
                        updated: change.Data.Updated,
                        municipalCode: change.Data.MunicipalCode,
                        status: DawaStatusMapper.MapAccessAddressStatus(change.Data.Status),
                        roadCode: change.Data.RoadCode,
                        houseNumber: change.Data.HouseNumber,
                        postCodeId: postCodeId,
                        eastCoordinate: change.Data.EastCoordinate,
                        northCoordinate: change.Data.NorthCoordinate,
                        locationUpdated: change.Data.LocationUpdated,
                        supplementaryTownName: change.Data.SupplementaryTownName,
                        plotId: change.Data.PlotId,
                        roadId: roadId,
                        existingRoadIds: existingRoadIds,
                        existingPostCodeIds: existingPostCodes);

                    if (createResult.IsSuccess)
                    {
                        _eventStore.Aggregates.Store(accessAddressAR);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            createResult.Errors.FirstOrDefault()?.Message);
                    }
                }
                else
                {
                    _logger.LogInformation(
                        "Access address with internal id: '{Id}' has already been created.",
                        change.Data.Id);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Update)
            {
                if (!addressProjection.AccessAddressOfficialIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var accessAddressId))
                {
                    throw new InvalidOperationException(
                        $"Could not lookup access address on id '{change.Data.Id}'.");
                }

                var accessAddressAR = _eventStore.Aggregates
                    .Load<AccessAddressAR>(accessAddressId);
                if (accessAddressAR is null)
                {
                    throw new InvalidOperationException(
                        @$"Could not load {nameof(AccessAddressAR)}
 on id '{accessAddressId}'.");
                }

                if (!addressProjection.PostCodeNumberToId.TryGetValue(
                        change.Data.PostDistrictCode, out var postCodeId))
                {
                    _logger.LogWarning(
                        @"Could not find id using official
post district code: '{PostDistrictCode}'.",
                        change.Data.PostDistrictCode);
                    continue;
                }

                if (!addressProjection.RoadOfficialIdIdToId.TryGetValue(
                        change.Data.RoadId.ToString(), out var roadId))
                {
                    _logger.LogWarning(
                        "Could not find roadId using official roadId code: '{RoadId}'.",
                        change.Data.RoadId);
                    continue;
                }

                var updateResult = accessAddressAR.Update(
                    officialId: change.Data.Id.ToString(),
                    updated: change.Data.Updated,
                    municipalCode: change.Data.MunicipalCode,
                    status: DawaStatusMapper.MapAccessAddressStatus(change.Data.Status),
                    roadCode: change.Data.RoadCode,
                    houseNumber: change.Data.HouseNumber,
                    postCodeId: postCodeId,
                    eastCoordinate: change.Data.EastCoordinate,
                    northCoordinate: change.Data.NorthCoordinate,
                    locationUpdated: change.Data.LocationUpdated,
                    supplementaryTownName: change.Data.SupplementaryTownName,
                    plotId: change.Data.PlotId,
                    roadId: roadId,
                    existingRoadIds: existingRoadIds,
                    existingPostCodeIds: existingPostCodes);

                if (updateResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(accessAddressAR);
                }
                else
                {
                    throw new InvalidOperationException(
                        updateResult.Errors.FirstOrDefault()?.Message);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Delete)
            {
                if (!addressProjection.AccessAddressOfficialIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var accessAddressId))
                {
                    throw new InvalidOperationException(
                        $"Could not lookup access address on id '{change.Data.Id}'.");
                }

                var accessAddressAR = _eventStore.Aggregates
                    .Load<AccessAddressAR>(accessAddressId);
                if (accessAddressAR is null)
                {
                    throw new InvalidOperationException(
                        @$"Could not load {nameof(AccessAddressAR)}
 on id '{accessAddressId}'.");
                }

                var deleteResult = accessAddressAR.Delete();
                if (deleteResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(accessAddressAR);
                }
                else
                {
                    throw new InvalidOperationException(
                        deleteResult.Errors.First().Message);
                }
            }

            count++;
        }

        return count;
    }

    private async Task<int> ImportUnitAddressChanges(
        ulong fromTransactionId,
        ulong toTransactionId,
        CancellationToken cancellationToken)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        // We use the access address ids out here since the operation might be expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;

        var unitAddressChangesAsyncEnumerable = _dawaClient.GetChangesUnitAddressAsync(
            fromTransactionId, toTransactionId, cancellationToken).ConfigureAwait(false);

        var count = 0;
        await foreach (var change in unitAddressChangesAsyncEnumerable)
        {
            if (change.Operation == DawaEntityChangeOperation.Insert)
            {
                if (addressProjection.UnitAddressOfficialIdToId
                    .ContainsKey(change.Data.Id.ToString()))
                {
                    _logger.LogWarning(
                        @"Cannot create unit address
with offical id '{OfficialId}' since it already has been created.", change.Data.Id);
                    continue;
                }

                if (!addressProjection.AccessAddressOfficialIdToId.TryGetValue(
                        change.Data.AccessAddressId.ToString(),
                        out var accessAddressId))
                {
                    _logger.LogWarning(
                        @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                        change.Data.AccessAddressId);
                    continue;
                }

                var unitAddressAR = new UnitAddressAR();

                var createResult = unitAddressAR.Create(
                    id: Guid.NewGuid(),
                    officialId: change.Data.Id.ToString(),
                    accessAddressId: accessAddressId,
                    status: DawaStatusMapper.MapUnitAddressStatus(change.Data.Status),
                    floorName: change.Data.FloorName,
                    suitName: change.Data.SuitName,
                    created: change.Data.Created,
                    updated: change.Data.Updated,
                    existingAccessAddressIds: existingAccessAddressIds);

                if (createResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(unitAddressAR);
                }
                else
                {
                    throw new InvalidOperationException(
                        createResult.Errors.FirstOrDefault()?.Message);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Update)
            {
                if (!addressProjection.UnitAddressOfficialIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var unitAddressId))
                {
                    _logger.LogWarning(
                        @"Could not find internal unit address id
using official id '{OfficialId}'.",
                        change.Data.Id);
                    continue;
                }

                if (!addressProjection.AccessAddressOfficialIdToId.TryGetValue(
                        change.Data.AccessAddressId.ToString(),
                        out var accessAddressId))
                {
                    _logger.LogWarning(
                        @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                        change.Data.AccessAddressId);
                    continue;
                }

                var unitAddressAR = _eventStore.Aggregates.Load<UnitAddressAR>(unitAddressId);

                var createResult = unitAddressAR.Update(
                    officialId: change.Data.Id.ToString(),
                    accessAddressId: accessAddressId,
                    status: DawaStatusMapper.MapUnitAddressStatus(change.Data.Status),
                    floorName: change.Data.FloorName,
                    suitName: change.Data.SuitName,
                    updated: change.Data.Updated,
                    existingAccessAddressIds: existingAccessAddressIds);

                if (createResult.IsSuccess)
                {
                    _eventStore.Aggregates.Store(unitAddressAR);
                }
                else
                {
                    throw new InvalidOperationException(
                        createResult.Errors.FirstOrDefault()?.Message);
                }
            }
            else if (change.Operation == DawaEntityChangeOperation.Delete)
            {
                if (addressProjection.UnitAddressOfficialIdToId
                    .TryGetValue(change.Data.Id.ToString(), out var unitAddressId))
                {
                    var unitAddressAR = _eventStore.Aggregates
                        .Load<UnitAddressAR>(unitAddressId);

                    var deleteResult = unitAddressAR.Delete();
                    if (deleteResult.IsSuccess)
                    {
                        _eventStore.Aggregates.Store(unitAddressAR);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            deleteResult.Errors.First().Message);
                    }
                }
                else
                {
                    _logger.LogWarning(
                        @"Could not find unit address
using official id '{OfficalId}'. Deletion can therefore not happen.", change.Data.Id);
                    continue;
                }
            }
            else
            {
                throw new InvalidOperationException(
                    "No valid handling of unit address DAWA change.");
            }

            count++;
        }

        return count;
    }
}
