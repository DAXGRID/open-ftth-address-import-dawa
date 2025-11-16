using DawaAddress;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressImport.Dawa;

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
            "Getting changes from '{FromTransactionId} to {ToTransactionId}'.",
            fromTransactionId,
            toTransactionId);

        var postCodeChanges = await _dawaClient
            .GetChangesPostCodesAsync(fromTransactionId, toTransactionId, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var roadChanges = await _dawaClient
            .GetChangesRoadsAsync(fromTransactionId, toTransactionId, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var accessAddressChanges = await _dawaClient
            .GetChangesAccessAddressAsync(fromTransactionId, toTransactionId, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var unitAddressChanges = await _dawaClient
            .GetChangesUnitAddressAsync(fromTransactionId, toTransactionId, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var entityChanges = new List<(ulong sequenceNumber, object data)>();

        foreach (var change in postCodeChanges)
        {
            entityChanges.Add(new(change.SequenceNumber, change.Data));
        }

        foreach (var change in roadChanges)
        {
            entityChanges.Add(new(change.SequenceNumber, change.Data));
        }

        foreach (var change in accessAddressChanges)
        {
            entityChanges.Add(new(change.SequenceNumber, change.Data));
        }

        foreach (var change in unitAddressChanges)
        {
            entityChanges.Add(new(change.SequenceNumber, change.Data));
        }

        foreach (var entityChange in entityChanges.OrderBy(x => x.sequenceNumber))
        {
            switch (entityChange.data)
            {
                case DawaEntityChange<DawaPostCode> dawaPostCodeChange:
                    await ImportPostCodeChange(dawaPostCodeChange).ConfigureAwait(false);
                    break;
                case DawaEntityChange<DawaRoad> dawaRoadChange:
                    await ImportRoadChange(dawaRoadChange).ConfigureAwait(false);
                    break;
                case DawaEntityChange<DawaAccessAddress> dawaAccessAddressChange:
                    await ImportAccessAddressChange(dawaAccessAddressChange).ConfigureAwait(false);
                    break;
                case DawaEntityChange<DawaUnitAddress> dawaUnitAddressChange:
                    await ImportUnitAddressChange(dawaUnitAddressChange).ConfigureAwait(false);
                    break;
            }
        }

        _logger.LogInformation("Finished processing a total of {PostCodeChangesCount}.", postCodeChanges.Count);
        _logger.LogInformation("Finished processing a total of {RoadChangesCount}.", roadChanges.Count);
        _logger.LogInformation("Finished processing a total of {AccessAddressChanges}.", accessAddressChanges.Count);
        _logger.LogInformation("Finished processing a total of {UnitAddressChangesCount}.", unitAddressChanges.Count);
    }

    private async Task ImportPostCodeChange(DawaEntityChange<DawaPostCode> postCodeChange)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        if (postCodeChange.Operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.PostCodeNumberToId
                .ContainsKey(postCodeChange.Data.Number))
            {
                _logger.LogWarning(
                    "Post code with number '{Number}' has already been created.",
                    postCodeChange.Data.Number);

                return;
            }

            var postCodeAR = new PostCodeAR();
            var createResult = postCodeAR.Create(
                id: Guid.NewGuid(),
                number: postCodeChange.Data.Number,
                name: postCodeChange.Data.Name,
                externalCreatedDate: postCodeChange.ChangeTime,
                externalUpdatedDate: postCodeChange.ChangeTime);

            if (createResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(postCodeAR)
                    .ConfigureAwait(false);
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
                var updateResult = postCodeAR.Update(
                    name: postCodeChange.Data.Name,
                    externalUpdatedDate: postCodeChange.ChangeTime);

                if (updateResult.IsSuccess)
                {
                    await _eventStore.Aggregates
                        .StoreAsync(postCodeAR)
                        .ConfigureAwait(false);
                }
                else
                {
                    // There will always only be one error.
                    var error = (PostCodeError)updateResult.Errors.First();
                    if (error.Code == PostCodeErrorCodes.NO_CHANGES)
                    {
                        // No changes is okay, we just log it.
                        _logger.LogDebug("{ErrorMessage}", error.Message);
                        return;
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
                var deleteResult = postCodeAR.Delete(
                    externalUpdatedDate: postCodeChange.ChangeTime);

                if (deleteResult.IsSuccess)
                {
                    await _eventStore.Aggregates
                        .StoreAsync(postCodeAR)
                        .ConfigureAwait(false);
                }
                else
                {
                    // There will always only be one error.
                    var error = (PostCodeError)deleteResult.Errors.First();
                    if (error.Code == PostCodeErrorCodes.CANNOT_DELETE_ALREADY_DELETED)
                    {
                        // No changes is okay, we just log it.
                        _logger.LogDebug("{ErrorMessage}", error.Message);
                        return;
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
        else
        {
            throw new InvalidOperationException(
                "No valid handling of post code change.");
        }
    }

    private async Task ImportRoadChange(DawaEntityChange<DawaRoad> change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        if (change.Operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.RoadExternalIdIdToId
                .ContainsKey(change.Data.Id.ToString()))
            {
                _logger.LogWarning(
                    @"Road with official id '{OfficialId}' has already been created.",
                    change.Data.Id);

                return;
            }

            var roadAR = new RoadAR();

            var createResult = roadAR.Create(
                id: Guid.NewGuid(),
                externalId: change.Data.Id.ToString(),
                name: change.Data.Name,
                status: DawaStatusMapper.MapRoadStatus(change.Data.Status),
                externalCreatedDate: change.Data.Created,
                externalUpdatedDate: change.Data.Updated);

            if (createResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(roadAR)
                    .ConfigureAwait(false);
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
            if (!addressProjection.RoadExternalIdIdToId
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
                externalId: change.Data.Id.ToString(),
                status: DawaStatusMapper.MapRoadStatus(change.Data.Status),
                externalUpdatedDate: change.Data.Updated);

            if (updateResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(roadAR)
                    .ConfigureAwait(false);
            }
            else
            {
                // There will always only be a single error.
                var error = (RoadError)updateResult.Errors.First();
                // No changes is okay, we just log it.
                _logger.LogWarning(
                    "Id: {ExternalId}, Error: {ErrorMessage}",
                    error.Message,
                    change.Data.Id.ToString());

                return;
            }
        }
        else if (change.Operation == DawaEntityChangeOperation.Delete)
        {
            if (!addressProjection.RoadExternalIdIdToId
                .TryGetValue(change.Data.Id.ToString(), out var roadId))
            {
                throw new InvalidOperationException(
                    $"Could not lookup road on id '{change.Data.Id}' for deletion.");
            }

            var roadAR = _eventStore.Aggregates.Load<RoadAR>(roadId);
            if (roadAR is null)
            {
                throw new InvalidOperationException(
                    $"Could not load {nameof(RoadAR)} on id '{roadId}'.");
            }

            var deleteResult = roadAR.Delete(
                externalUpdatedDate: change.ChangeTime);

            if (deleteResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(roadAR)
                    .ConfigureAwait(false);
            }
            else
            {
                // There will always only be a single error.
                var error = (RoadError)deleteResult.Errors.First();
                if (error.Code == RoadErrorCode.CANNOT_DELETE_ALREADY_DELETED)
                {
                    // No changes is okay, we just log it.
                    _logger.LogDebug("{ErrorMessage}", error.Message);

                    return;
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
                "No valid handling of post code change.");
        }
    }

    private async Task ImportAccessAddressChange(DawaEntityChange<DawaAccessAddress> change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        // Important to do this outside since they're expensive computations.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodes = addressProjection.GetPostCodeIds();

        if (change.Operation == DawaEntityChangeOperation.Insert)
        {
            if (!addressProjection.AccessAddressExternalIdToId
                .ContainsKey(change.Data.Id.ToString()))
            {
                var accessAddressAR = new AccessAddressAR();
                if (!addressProjection.PostCodeNumberToId.TryGetValue(
                        change.Data.PostDistrictCode, out var postCodeId))
                {
                    _logger.LogWarning(
                        @"Could not find id using official
post district code: '{PostDistrictCode}'.",
                        change.Data.PostDistrictCode);

                    return;
                }

                if (!addressProjection.RoadExternalIdIdToId.TryGetValue(
                        change.Data.RoadId.ToString(), out var roadId))
                {
                    _logger.LogWarning(
                        "Could not find roadId using official roadId code: '{RoadId}'.",
                        change.Data.RoadId);

                    return;
                }

                var createResult = accessAddressAR.Create(
                    id: Guid.NewGuid(),
                    externalId: change.Data.Id.ToString(),
                    externalCreatedDate: change.Data.Created,
                    externalUpdatedDate: change.Data.Updated,
                    municipalCode: change.Data.MunicipalCode,
                    status: DawaStatusMapper.MapAccessAddressStatus(change.Data.Status),
                    roadCode: change.Data.RoadCode,
                    houseNumber: change.Data.HouseNumber,
                    postCodeId: postCodeId,
                    eastCoordinate: change.Data.EastCoordinate,
                    northCoordinate: change.Data.NorthCoordinate,
                    supplementaryTownName: change.Data.SupplementaryTownName,
                    plotId: change.Data.PlotId,
                    roadId: roadId,
                    existingRoadIds: existingRoadIds,
                    existingPostCodeIds: existingPostCodes,
                    pendingOfficial: false);

                if (createResult.IsSuccess)
                {
                    await _eventStore.Aggregates
                        .StoreAsync(accessAddressAR)
                        .ConfigureAwait(false);
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
            if (!addressProjection.AccessAddressExternalIdToId
                .TryGetValue(change.Data.Id.ToString(), out var accessAddressId))
            {
                _logger.LogWarning(
                    "Could not lookup access address on id {ChangeDataId}.",
                    change.Data.Id);

                return;
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
                    "Could not find id using official post district code: '{PostDistrictCode}'.",
                    change.Data.PostDistrictCode);

                return;
            }

            if (!addressProjection.RoadExternalIdIdToId.TryGetValue(
                    change.Data.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using official roadId code: '{RoadId}'.",
                    change.Data.RoadId);

                return;
            }

            var updateResult = accessAddressAR.Update(
                externalId: change.Data.Id.ToString(),
                externalUpdatedDate: change.Data.Updated,
                municipalCode: change.Data.MunicipalCode,
                status: DawaStatusMapper.MapAccessAddressStatus(change.Data.Status),
                roadCode: change.Data.RoadCode,
                houseNumber: change.Data.HouseNumber,
                postCodeId: postCodeId,
                eastCoordinate: change.Data.EastCoordinate,
                northCoordinate: change.Data.NorthCoordinate,
                supplementaryTownName: change.Data.SupplementaryTownName,
                plotId: change.Data.PlotId,
                roadId: roadId,
                existingRoadIds: existingRoadIds,
                existingPostCodeIds: existingPostCodes,
                pendingOfficial: false);

            if (updateResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(accessAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                // There will always only be a single error.
                var error = (AccessAddressError)updateResult.Errors.First();
                if (error.Code == AccessAddressErrorCode.NO_CHANGES ||
                    error.Code == AccessAddressErrorCode.CANNOT_UPDATE_DELETED)
                {
                    _logger.LogInformation(
                        "{ExternalId}: {ErrorMessage}",
                        accessAddressAR.ExternalId,
                        error.Message);

                    return;
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
        else if (change.Operation == DawaEntityChangeOperation.Delete)
        {
            if (!addressProjection.AccessAddressExternalIdToId
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

            var deleteResult = accessAddressAR.Delete(
                externalUpdatedDate: change.ChangeTime);

            if (deleteResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(accessAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                // There will always only be a single error.
                var error = (AccessAddressError)deleteResult.Errors.First();
                if (error.Code == AccessAddressErrorCode.CANNOT_DELETE_ALREADY_DELETED)
                {
                    // No changes is okay, we just log it.
                    _logger.LogDebug("{ErrorMessage}", error.Message);
                    return;
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
    }

    private async Task ImportUnitAddressChange(DawaEntityChange<DawaUnitAddress> change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        // We use the access address ids out here since the operation might be expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;

        if (change.Operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.UnitAddressExternalIdToId
                .ContainsKey(change.Data.Id.ToString()))
            {
                _logger.LogWarning(
                    @"Cannot create unit address
with offical id '{OfficialId}' since it already has been created.", change.Data.Id);

                return;
            }

            if (!addressProjection.AccessAddressExternalIdToId.TryGetValue(
                    change.Data.AccessAddressId.ToString(),
                    out var accessAddressId))
            {
                _logger.LogWarning(
                    @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                    change.Data.AccessAddressId);

                return;
            }

            var unitAddressAR = new UnitAddressAR();

            var createResult = unitAddressAR.Create(
                id: Guid.NewGuid(),
                externalId: change.Data.Id.ToString(),
                accessAddressId: accessAddressId,
                status: DawaStatusMapper.MapUnitAddressStatus(change.Data.Status),
                floorName: change.Data.FloorName,
                suiteName: change.Data.SuitName,
                externalCreatedDate: change.Data.Created,
                externalUpdatedDate: change.Data.Updated,
                existingAccessAddressIds: existingAccessAddressIds,
                pendingOfficial: false);

            if (createResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(unitAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException(
                    createResult.Errors.FirstOrDefault()?.Message);
            }
        }
        else if (change.Operation == DawaEntityChangeOperation.Update)
        {
            if (!addressProjection.UnitAddressExternalIdToId
                .TryGetValue(change.Data.Id.ToString(), out var unitAddressId))
            {
                _logger.LogWarning(
                    @"Could not find internal unit address id
using official id '{OfficialId}'.",
                    change.Data.Id);

                return;
            }

            if (!addressProjection.AccessAddressExternalIdToId.TryGetValue(
                    change.Data.AccessAddressId.ToString(),
                    out var accessAddressId))
            {
                _logger.LogWarning(
                    @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                    change.Data.AccessAddressId);

                return;
            }

            var unitAddressAR = _eventStore.Aggregates
                .Load<UnitAddressAR>(unitAddressId);

            var updateResult = unitAddressAR.Update(
                externalId: change.Data.Id.ToString(),
                accessAddressId: accessAddressId,
                status: DawaStatusMapper.MapUnitAddressStatus(change.Data.Status),
                floorName: change.Data.FloorName,
                suiteName: change.Data.SuitName,
                externalUpdatedDate: change.Data.Updated,
                existingAccessAddressIds: existingAccessAddressIds,
                pendingOfficial: false);

            if (updateResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(unitAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                // There will always only be a single error.
                var error = (UnitAddressError)updateResult.Errors.First();
                if (error.Code == UnitAddressErrorCode.NO_CHANGES ||
                    error.Code == UnitAddressErrorCode.CANNOT_UPDATE_DELETED)
                {
                    _logger.LogInformation(
                        "{ExternalId}: {ErrorMessage}",
                        unitAddressAR.ExternalId,
                        error.Message);

                    return;
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
        else if (change.Operation == DawaEntityChangeOperation.Delete)
        {
            if (addressProjection.UnitAddressExternalIdToId
                .TryGetValue(change.Data.Id.ToString(), out var unitAddressId))
            {
                var unitAddressAR = _eventStore.Aggregates
                    .Load<UnitAddressAR>(unitAddressId);

                var deleteResult = unitAddressAR.Delete(
                    externalUpdatedDate: change.ChangeTime);

                if (deleteResult.IsSuccess)
                {
                    await _eventStore.Aggregates
                        .StoreAsync(unitAddressAR)
                        .ConfigureAwait(false);
                }
                else
                {
                    var error = (UnitAddressError)deleteResult.Errors.First();
                    if (error.Code == UnitAddressErrorCode
                        .CANNOT_DELETE_ALREADY_DELETED)
                    {
                        // No changes is okay, we just log it.
                        _logger.LogWarning("{ErrorMessage}", error.Message);

                        return;
                    }
                    else
                    {
                        throw new InvalidOperationException(error.Message);
                    }
                }
            }
            else
            {
                _logger.LogWarning(
                    @"Could not find unit address
using official id '{OfficalId}'. Deletion can therefore not happen.", change.Data.Id);

                return;
            }
        }
        else
        {
            throw new InvalidOperationException(
                "No valid handling of unit address DAWA change.");
        }
    }
}

internal static class AsyncEnumerableExtensions
{
    public static async Task<List<T>> ToListAsync<T>(
        this IAsyncEnumerable<T> items,
        CancellationToken cancellationToken = default)
    {
        var results = new List<T>();

        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            results.Add(item);

        return results;
    }
}
