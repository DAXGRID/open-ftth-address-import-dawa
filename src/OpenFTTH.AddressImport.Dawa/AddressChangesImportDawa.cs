using DawaAddress;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;
using System.Text.Json;

namespace OpenFTTH.AddressImport.Dawa;

internal sealed class AddressChangesImportDawa : IAddressChangesImport
{
    private readonly DatafordelerClient _datafordelerClient;
    private readonly ILogger<AddressFullImportDawa> _logger;
    private readonly IEventStore _eventStore;
    private const string _apiKey = "";

    public AddressChangesImportDawa(
        HttpClient httpClient,
        ILogger<AddressFullImportDawa> logger,
        IEventStore eventStore)
    {
        _datafordelerClient = new DatafordelerClient(httpClient, _apiKey);
        _logger = logger;
        _eventStore = eventStore;
    }

    public async Task Start(
        DateTime fromTimeStamp,
        DateTime toTimeStamp,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Getting changes from '{FromTimeStamp} to {ToTimeStamp}'.",
            fromTimeStamp,
            toTimeStamp);

        // Post codes
        var postCodeChanges = await _datafordelerClient
            .GetAllPostCodesAsync(fromTimeStamp, toTimeStamp, null, cancellationToken)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        var roadChanges = await _datafordelerClient
            .GetAllRoadsAsync(fromTimeStamp, toTimeStamp, null, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        // Access addresses
        var accessAddressChanges = await _datafordelerClient
            .GetAllAccessAddressesAsync(fromTimeStamp, toTimeStamp, null, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        // Unit addresses
        var unitAddressChanges = await _datafordelerClient
            .GetAllUnitAddressesAsync(fromTimeStamp, toTimeStamp, null, cancellationToken)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var entityChanges = new List<(DateTime sequenceNumber, object data)>();

        foreach (var change in postCodeChanges)
        {
            // We do minimum value to make sure post codes always come first.
            // We  do now get the timestamp it was updated from Datafordeleren.
            entityChanges.Add(new(DateTime.MinValue, change));
        }

        foreach (var change in roadChanges)
        {
            entityChanges.Add(new(change.Updated, change));
        }

        foreach (var change in accessAddressChanges)
        {
            entityChanges.Add(new(change.Updated, change));
        }

        foreach (var change in unitAddressChanges)
        {
            entityChanges.Add(new(change.Updated, change));
        }

        foreach (var entityChange in entityChanges.OrderBy(x => x.sequenceNumber))
        {
            _logger.LogInformation("Processing {Change}", JsonSerializer.Serialize(entityChange.data));

            try
            {
                switch (entityChange.data)
                {
                    case DawaPostCode dawaPostCodeChange:
                        await ImportPostCodeChange(dawaPostCodeChange).ConfigureAwait(false);
                        break;
                    case DawaRoad dawaRoadChange:
                        await ImportRoadChange(dawaRoadChange).ConfigureAwait(false);
                        break;
                    case DawaAccessAddress dawaAccessAddressChange:
                        await ImportAccessAddressChange(dawaAccessAddressChange).ConfigureAwait(false);
                        break;
                    case DawaUnitAddress dawaUnitAddressChange:
                        await ImportUnitAddressChange(dawaUnitAddressChange).ConfigureAwait(false);
                        break;
                    default:
                        throw new ArgumentException($"Unsupported type.");
                }

                await _eventStore.CatchUpAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogCritical("Failed on change {Change}, {Exception}", JsonSerializer.Serialize(entityChange.data), ex);
                throw;
            }
        }

        _logger.LogInformation("Finished processing a total of {PostCodeChangesCount}.", postCodeChanges.Count);
        _logger.LogInformation("Finished processing a total of {RoadChangesCount}.", roadChanges.Count);
        _logger.LogInformation("Finished processing a total of {AccessAddressChanges}.", accessAddressChanges.Count);
        _logger.LogInformation("Finished processing a total of {UnitAddressChangesCount}.", unitAddressChanges.Count);
    }

    private async Task ImportPostCodeChange(DawaPostCode postCodeChange)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var exists = false;
        if (addressProjection.PostCodeNumberToId.TryGetValue(postCodeChange.Number, out var postCodeId))
        {
            exists = true;
        }

        var postCode = _eventStore.Aggregates.Load<PostCodeAR>(postCodeId);

        DawaEntityChangeOperation? operation = null;

        if (!exists && postCodeChange.Status == DawaPostCodeStatus.Active)
        {
            operation = DawaEntityChangeOperation.Insert;
        }
        else if (postCodeChange.Status == DawaPostCodeStatus.Active)
        {
            operation = DawaEntityChangeOperation.Update;
        }
        else if (postCodeChange.Status == DawaPostCodeStatus.Discontinued)
        {
            operation = DawaEntityChangeOperation.Delete;
        }
        else
        {
            throw new InvalidOperationException("Could not figure out what has happend to the entity.");
        }

        if (operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.PostCodeNumberToId
                .ContainsKey(postCodeChange.Number))
            {
                _logger.LogWarning(
                    "Post code with number '{Number}' has already been created.",
                    postCodeChange.Number);

                return;
            }

            var postCodeAR = new PostCodeAR();
            var createResult = postCodeAR.Create(
                id: Guid.NewGuid(),
                number: postCodeChange.Number,
                name: postCodeChange.Name,
                externalCreatedDate: postCodeChange.Created,
                externalUpdatedDate: postCodeChange.Updated);

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
        else if (operation == DawaEntityChangeOperation.Update)
        {
            var postCodeAR = _eventStore.Aggregates.Load<PostCodeAR>(postCodeId);

            if (postCodeAR is not null)
            {
                var updateResult = postCodeAR.Update(
                    name: postCodeChange.Name,
                    externalUpdatedDate: postCodeChange.Updated);

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
                throw new InvalidOperationException(@$"Could not load {nameof(postCodeAR)} on {nameof(postCodeId)}: '{postCodeId}'");
            }
        }
        else if (operation == DawaEntityChangeOperation.Delete)
        {
            var postCodeAR = _eventStore.Aggregates.Load<PostCodeAR>(postCodeId);
            if (postCodeAR is not null)
            {
                var deleteResult = postCodeAR.Delete(
                    externalUpdatedDate: postCodeChange.Updated);

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

    private async Task ImportRoadChange(DawaRoad change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var exists = false;
        if (!addressProjection.RoadExternalIdIdToId.TryGetValue(change.Id.ToString(), out var roadId))
        {
            exists = true;
        }

        if (roadId == Guid.Empty)
        {
            exists = false;
        }

        DawaEntityChangeOperation? operation = null;

        if (!exists && (change.Status == DawaRoadStatus.Effective || change.Status == DawaRoadStatus.Temporary))
        {
            operation = DawaEntityChangeOperation.Insert;
        }
        else if (exists && (change.Status == DawaRoadStatus.Effective || change.Status == DawaRoadStatus.Temporary))
        {
            operation = DawaEntityChangeOperation.Update;
        }
        else if (exists && (change.Status == DawaRoadStatus.Discontinued || change.Status == DawaRoadStatus.Canceled))
        {
            operation = DawaEntityChangeOperation.Delete;
        }
        else if (!exists && (change.Status == DawaRoadStatus.Discontinued || change.Status == DawaRoadStatus.Canceled))
        {
            _logger.LogInformation("The access address has never been created so we just return. '{Data}'.", JsonSerializer.Serialize(change));
            return;
        }
        else
        {
            throw new InvalidOperationException($"Could not figure out what to do with the road change with id: Dawa id {change.Id}.");
        }

        if (operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.RoadExternalIdIdToId
                .ContainsKey(change.Id.ToString()))
            {
                _logger.LogWarning(
                    @"Road with official id '{OfficialId}' has already been created.",
                    change.Id);

                return;
            }

            var roadAR = new RoadAR();

            var createResult = roadAR.Create(
                id: Guid.NewGuid(),
                externalId: change.Id.ToString(),
                name: change.Name,
                status: DawaStatusMapper.MapRoadStatus(change.Status),
                externalCreatedDate: change.Created,
                externalUpdatedDate: change.Updated);

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
        else if (operation == DawaEntityChangeOperation.Update)
        {
            var roadAR = _eventStore.Aggregates.Load<RoadAR>(roadId);
            if (roadAR is null)
            {
                throw new InvalidOperationException(
                    $"Could not load {nameof(RoadAR)} on id '{roadId}'.");
            }

            var updateResult = roadAR.Update(
                name: change.Name,
                externalId: change.Id.ToString(),
                status: DawaStatusMapper.MapRoadStatus(change.Status),
                externalUpdatedDate: change.Updated);

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
                    change.Id.ToString());

                return;
            }
        }
        else if (operation == DawaEntityChangeOperation.Delete)
        {
            var roadAR = _eventStore.Aggregates.Load<RoadAR>(roadId);
            if (roadAR is null)
            {
                throw new InvalidOperationException(
                    $"Could not load {nameof(RoadAR)} on id '{roadId}'.");
            }

            var deleteResult = roadAR.Delete(
                externalUpdatedDate: change.Updated);

            if (deleteResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(roadAR)
                    .ConfigureAwait(false);
            }
            else
            {
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

    private async Task ImportAccessAddressChange(DawaAccessAddress change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var exists = false;
        if (addressProjection.AccessAddressExternalIdToId.TryGetValue(change.Id.ToString(), out var accessAddressId))
        {
            exists = true;
        }

        if (accessAddressId == Guid.Empty)
        {
            exists = false;
        }

        DawaEntityChangeOperation? operation = null;

        if (!exists && (change.Status == DawaStatus.Active || change.Status == DawaStatus.Pending))
        {
            operation = DawaEntityChangeOperation.Insert;
        }
        else if (exists && (change.Status == DawaStatus.Active || change.Status == DawaStatus.Pending))
        {
            operation = DawaEntityChangeOperation.Update;
        }
        else if (exists && (change.Status == DawaStatus.Canceled || change.Status == DawaStatus.Discontinued))
        {
            operation = DawaEntityChangeOperation.Delete;
        }
        else if (!exists && (change.Status == DawaStatus.Canceled || change.Status == DawaStatus.Discontinued))
        {
            _logger.LogInformation("The access address has never been created so we just return. '{Data}'.", JsonSerializer.Serialize(change));
            return;
        }
        else
        {
            throw new InvalidOperationException($"Could not figure out what to do with the access address change with id: Dawa id {change.Id}, {JsonSerializer.Serialize(change)}.");
        }

        // Important to do this outside since they're expensive computations.
        var existingRoadIds = addressProjection.GetRoadIds();
        var existingPostCodes = addressProjection.GetPostCodeIds();

        if (operation == DawaEntityChangeOperation.Insert)
        {
            if (!addressProjection.AccessAddressExternalIdToId
                .ContainsKey(change.Id.ToString()))
            {
                var accessAddressAR = new AccessAddressAR();
                if (!addressProjection.PostCodeNumberToId.TryGetValue(
                        change.PostDistrictCode, out var postCodeId))
                {
                    _logger.LogWarning(
                        @"Could not find id using official
post district code: '{PostDistrictCode}'.",
                        change.PostDistrictCode);

                    return;
                }

                if (!addressProjection.RoadExternalIdIdToId.TryGetValue(
                        change.RoadId.ToString(), out var roadId))
                {
                    _logger.LogWarning(
                        "Could not find roadId using official roadId code: '{RoadId}'.",
                        change.RoadId);

                    return;
                }

                var createResult = accessAddressAR.Create(
                    id: Guid.NewGuid(),
                    externalId: change.Id.ToString(),
                    externalCreatedDate: change.Created,
                    externalUpdatedDate: change.Updated,
                    municipalCode: change.MunicipalCode,
                    status: DawaStatusMapper.MapAccessAddressStatus(change.Status),
                    roadCode: change.RoadCode,
                    houseNumber: change.HouseNumber,
                    postCodeId: postCodeId,
                    eastCoordinate: change.EastCoordinate,
                    northCoordinate: change.NorthCoordinate,
                    supplementaryTownName: change.SupplementaryTownName,
                    plotId: change.PlotId,
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
                    change.Id);
            }
        }
        else if (operation == DawaEntityChangeOperation.Update)
        {
            var accessAddressAR = _eventStore.Aggregates
                .Load<AccessAddressAR>(accessAddressId);

            if (accessAddressAR is null)
            {
                throw new InvalidOperationException(
                    @$"Could not load {nameof(AccessAddressAR)}
 on id '{accessAddressId}'.");
            }

            if (!addressProjection.PostCodeNumberToId.TryGetValue(
                    change.PostDistrictCode, out var postCodeId))
            {
                _logger.LogWarning(
                    "Could not find id using official post district code: '{PostDistrictCode}'.",
                    change.PostDistrictCode);

                return;
            }

            if (!addressProjection.RoadExternalIdIdToId.TryGetValue(
                    change.RoadId.ToString(), out var roadId))
            {
                _logger.LogWarning(
                    "Could not find roadId using official roadId code: '{RoadId}'.",
                    change.RoadId);

                return;
            }

            var updateResult = accessAddressAR.Update(
                externalId: change.Id.ToString(),
                externalUpdatedDate: change.Updated,
                municipalCode: change.MunicipalCode,
                status: DawaStatusMapper.MapAccessAddressStatus(change.Status),
                roadCode: change.RoadCode,
                houseNumber: change.HouseNumber,
                postCodeId: postCodeId,
                eastCoordinate: change.EastCoordinate,
                northCoordinate: change.NorthCoordinate,
                supplementaryTownName: change.SupplementaryTownName,
                plotId: change.PlotId,
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
                var error = (AccessAddressError)updateResult.Errors.First();
                if (error.Code == AccessAddressErrorCode.NO_CHANGES)
                {
                    _logger.LogDebug(
                        "{ExternalId}: {ErrorMessage}",
                        accessAddressAR.ExternalId,
                        error.Message);

                    return;
                }
                else if (error.Code == AccessAddressErrorCode.CANNOT_UPDATE_DELETED)
                {
                    _logger.LogWarning(
                        "{ExternalId}: {ErrorMessage}",
                        accessAddressAR.ExternalId,
                        error.Message);
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
        else if (operation == DawaEntityChangeOperation.Delete)
        {
            _logger.LogInformation("{Id}", accessAddressId);
            var accessAddressAR = _eventStore.Aggregates.Load<AccessAddressAR>(accessAddressId);

            if (accessAddressAR is null)
            {
                throw new InvalidOperationException(@$"Could not load {nameof(AccessAddressAR)} on id '{accessAddressId}'.");
            }

            var deleteResult = accessAddressAR.Delete(externalUpdatedDate: change.Updated);

            if (deleteResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(accessAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                var error = (AccessAddressError)deleteResult.Errors.First();
                if (error.Code == AccessAddressErrorCode.CANNOT_DELETE_ALREADY_DELETED)
                {
                    _logger.LogError("{ErrorMessage}", error.Message);
                    return;
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
    }

    private async Task ImportUnitAddressChange(DawaUnitAddress change)
    {
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var exists = false;
        if (addressProjection.UnitAddressExternalIdToId.TryGetValue(change.Id.ToString(), out var unitAddressId))
        {
            exists = true;
        }

        if (unitAddressId == Guid.Empty)
        {
            exists = false;
        }

        DawaEntityChangeOperation? operation = null;

        if (!exists && (change.Status == DawaStatus.Active || change.Status == DawaStatus.Pending))
        {
            operation = DawaEntityChangeOperation.Insert;
        }
        else if (exists && (change.Status == DawaStatus.Active || change.Status == DawaStatus.Pending))
        {
            operation = DawaEntityChangeOperation.Update;
        }
        else if (exists && (change.Status == DawaStatus.Canceled || change.Status == DawaStatus.Discontinued))
        {
            operation = DawaEntityChangeOperation.Delete;
        }
        else if (!exists && (change.Status == DawaStatus.Canceled || change.Status == DawaStatus.Discontinued))
        {
            _logger.LogInformation("The unit address has never been created so we just return. '{Data}'.", JsonSerializer.Serialize(change));
            return;
        }
        else
        {
            throw new InvalidOperationException($"Could not figure out what to do with the road change with id: Dawa id {change.Id}.");
        }

        // We use the access address ids out here since the operation might be expensive.
        var existingAccessAddressIds = addressProjection.AccessAddressIds;

        if (operation == DawaEntityChangeOperation.Insert)
        {
            if (addressProjection.UnitAddressExternalIdToId
                .ContainsKey(change.Id.ToString()))
            {
                _logger.LogWarning(
                    @"Cannot create unit address
with offical id '{OfficialId}' since it already has been created.", change.Id);

                return;
            }

            if (!addressProjection.AccessAddressExternalIdToId.TryGetValue(
                    change.AccessAddressId.ToString(),
                    out var accessAddressId))
            {
                _logger.LogWarning(
                    @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                    change.AccessAddressId);

                return;
            }

            var accessAddressAr = _eventStore.Aggregates
                .Load<AccessAddressAR>(accessAddressId);

            if (accessAddressAr.Deleted)
            {
                _logger.LogError(
                    "Cannot insert unit address {UnitAddressExternalid} because the access address has been deleted {InternalAccessAddressId}.",
                    change.Id,
                    accessAddressId);

                return;
            }

            var unitAddressAR = new UnitAddressAR();

            var createResult = unitAddressAR.Create(
                id: Guid.NewGuid(),
                externalId: change.Id.ToString(),
                accessAddressId: accessAddressId,
                status: DawaStatusMapper.MapUnitAddressStatus(change.Status),
                floorName: change.FloorName,
                suiteName: change.SuitName,
                externalCreatedDate: change.Created,
                externalUpdatedDate: change.Updated,
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
        else if (operation == DawaEntityChangeOperation.Update)
        {
            if (!addressProjection.AccessAddressExternalIdToId.TryGetValue(
                    change.AccessAddressId.ToString(),
                    out var accessAddressId))
            {
                _logger.LogWarning(
                    @"Could not find accessAddress using
official accessAddressId: '{AccessAddressId}'.",
                    change.AccessAddressId);

                return;
            }

            var unitAddressAR = _eventStore.Aggregates
                .Load<UnitAddressAR>(unitAddressId);

            var updateResult = unitAddressAR.Update(
                externalId: change.Id.ToString(),
                accessAddressId: accessAddressId,
                status: DawaStatusMapper.MapUnitAddressStatus(change.Status),
                floorName: change.FloorName,
                suiteName: change.SuitName,
                externalUpdatedDate: change.Updated,
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

                var error = (UnitAddressError)updateResult.Errors.First();
                if (error.Code == UnitAddressErrorCode.NO_CHANGES)
                {
                    _logger.LogDebug(
                        "{ExternalId}: {ErrorMessage}",
                        unitAddressAR.ExternalId,
                        error.Message);

                    return;
                }
                else if (error.Code == UnitAddressErrorCode.CANNOT_UPDATE_DELETED)
                {
                    _logger.LogWarning(
                        "{ExternalId}: {ErrorMessage}",
                        unitAddressAR.ExternalId,
                        error.Message);
                }
                else
                {
                    throw new InvalidOperationException(error.Message);
                }
            }
        }
        else if (operation == DawaEntityChangeOperation.Delete)
        {
            var unitAddressAR = _eventStore.Aggregates
                .Load<UnitAddressAR>(unitAddressId);

            var deleteResult = unitAddressAR.Delete(externalUpdatedDate: change.Updated);

            if (deleteResult.IsSuccess)
            {
                await _eventStore.Aggregates
                    .StoreAsync(unitAddressAR)
                    .ConfigureAwait(false);
            }
            else
            {
                var error = (UnitAddressError)deleteResult.Errors.First();
                if (error.Code == UnitAddressErrorCode.CANNOT_DELETE_ALREADY_DELETED)
                {
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
