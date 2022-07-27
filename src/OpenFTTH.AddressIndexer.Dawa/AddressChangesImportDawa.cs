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
        ulong lastTransactionId,
        CancellationToken cancellation = default)
    {
        var latestTransaction = await _dawaClient
            .GetLatestTransactionAsync(cancellation)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Getting changes from '{LastTransactionId} to {LastestTransactionId}'.",
            lastTransactionId,
            latestTransaction.Id);

        var changesPostCodesCount = await ImportChangesPostCodes(
            lastTransactionId, latestTransaction, cancellation).ConfigureAwait(false);

        _logger.LogInformation(
            "Finished handling '{Count}' post code changes.",
            changesPostCodesCount);
    }

    private async Task<int> ImportChangesPostCodes(
        ulong lastTransactionId,
        DawaTransaction latestTransaction,
        CancellationToken cancellation)
    {
        var changesPostCodesAsyncEnumerable = _dawaClient.GetChangesPostCodesAsync(
            latestTransaction.Id, lastTransactionId, cancellation).ConfigureAwait(false);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        var count = 0;
        await foreach (var postCodeChange in changesPostCodesAsyncEnumerable)
        {
            if (postCodeChange.Operation == DawaEntityChangeOperation.Insert)
            {
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
                    throw new InvalidOperationException(
                        createResult.Errors.FirstOrDefault()?.Message);
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
                    postCodeAR.Update(postCodeChange.Data.Name);
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
                    postCodeAR.Delete();
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
}
