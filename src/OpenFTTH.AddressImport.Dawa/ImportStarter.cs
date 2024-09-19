using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressImport.Dawa;

public class ImportStarter
{
    private readonly ILogger<ImportStarter> _logger;
    private readonly IEventStore _eventStore;
    private readonly ITransactionStore _transactionStore;
    private readonly IAddressFullImport _addressFullImport;
    private readonly IAddressChangesImport _addressChangesImport;

    public ImportStarter(
        ILogger<ImportStarter> logger,
        IEventStore eventStore,
        ITransactionStore transactionStore,
        IAddressFullImport addressFullImport,
        IAddressChangesImport addressChangesImport)
    {
        _logger = logger;
        _addressFullImport = addressFullImport;
        _eventStore = eventStore;
        _transactionStore = transactionStore;
        _addressChangesImport = addressChangesImport;
    }

    public async Task Start(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting {Name}.", nameof(ImportStarter));

        // Init transaction store if not exists
        await _transactionStore.Init().ConfigureAwait(false);

        _logger.LogInformation("Starting to dehydrate projections.");
        await _eventStore
            .DehydrateProjectionsAsync(cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Getting last completed transaction.");
        var lastCompletedTransactionId = await _transactionStore
            .LastCompleted(cancellationToken)
            .ConfigureAwait(false);

        if (lastCompletedTransactionId is null)
        {
            var newestTransactionId = await _transactionStore
                .Newest(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "First run, so we do full import with transaction id: {TransactionId}.",
                newestTransactionId);

            await _addressFullImport
                .Start(newestTransactionId, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Storing transaction id: '{TransactionId}'.",
                newestTransactionId);

            var stored = await _transactionStore
                .Store(newestTransactionId)
                .ConfigureAwait(false);

            if (!stored)
            {
                throw new InvalidOperationException(
                    $"Failed storing transaction id: '{newestTransactionId}'");
            }
        }
        else
        {
            var newestTransactionId = await _transactionStore
                .Newest(cancellationToken)
                .ConfigureAwait(false);

            if (newestTransactionId > lastCompletedTransactionId.Value)
            {
                var transactionIds = await _transactionStore
                    .TransactionIdsAfter(lastCompletedTransactionId.Value, cancellationToken)
                    .ConfigureAwait(false);

                var lastTransactionId = lastCompletedTransactionId.Value;
                foreach (var nextTransactionId in transactionIds)
                {
                    _logger.LogInformation(
                        "Starting import from transaction range: {LastTransactionId} - {NextTransactionId}.",
                        lastTransactionId,
                        nextTransactionId);

                    await _addressChangesImport
                        .Start(nextTransactionId,
                               nextTransactionId,
                               cancellationToken)
                        .ConfigureAwait(false);

                    _logger.LogInformation(
                        "Storing transaction id: '{TransactionId}'.",
                        nextTransactionId);

                    var stored = await _transactionStore
                        .Store(nextTransactionId)
                        .ConfigureAwait(false);

                    if (!stored)
                    {
                        throw new InvalidOperationException(
                            $"Failed storing transaction id: '{nextTransactionId}'");
                    }

                    // We update the last completed transaction id to the last completed.
                    lastTransactionId = nextTransactionId;
                }
            }
            else
            {
                _logger.LogInformation(
                    "No chanes in the transaction id, skipping import. {LastTransactionId} - {NewestTransactionId}.",
                    lastCompletedTransactionId.Value,
                    newestTransactionId
                );
            }
        }
    }
}
