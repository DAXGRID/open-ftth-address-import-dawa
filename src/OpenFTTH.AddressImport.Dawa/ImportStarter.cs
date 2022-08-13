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

        _logger.LogInformation("Starting to dehydrate projections.");
        await _eventStore
            .DehydrateProjectionsAsync(cancellationToken)
            .ConfigureAwait(false);

        var lastCompletedTransactionId = await _transactionStore
            .LastCompleted(cancellationToken)
            .ConfigureAwait(false);

        var newestTransactionId = await _transactionStore
            .Newest(cancellationToken)
            .ConfigureAwait(false);

        if (lastCompletedTransactionId is null)
        {
            _logger.LogInformation("First run, so we do full import.");
            await _addressFullImport
                .Start(newestTransactionId, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await _addressChangesImport
                .Start(lastCompletedTransactionId.Value,
                       newestTransactionId,
                       cancellationToken)
                .ConfigureAwait(false);
        }

        _logger.LogInformation(
            "Storing transaction id: '{TransactionId}'.", newestTransactionId);
        var stored = await _transactionStore
            .Store(newestTransactionId)
            .ConfigureAwait(false);

        if (!stored)
        {
            throw new InvalidOperationException(
                $"Failed storing transaction id: '{newestTransactionId}'");
        }
    }
}
