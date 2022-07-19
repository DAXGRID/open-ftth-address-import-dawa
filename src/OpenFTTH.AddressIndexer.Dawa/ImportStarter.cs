using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa;

public class ImportStarter
{
    private readonly ILogger<ImportStarter> _logger;
    private readonly IAddressImport _addressImport;
    private readonly IEventStore _eventStore;
    private readonly ITransactionStore _transactionStore;

    public ImportStarter(
        ILogger<ImportStarter> logger,
        IAddressImport addressImport,
        IEventStore eventStore,
        ITransactionStore transactionStore)
    {
        _logger = logger;
        _addressImport = addressImport;
        _eventStore = eventStore;
        _transactionStore = transactionStore;
    }

    public async Task Start(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting {Name}.", nameof(ImportStarter));

        var lastTransctionId = await _transactionStore
            .GetLastId()
            .ConfigureAwait(false);

        if (lastTransctionId is null)
        {
            _logger.LogInformation("First run so we do full import.");
            await _addressImport.Full(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // We only need to dehydrate if we are getting changeset.
            await _eventStore.DehydrateProjectionsAsync().ConfigureAwait(false);

            _logger.LogInformation("Importing from {LastTransactionId}.", lastTransctionId);
            await _addressImport.Changes(lastTransctionId.Value, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
