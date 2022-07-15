using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa;

internal class Startup
{
    private ILogger<Startup> _logger;
    private IAddressImport _addressImport;
    private IEventStore _eventStore;

    public Startup(
        ILogger<Startup> logger,
        IAddressImport addressImport,
        IEventStore eventStore)
    {
        _logger = logger;
        _addressImport = addressImport;
        _eventStore = eventStore;
    }

    public async Task Start(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting {Name}.", nameof(Startup));

        await _eventStore.DehydrateProjectionsAsync().ConfigureAwait(false);

        var lastRunTransctionId = await TransactionStore
            .GetLastRunTransactionId()
            .ConfigureAwait(false);

        if (lastRunTransctionId is null)
        {
            _logger.LogInformation(
                "{LastRunTransactionId} is null so we do full import.",
                nameof(lastRunTransctionId));

            await _addressImport.Full(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Changes import
        }
    }
}
