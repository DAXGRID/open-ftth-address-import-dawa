using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressIndexer.Dawa;

internal class Startup
{
    private ILogger<Startup> _logger;
    private IAddressImport _addressImport;

    public Startup(ILogger<Startup> logger, IAddressImport addressImport)
    {
        _logger = logger;
        _addressImport = addressImport;
    }

    public async Task Start(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting {Name}.", nameof(Startup));

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


        await Task.CompletedTask.ConfigureAwait(false);
    }
}
