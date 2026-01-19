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

        _logger.LogInformation("Getting last completed transaction.");
        var lastCompletedDateTime = await _transactionStore
            .LastCompleted(cancellationToken)
            .ConfigureAwait(false);

        if (lastCompletedDateTime is null)
        {
            var newestDateTime = await _transactionStore
                .Newest(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("First run, so we do full import.");

            var fullImportTimeStamp = await _addressFullImport
                .Start(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Storing timestamp: '{TimeStamp}'.",
                fullImportTimeStamp);

            var storedFullImport = await _transactionStore
                .Store(fullImportTimeStamp)
                .ConfigureAwait(false);

            if (!storedFullImport)
            {
                throw new InvalidOperationException(
                    $"Failed storing transaction id: '{fullImportTimeStamp}'");
            }

            // Get changes from last full improt to newest datetime.
            _logger.LogInformation("Starting to dehydrate projections.");
            await _eventStore
                .DehydrateProjectionsAsync(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("Getting last completed timestamp.");
            var lastCompletedTimeStamp = await _transactionStore
                .LastCompleted(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Starting to import changes from {StartTime} to {EndTime}.",
                lastCompletedTimeStamp,
                newestDateTime);

            await _addressChangesImport
                .Start(lastCompletedTimeStamp,
                       newestDateTime,
                       cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Storing datetime: '{DateTime}'.",
                newestDateTime);

            var storedChanges = await _transactionStore
                .Store(newestDateTime)
                .ConfigureAwait(false);

            if (!storedChanges)
            {
                throw new InvalidOperationException(
                    $"Failed storing date time: '{lastCompletedTimeStamp}'");
            }
        }
        else
        {
            _logger.LogInformation("Starting to dehydrate projections.");
            await _eventStore
                .DehydrateProjectionsAsync(cancellationToken)
                .ConfigureAwait(false);

            var newestDateTime = await _transactionStore
                .Newest(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Starting import from date time range: {LastCompletedDateTime} - {NewestDateTime}.",
                lastCompletedDateTime.Value,
                newestDateTime);

            await _addressChangesImport
                .Start(lastCompletedDateTime.Value,
                       newestDateTime,
                       cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Storing datetime: '{DateTime}'.",
                newestDateTime);

            var stored = await _transactionStore
                .Store(newestDateTime)
                .ConfigureAwait(false);

            if (!stored)
            {
                throw new InvalidOperationException(
                    $"Failed storing date time: '{newestDateTime}'");
            }
        }
    }
}
