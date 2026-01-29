using FakeItEasy;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;
using Xunit.Extensions.Ordering;

namespace OpenFTTH.AddressImport.Dawa.Tests;

[Order(10)]
public class ImportStarterTest : IClassFixture<DatabaseFixture>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEventStore _eventStore;
    private readonly IAddressFullImport _addressFullImport;
    private readonly IAddressChangesImport _addressChangesImport;
    private readonly ILogger<ImportStarter> _logger;
    private readonly AddressImportSettings _settings;

    public ImportStarterTest(
        IServiceProvider serviceProvider,
        IEventStore eventStore,
        IAddressFullImport addressFullImport,
        IAddressChangesImport addresssChangesImport,
        ILogger<ImportStarter> logger,
        AddressImportSettings settings)
    {
        _serviceProvider = serviceProvider;
        _eventStore = eventStore;
        _addressFullImport = addressFullImport;
        _addressChangesImport = addresssChangesImport;
        _logger = logger;
        _settings = settings;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task No_previous_transaction_id_do_full_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();
        var latestTransactionId = DateTime.UtcNow;

        A.CallTo(() => transactionStore.LastCompletedUtc(default))
            .Returns<DateTime?>(null);

        A.CallTo(() => transactionStore.NewestUtc(default))
            .Returns<DateTime>(latestTransactionId);

        A.CallTo(() => transactionStore.Store(latestTransactionId))
            .Returns<bool>(true);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport,
            settings: _settings);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressFullImport.Start(default))
            .MustHaveHappenedOnceExactly();
    }
}
