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
    const int REMOVE_TRANSACTION = 500;
    const ulong GLOBAL_TRANSACTION_ID = 3954964UL;

    private readonly IServiceProvider _serviceProvider;
    private readonly IEventStore _eventStore;
    private readonly IAddressFullImport _addressFullImport;
    private readonly IAddressChangesImport _addressChangesImport;
    private readonly ILogger<ImportStarter> _logger;

    public ImportStarterTest(
        IServiceProvider serviceProvider,
        IEventStore eventStore,
        IAddressFullImport addressFullImport,
        IAddressChangesImport addresssChangesImport,
        ILogger<ImportStarter> logger)
    {
        _serviceProvider = serviceProvider;
        _eventStore = eventStore;
        _addressFullImport = addressFullImport;
        _addressChangesImport = addresssChangesImport;
        _logger = logger;
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
        var latestTransactionId = 250000UL;

        A.CallTo(() => transactionStore.LastCompleted(default))
            .Returns<ulong?>(null);

        A.CallTo(() => transactionStore.Newest(default))
            .Returns<ulong>(latestTransactionId);

        A.CallTo(() => transactionStore.Store(latestTransactionId))
            .Returns<bool>(true);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressFullImport.Start(latestTransactionId, default))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => addressChangesImport.Start(default, latestTransactionId, default))
            .MustNotHaveHappened();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Has_previous_transaction_id_do_change_import()
    {
        using var source = new CancellationTokenSource();
        var token = source.Token;

        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();
        var lastCompletedTransactionId = 50UL;
        var transactionIdsAfter = new List<ulong> { 51, 52, 53, 54, 55, 56 };

        A.CallTo(() => transactionStore.LastCompleted(token))
            .Returns<ulong?>(lastCompletedTransactionId);

        A.CallTo(() => transactionStore.TransactionIdsAfter(lastCompletedTransactionId, token))
            .Returns<List<ulong>>(transactionIdsAfter);

        A.CallTo(() => transactionStore.Newest(token))
            .Returns<ulong>(transactionIdsAfter.Last());

        foreach (var transactionId in transactionIdsAfter)
        {
            A.CallTo(() => transactionStore.Store(transactionId))
                .Returns<bool>(true);
        }

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start(token).ConfigureAwait(true);

        // This is a bit ugly, we just want to make sure that each transaction id is used to call address changes.
        foreach (var tId in transactionIdsAfter)
        {
            A.CallTo(() => addressChangesImport
                     .Start(tId, tId, token))
                .MustHaveHappenedOnceExactly();

            lastCompletedTransactionId = tId;
        }

        A.CallTo(() => addressFullImport
                 .Start(lastCompletedTransactionId, token))
            .MustNotHaveHappened();
    }

    [Fact, Order(1)]
    [Trait("Category", "Integration")]
    public async Task Full_import()
    {
        _eventStore.ScanForProjections();

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(60));

        var newestTransactionId = GLOBAL_TRANSACTION_ID - REMOVE_TRANSACTION;
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.LastCompleted(cts.Token))
            .Returns<ulong?>(null);

        A.CallTo(() => transactionStore.Newest(cts.Token))
            .Returns<ulong>(newestTransactionId);

        A.CallTo(() => transactionStore.Store(newestTransactionId))
            .Returns<bool>(true);

        var importStarter = new ImportStarter(
            logger: _logger,
            eventStore: _eventStore,
            transactionStore: transactionStore,
            addressFullImport: _addressFullImport,
            addressChangesImport: _addressChangesImport);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        await importStarter.Start(cts.Token).ConfigureAwait(true);

        addressProjection.GetPostCodeIds().Count
            .Should().BeGreaterThan(100);
        addressProjection.GetRoadIds().Count
            .Should().BeGreaterThan(100);
        addressProjection.AccessAddressIds.Count
            .Should().BeGreaterThan(100);
        addressProjection.AccessAddressExternalIdToId.Count
            .Should().BeGreaterThan(100);
    }

    [Fact, Order(2)]
    [Trait("Category", "Integration")]
    public async Task Change_import()
    {
        _eventStore.ScanForProjections();

        // This is ugly, but we want a clean projection for running this test.
        var addressProjection = _eventStore.Projections.Get<AddressProjection>();
        addressProjection.AccessAddressIds.Clear();
        addressProjection.PostCodeNumberToId.Clear();
        addressProjection.UnitAddressExternalIdToId.Clear();
        addressProjection.AccessAddressExternalIdToId.Clear();
        addressProjection.RoadExternalIdIdToId.Clear();
        // end of ugly

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromHours(8));

        var newestTransactionId = GLOBAL_TRANSACTION_ID;
        var transactionStore = A.Fake<ITransactionStore>();

        // We never have numbers bigger than int in the tests, so this is safe.
        var transactionIdsAfter = Enumerable
            .Range((int)newestTransactionId - REMOVE_TRANSACTION, (int)GLOBAL_TRANSACTION_ID)
            .Select(x => (ulong)x)
            .ToList();

        var lastCompletedTransactionId = newestTransactionId - REMOVE_TRANSACTION;

        A.CallTo(() => transactionStore.LastCompleted(cts.Token))
             .Returns<ulong?>(lastCompletedTransactionId);

        A.CallTo(() => transactionStore.Newest(cts.Token))
            .Returns<ulong>(newestTransactionId);

        A.CallTo(() => transactionStore.TransactionIdsAfter(lastCompletedTransactionId, cts.Token))
            .Returns<List<ulong>>(transactionIdsAfter);

        foreach (var transactionIdAfter in transactionIdsAfter)
        {
            A.CallTo(() => transactionStore.Store(transactionIdAfter))
                .Returns<bool>(true);
        }

        var importStarter = new ImportStarter(
            logger: _logger,
            eventStore: _eventStore,
            transactionStore: transactionStore,
            addressFullImport: _addressFullImport,
            addressChangesImport: _addressChangesImport);

        await importStarter.Start(cts.Token).ConfigureAwait(true);
    }
}
