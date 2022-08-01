using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;
using Xunit.Extensions.Ordering;

namespace OpenFTTH.AddressImport.Dawa.Tests;

[Order(0)]
public class ImportStarterTest : IClassFixture<DatabaseFixture>
{
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
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();
        var lastCompletedTransactionId = 50UL;
        var newestTransactionId = 100UL;

        A.CallTo(() => transactionStore.LastCompleted(default))
            .Returns<ulong?>(lastCompletedTransactionId);

        A.CallTo(() => transactionStore.Newest(default))
            .Returns<ulong>(newestTransactionId);

        A.CallTo(() => transactionStore.Store(newestTransactionId))
            .Returns<bool>(true);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressChangesImport
                 .Start(lastCompletedTransactionId, newestTransactionId, default))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => addressFullImport
                 .Start(lastCompletedTransactionId, default))
            .MustNotHaveHappened();
    }

    [Fact, Order(1)]
    [Trait("Category", "Integration")]
    public async Task Full_import()
    {
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(20));

        var newestTransactionId = 3905212UL - 20000;
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
        addressProjection.AccessAddressOfficialIdToId.Count
            .Should().BeGreaterThan(100);
    }

    [Fact, Order(2)]
    [Trait("Category", "Integration")]
    public async Task Change_import()
    {
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(5));

        var newestTransactionId = 3905212UL;
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.LastCompleted(cts.Token))
            .Returns<ulong?>(newestTransactionId - 20000);

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

        await importStarter.Start(cts.Token).ConfigureAwait(true);
    }
}
