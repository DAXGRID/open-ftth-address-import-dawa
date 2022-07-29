using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;
using Xunit.Extensions.Ordering;

namespace OpenFTTH.AddressIndexer.Dawa.Tests;

public class ImportStarterTest
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEventStore _eventStore;
    private readonly IAddressFullImport _addressFullImport;
    private readonly IAddressChangesImport _addressChangesImport;
    private readonly ITransactionStore _transactionStore;
    private readonly ILogger<ImportStarter> _logger;

    public ImportStarterTest(
        IServiceProvider serviceProvider,
        IEventStore eventStore,
        IAddressFullImport addressFullImport,
        IAddressChangesImport addresssChangesImport,
        ITransactionStore transactionStore,
        ILogger<ImportStarter> logger)
    {
        _serviceProvider = serviceProvider;
        _eventStore = eventStore;
        _addressFullImport = addressFullImport;
        _addressChangesImport = addresssChangesImport;
        _transactionStore = transactionStore;
        _logger = logger;
    }

    [Fact]
    public async Task No_previous_transaction_id_do_full_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();
        var latestTransactionId = 250000UL;

        A.CallTo(() => transactionStore.GetLastCompletedTransactionId(default))
            .Returns<ulong?>(null);

        A.CallTo(() => transactionStore.GetNewestTransactionId(default))
            .Returns<ulong>(latestTransactionId);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressFullImport.Start(latestTransactionId, default))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => addressChangesImport.Start(default, default, default))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task Has_previous_transaction_id_do_change_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();
        var lastCompletedTransactionId = 50UL;

        A.CallTo(() => transactionStore.GetLastCompletedTransactionId(default))
            .Returns<ulong?>(lastCompletedTransactionId);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressChangesImport.Start(lastCompletedTransactionId, default, default))
            .MustHaveHappenedOnceExactly();
        A.CallTo(() => addressFullImport.Start(lastCompletedTransactionId, default))
            .MustNotHaveHappened();
    }

    [Fact, Order(1)]
    public async Task Full_import()
    {
        // We cancel after 20 mins, to indicate something is wrong.
        // Since it should not take any longer than ~10mins.
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(20));

        var importStarter = new ImportStarter(
            logger: _logger,
            eventStore: _eventStore,
            transactionStore: _transactionStore,
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
    public async Task Change_import()
    {
        // We cancel after 20 mins, to indicate something is wrong.
        // Since it should not take any longer than ~10mins.
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(20));

        var transactionId = 3905212UL;
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.GetLastCompletedTransactionId(default))
            .Returns<ulong?>(transactionId - 20000);

        A.CallTo(() => transactionStore.GetNewestTransactionId(default))
            .Returns<ulong>(transactionId);

        var importStarter = new ImportStarter(
            logger: _logger,
            eventStore: _eventStore,
            transactionStore: transactionStore,
            addressFullImport: _addressFullImport,
            addressChangesImport: _addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => transactionStore.StoreTransactionId(transactionId))
            .MustHaveHappened();
    }
}
