using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa.Tests;

public class ImportStarterTest
{
    private readonly ImportStarter _importStarter;
    private readonly IEventStore _eventStore;

    public ImportStarterTest(ImportStarter importStarter, IEventStore eventStore)
    {
        _importStarter = importStarter;
        _eventStore = eventStore;
    }

    [Fact]
    public async Task No_previous_transaction_id_do_full_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.GetLastId()).Returns<ulong?>(null);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressFullImport.Start(default)).MustHaveHappenedOnceExactly();
        A.CallTo(() => addressChangesImport.Start(0, default)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Has_previous_transaction_id_do_change_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressFullImport = A.Fake<IAddressFullImport>();
        var addressChangesImport = A.Fake<IAddressChangesImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.GetLastId()).Returns<ulong?>(50);

        var importStarter = new ImportStarter(
            logger: logger,
            eventStore: eventStore,
            transactionStore: transactionStore,
            addressFullImport: addressFullImport,
            addressChangesImport: addressChangesImport);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressChangesImport.Start(50, default))
            .MustHaveHappenedOnceExactly();
        A.CallTo(() => addressFullImport.Start(default))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task Full_import()
    {
        // We cancel after 20 mins, to indicate something is wrong.
        // Since it should not take any longer than ~10mins.
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(20));

        await _importStarter.Start(cts.Token).ConfigureAwait(true);

        var addressProjection = _eventStore.Projections.Get<AddressProjection>();

        addressProjection.GetPostCodeIds().Count.Should().BeGreaterThan(100);
        addressProjection.GetRoadIds().Count.Should().BeGreaterThan(100);
        addressProjection.AccessAddressIds.Count.Should().BeGreaterThan(100);
    }
}
