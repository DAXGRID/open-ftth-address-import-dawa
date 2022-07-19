using FakeItEasy;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressIndexer.Dawa.Tests;

public class ImportStarterTest
{
    [Fact]
    public async Task No_previous_transaction_id_do_full_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressImport = A.Fake<IAddressImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.GetLastId()).Returns<ulong?>(null);

        var importStarter = new ImportStarter(
            logger, addressImport, eventStore, transactionStore);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressImport.Full(default)).MustHaveHappenedOnceExactly();
        A.CallTo(() => addressImport.Changes(0, default)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Has_previous_transaction_id_do_change_import()
    {
        var logger = A.Fake<ILogger<ImportStarter>>();
        var addressImport = A.Fake<IAddressImport>();
        var eventStore = A.Fake<IEventStore>();
        var transactionStore = A.Fake<ITransactionStore>();

        A.CallTo(() => transactionStore.GetLastId()).Returns<ulong?>(50);

        var importStarter = new ImportStarter(
            logger, addressImport, eventStore, transactionStore);

        await importStarter.Start().ConfigureAwait(true);

        A.CallTo(() => addressImport.Changes(50, default)).MustHaveHappenedOnceExactly();
        A.CallTo(() => addressImport.Full(default)).MustNotHaveHappened();
    }
}
