using FluentAssertions;
using Xunit.Extensions.Ordering;

namespace OpenFTTH.AddressImporter.Dawa.Tests;

[Order(10)]
public class PostgresTransactionStoreTest : IClassFixture<DatabaseFixture>
{
    [Fact, Order(1)]
    [Trait("Category", "Integration")]
    public async Task Transaction_store_flow()
    {
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(1));

        using var httpClient = new HttpClient();

        var transactionStore = new PostgresTransactionStore(
            httpClient, new(DatabaseFixture.TestDbConnectionString));

        await transactionStore.Init().ConfigureAwait(true);

        var newestTransactionId = await transactionStore
            .Newest()
            .ConfigureAwait(true);

        // Store transactions (multiple to make sure we get latest)
        await transactionStore
            .Store(newestTransactionId - 50000)
            .ConfigureAwait(true);

        await transactionStore
            .Store(newestTransactionId - 40000)
            .ConfigureAwait(true);

        await transactionStore
            .Store(newestTransactionId - 20000)
            .ConfigureAwait(true);

        await transactionStore
            .Store(newestTransactionId - 10000)
            .ConfigureAwait(true);

        await transactionStore
            .Store(newestTransactionId)
            .ConfigureAwait(true);

        var lastCompletedTransactionId = await transactionStore
            .LastCompleted()
            .ConfigureAwait(true);

        lastCompletedTransactionId.Should().Be(newestTransactionId);
    }
}
