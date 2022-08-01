using DawaAddress;
using Npgsql;

namespace OpenFTTH.AddressImporter.Dawa;

internal class PostgresTransactionStore : ITransactionStore
{
    private DawaClient _dawaClient;
    private string _connectionString;
    private const string _schemaName = "address_indexer";
    private const string _tableName = "transaction_store";

    public PostgresTransactionStore(HttpClient httpClient, Settings settings)
    {
        _connectionString = settings.EventStoreConnectionString;
        _dawaClient = new DawaClient(httpClient);
    }

    public async Task<ulong?> LastCompleted(CancellationToken cancellationToken = default)
    {
        const string queryLastCompleted =
            @$"SELECT transaction_id
               FROM {_schemaName}.{_tableName}
               ORDER BY transaction_id DESC
               LIMIT 1";

        using var connection = new NpgsqlConnection(_connectionString);
        using var cmd = new NpgsqlCommand(queryLastCompleted, connection);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var result = await cmd
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false) as long?;

        return result is not null ? (ulong)result : null;
    }

    public async Task<ulong> Newest(CancellationToken cancellationToken = default)
    {
        var transaction = await _dawaClient
            .GetLatestTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        return transaction.Id;
    }

    public async Task<bool> Store(ulong transactionId)
    {
        if (transactionId > long.MaxValue)
        {
            throw new ArgumentException(
                $"Cannot store value bigger than {long.MaxValue}",
                nameof(transactionId));
        }

        const string insertSql =
            $@"INSERT INTO {_schemaName}.{_tableName} (
                 transaction_id)
               VALUES (
                 @transactionId)";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(insertSql, connection);
        cmd.Parameters.AddWithValue("@transactionId", (long)transactionId);

        return (await cmd.ExecuteNonQueryAsync().ConfigureAwait(false)) == 1;
    }

    public async Task Init()
    {
        if (!(await SchemaExist().ConfigureAwait(false)))
        {
            await InitSchemaAndTable().ConfigureAwait(false);
        }
    }

    private async Task InitSchemaAndTable()
    {
        const string schemaSetup =
            $@"CREATE SCHEMA {_schemaName};
               CREATE TABLE {_schemaName}.{_tableName} (
                 id SERIAL PRIMARY KEY,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 transaction_id BIGINT CHECK (transaction_id > 0));";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var transaction = await connection
                      .BeginTransactionAsync()
                      .ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(schemaSetup, connection, transaction);

        var result = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        if (result == 0)
        {
            await transaction.RollbackAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                "Failed setting up schema and table.");
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    private async Task<bool> SchemaExist()
    {
        const string schemaExistsQuery =
            @$"SELECT schema_name
               FROM information_schema.schemata
               WHERE schema_name = '{_schemaName}'";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(schemaExistsQuery, connection);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return result is not null;
    }
}
