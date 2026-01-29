using Npgsql;

namespace OpenFTTH.AddressImport.Dawa;

internal class PostgresTransactionStore : ITransactionStore
{
    private string _connectionString;
    private const string _schemaName = "datafordeleren_address_import";
    private const string _tableName = "transaction_store";

    public PostgresTransactionStore(AddressImportSettings settings)
    {
        _connectionString = settings.EventStoreConnectionString;
    }

    public async Task<DateTime?> LastCompletedUtc(CancellationToken cancellationToken = default)
    {
        const string queryLastCompleted =
            @$"SELECT timestamp
               FROM {_schemaName}.{_tableName}
               ORDER BY id DESC
               LIMIT 1";

        using var connection = new NpgsqlConnection(_connectionString);
        using var cmd = new NpgsqlCommand(queryLastCompleted, connection);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var result = await cmd
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false) as DateTime?;

        return result is not null ? result.Value.ToUniversalTime() : null;
    }

    public Task<DateTime> NewestUtc(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(DateTime.UtcNow);
    }

    public async Task<bool> Store(DateTime timestamp)
    {
        const string insertSql =
            $@"INSERT INTO {_schemaName}.{_tableName} (
                 timestamp)
               VALUES (
                 @timestamp)";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(insertSql, connection);
        cmd.Parameters.AddWithValue("@timestamp", timestamp);

        return (await cmd.ExecuteNonQueryAsync().ConfigureAwait(false)) == 1;
    }

    public async Task Init(bool enableMigration)
    {
        if (!(await SchemaExist(_schemaName).ConfigureAwait(false)))
        {
            await InitSchemaAndTable().ConfigureAwait(false);

            if (enableMigration)
            {
                // The old schema.
                // This is done becaues before we used the Dataforsyningen provider.
                // They closed down, and we had to switch to Datafordeleren.
                // Datafordeleren does not store the data using a transaction ID, but a date time.
                // We query the last timestamp for the transaction ID and insert it into the new datastore if no data exists in it.
                // That way we can switch over without anything manual having to be done.
                if ((await SchemaExist("address_import").ConfigureAwait(false)))
                {
                    var lastTransactionTimeStamp = await MigrationGetLastTransactionTimeStamp().ConfigureAwait(false);
                    if (lastTransactionTimeStamp is not null)
                    {
                        await Store(lastTransactionTimeStamp.Value).ConfigureAwait(false);
                    }
                }
            }
        }
    }

    private async Task InitSchemaAndTable()
    {
        const string schemaSetup =
            $@"CREATE SCHEMA {_schemaName};
               CREATE TABLE {_schemaName}.{_tableName} (
                 id SERIAL PRIMARY KEY,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 timestamp TIMESTAMPTZ NOT NULL);";

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

    private async Task<DateTime?> MigrationGetLastTransactionTimeStamp()
    {
        string getLatestTimeStamp = @"SELECT created_at
         FROM address_import.transaction_store
         ORDER BY id DESC
         LIMIT 1";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(getLatestTimeStamp, connection);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return (DateTime?)result;
    }

    private async Task<bool> SchemaExist(string schemaName)
    {
        string schemaExistsQuery =
            @$"SELECT schema_name
               FROM information_schema.schemata
               WHERE schema_name = '{schemaName}'";

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new NpgsqlCommand(schemaExistsQuery, connection);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return result is not null;
    }
}
