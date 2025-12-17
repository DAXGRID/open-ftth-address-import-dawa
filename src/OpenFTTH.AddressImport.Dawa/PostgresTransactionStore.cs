using Npgsql;

namespace OpenFTTH.AddressImport.Dawa;

internal class PostgresTransactionStore : ITransactionStore
{
    private string _connectionString;
    private const string _schemaName = "datafordeleren_address_import";
    private const string _tableName = "transaction_store";

    public PostgresTransactionStore(HttpClient httpClient, Settings settings)
    {
        _connectionString = settings.EventStoreConnectionString;
    }

    public async Task<DateTime?> LastCompleted(CancellationToken cancellationToken = default)
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

        return result is not null ? result : null;
    }

    public async Task<DateTime> Newest(CancellationToken cancellationToken = default)
    {
        return DateTime.UtcNow;
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
