using Npgsql;

namespace OpenFTTH.AddressImporter.Dawa.Tests;

internal class DatabaseFixture
{
    private const string _testDbName = "test_db";
    private const string _masterConnectionString =
        "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=master";
    public const string TestDbConnectionString =
        "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=test_db";

    public DatabaseFixture()
    {
        DeleteDatabase();
        SetupDatabase();
    }

    private static void SetupDatabase()
    {
        var createDbSql = $"CREATE DATABASE {_testDbName}";
        using var connection = new NpgsqlConnection(_masterConnectionString);
        using var cmd = new NpgsqlCommand(createDbSql, connection);
        connection.Open();
        cmd.ExecuteNonQuery();
    }

    private static void DeleteDatabase()
    {
        var dropDbSql = $"DROP DATABASE IF EXISTS {_testDbName}";
        using var connection = new NpgsqlConnection(_masterConnectionString);
        using var cmd = new NpgsqlCommand(dropDbSql, connection);
        connection.Open();
        cmd.ExecuteNonQuery();
    }
}
