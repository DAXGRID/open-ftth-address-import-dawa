using System.Text.Json.Serialization;

namespace OpenFTTH.AddressImport.Dawa;

public sealed record AddressImportSettings
{
    [JsonPropertyName("eventStoreConnectionString")]
    public string EventStoreConnectionString { get; init; }

    [JsonPropertyName("datafordelerApiKey")]
    public string DatafordelerApiKey { get; init; }

    [JsonPropertyName("enableMigration")]
    public bool EnableMigration { get; init; }

    [JsonConstructor]
    public AddressImportSettings(string eventStoreConnectionString, string datafordelerApiKey, bool enableMigration)
    {
        if (string.IsNullOrWhiteSpace(eventStoreConnectionString))
        {
            throw new ArgumentException(
                "Cannot be null, empty or whitespace",
                nameof(eventStoreConnectionString));
        }

        if (string.IsNullOrWhiteSpace(datafordelerApiKey))
        {
            throw new ArgumentException(
                "Cannot be null, empty or whitespace",
                nameof(datafordelerApiKey));
        }

        EventStoreConnectionString = eventStoreConnectionString;
        DatafordelerApiKey = datafordelerApiKey;
        EnableMigration = enableMigration;
    }
}
