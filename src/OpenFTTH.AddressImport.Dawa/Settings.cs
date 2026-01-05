using System.Text.Json.Serialization;

namespace OpenFTTH.AddressImport.Dawa;

internal sealed record Settings
{
    [JsonPropertyName("eventStoreConnectionString")]
    public string EventStoreConnectionString { get; init; }

    [JsonPropertyName("datafordelerApiKey")]
    public string DatafordelerApiKey { get; init; }

    [JsonConstructor]
    public Settings(string eventStoreConnectionString, string datafordelerApiKey)
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
    }
}
