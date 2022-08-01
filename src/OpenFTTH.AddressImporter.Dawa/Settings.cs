using System.Text.Json.Serialization;

namespace OpenFTTH.AddressImporter.Dawa;

internal sealed record Settings
{
    [JsonPropertyName("eventStoreConnectionString")]
    public string EventStoreConnectionString { get; init; }

    [JsonConstructor]
    public Settings(string eventStoreConnectionString)
    {
        if (string.IsNullOrWhiteSpace(eventStoreConnectionString))
        {
            throw new ArgumentException(
                "Cannot be null, empty or whitespace",
                nameof(eventStoreConnectionString));
        }

        EventStoreConnectionString = eventStoreConnectionString;
    }
}
