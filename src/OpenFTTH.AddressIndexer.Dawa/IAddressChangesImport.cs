namespace OpenFTTH.AddressIndexer.Dawa;

public interface IAddressChangesImport
{
    Task Start(ulong lastTransactionId, CancellationToken cancellation = default);
}
