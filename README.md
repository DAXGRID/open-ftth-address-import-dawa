#  OpenFTTH Address Indexer DAWA

## Testing

Running all tests

```sh
dotnet test
```

### Running unit tests

```
dotnet test --filter Category=Unit
```

### Running integration tests

```
dotnet test --filter Category=Integration
```

Setting up Postgres for running integration-tests.

```sh
docker run --name test-postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB="master" \
    -p 5432:5432 \
    -d postgres
```
