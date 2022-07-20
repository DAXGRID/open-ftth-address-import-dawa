#  OpenFTTH Address Indexer DAWA

## Testing

Setting up Postgres.

```sh
docker run --name test-postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB="test_db" \
    -p 5432:5432 \
    -d postgres
```
