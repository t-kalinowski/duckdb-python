# Sharing a native connection

`duckdb.from_shared_connection(exported)` imports a live native DuckDB connection from another binding in the same process. It returns a `DuckDBPyConnection`, with the usual Python parameter conversion, result conversion, relation API, and DB-API methods. The imported connection uses the producer's session, so temporary tables, settings, and transactions are shared.

An exporter supplies a dictionary with two entries:

- `address`: the decimal address of a `duckdb_shared_connection` descriptor, defined in [duckdb_shared_connection.h](src/include/duckdb_python/duckdb_shared_connection.h).
- `owner`: an object that keeps that descriptor, its callbacks, and the producer's connection and database alive.

For example, the R exporter can be passed through reticulate:

```r
con <- DBI::dbConnect(duckdb::duckdb())
py_duckdb <- reticulate::import("duckdb", convert = FALSE)
py_con <- py_duckdb$from_shared_connection(
  duckdb::duckdb_export_connection(con, environment = globalenv())
)

DBI::dbBegin(con)
py_con$execute("CREATE TEMP TABLE shared_values AS SELECT 42 AS value")
DBI::dbGetQuery(con, "SELECT * FROM shared_values")
DBI::dbRollback(con)
py_con$close()
```

The R connection continues to support DBI, R data-frame registration, and lookup in the exported environment. A Python lazy relation retains the data frames it found when it bound, even if their R names are rebound or removed. Python binds parameters and converts results through its own native implementation. The producer's operation callbacks establish its environment and check that the calling thread can use it.

The native `SharedConnectionScanState` interface lets the producer participate in the consumer's lazy source capture. This version supports one source-capture consumer implementation per session; multiple Python imports share that state.

## Ownership and close

Garbage collecting the Python wrapper releases its reference to `owner`; it does not explicitly close the producer's connection. Relations, results, and derived cursors retain the owner while they need it. An explicit `close()` on an imported connection asks the producer to close the shared session. Closing it from the producer also invalidates its Python imports and ends their pending results before the native connection is destroyed.

As with other DuckDB Python connections, `cursor()` creates a separate connection to the same database, with a separate transaction. It retains the producer's owner and operation callbacks. Closing the shared producer also closes these derived cursors. Closing a derived cursor affects only that cursor.

Use an imported connection and its derived cursors on the thread that imported it. The producer can impose additional restrictions; the R exporter requires R's main thread and rejects reentrant operations. A Python UDF that calls R must also run on that thread, for example with `SET threads = 1`. Ordinary native queries can still run in parallel.

## Compatibility

This API shares C++ objects. Import validates the protocol version, descriptor size, and native ABI identifier before calling the producer or reading its native connection. The identifier includes the DuckDB source revision, platform, compiler, C++ standard library, and selected object layouts. Unsupported build environments or mismatched identifiers raise `InvalidInputException` with an incompatibility message. A caller can then choose an adapter or a separate connection explicitly.

Both bindings must be built against compatible DuckDB sources and C++ settings. A matching package version alone is insufficient. The protocol does not make independently built, arbitrary DuckDB releases binary compatible.

The descriptor address is trusted process-local input, like an Arrow pointer import. It must come from a cooperating exporter and remain valid through `owner`. Do not deserialize addresses from files, accept them from another process, or construct them from untrusted input.
