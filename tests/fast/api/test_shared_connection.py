import ctypes
import gc
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest

import duckdb


@pytest.mark.parametrize(
    "exported",
    [{}, {"address": "0", "owner": object()}, {"address": "invalid", "owner": object()}],
)
def test_rejects_invalid_shared_connection_export(exported):
    with pytest.raises(duckdb.InvalidInputException, match="shared connection"):
        duckdb.from_shared_connection(exported)


class SharedConnectionExport(ctypes.Structure):
    """The public C export protocol, with callbacks unused by rejection tests."""

    _fields_ = (
        ("version", ctypes.c_uint64),
        ("struct_size", ctypes.c_uint64),
        ("abi_id", ctypes.c_char_p),
        ("context", ctypes.c_void_p),
        ("get_connection", ctypes.c_void_p),
        ("get_database", ctypes.c_void_p),
        ("is_valid", ctypes.c_void_p),
        ("close", ctypes.c_void_p),
        ("begin_operation", ctypes.c_void_p),
        ("end_operation", ctypes.c_void_p),
        ("subscribe_close", ctypes.c_void_p),
        ("unsubscribe_close", ctypes.c_void_p),
    )


@pytest.mark.parametrize(
    ("version", "size", "abi_id", "message"),
    [
        (2, ctypes.sizeof(SharedConnectionExport), b"incompatible", "protocol"),
        (1, 16, b"incompatible", "protocol"),
        (1, ctypes.sizeof(SharedConnectionExport), b"incompatible", "native ABI"),
        (1, ctypes.sizeof(SharedConnectionExport), b"", "native ABI"),
        (1, ctypes.sizeof(SharedConnectionExport), None, "native ABI"),
    ],
)
def test_rejects_incompatible_export_before_calling_producer(version, size, abi_id, message):
    owner = SharedConnectionExport(version=version, struct_size=size, abi_id=abi_id)
    exported = {"address": str(ctypes.addressof(owner)), "owner": owner}
    with pytest.raises(duckdb.InvalidInputException, match=message):
        duckdb.from_shared_connection(exported)


def test_python_export_preserves_session_and_transactions():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    consumer = duckdb.from_shared_connection(exported)
    assert type(consumer) is duckdb.DuckDBPyConnection
    producer.execute("CREATE TABLE shared_values(value BIGINT)")
    producer.begin()
    consumer.execute("INSERT INTO shared_values VALUES (9007199254740993)")
    assert producer.execute("SELECT * FROM shared_values").fetchall() == [(9007199254740993,)]
    consumer.rollback()
    assert producer.execute("SELECT * FROM shared_values").fetchall() == []
    producer.execute("CREATE TEMP TABLE temporary_values AS SELECT 42 AS value")
    assert consumer.execute("SELECT * FROM temporary_values").fetchall() == [(42,)]
    consumer.close()
    with pytest.raises(duckdb.ConnectionException):
        producer.execute("SELECT 1")


def test_python_export_owner_retains_native_connection():
    producer = duckdb.connect()
    producer.execute("CREATE TEMP TABLE retained_values AS SELECT 42 AS value")
    exported = duckdb.export_connection(producer)
    del producer
    gc.collect()
    consumer = duckdb.from_shared_connection(exported)
    del exported
    gc.collect()
    assert consumer.execute("SELECT * FROM retained_values").fetchall() == [(42,)]
    consumer.close()


def test_python_export_roundtrip_uses_canonical_producer():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    imported = duckdb.from_shared_connection(exported)
    forwarded = duckdb.export_connection(imported)
    assert forwarded["address"] == exported["address"]
    second = duckdb.from_shared_connection(forwarded)
    producer.close()
    for connection in [imported, second]:
        with pytest.raises(duckdb.ConnectionException):
            connection.execute("SELECT 1")


def test_python_export_rejects_closed_connection():
    producer = duckdb.connect()
    producer.close()
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        duckdb.export_connection(producer)


def test_python_export_operation_refuses_reentrancy():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    api = SharedConnectionExport.from_address(int(exported["address"]))
    begin = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)(api.begin_operation)
    end = ctypes.CFUNCTYPE(None, ctypes.c_void_p)(api.end_operation)
    producer.execute("SELECT * FROM range(100000)")
    assert begin(api.context) is None
    try:
        with pytest.raises(duckdb.ResourceInUseException, match="busy"):
            producer.execute("SELECT 1")
        assert begin(api.context) is not None
    finally:
        end(api.context)
    assert producer.execute("SELECT 42").fetchall() == [(42,)]
    producer.close()


def test_python_export_notifies_before_close_and_allows_unsubscribe_in_callback():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    api = SharedConnectionExport.from_address(int(exported["address"]))
    valid = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p)(api.is_valid)
    unsubscribe = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint64)(api.unsubscribe_close)
    subscribed = ctypes.c_uint64()
    observed = []

    @ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_void_p)
    def before_close(_):
        observed.append(valid(api.context))
        unsubscribe(api.context, subscribed.value)
        return None

    subscribe = ctypes.CFUNCTYPE(
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64)
    )(api.subscribe_close)
    assert subscribe(api.context, before_close, None, ctypes.byref(subscribed)) is None
    producer.close()
    producer.close()
    assert observed == [0]
    assert valid(api.context) == 0


def test_python_export_close_from_another_thread_preserves_connection():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pytest.raises(duckdb.ConnectionException, match="exporting thread"),
    ):
        pool.submit(producer.close).result(timeout=5)
    assert producer.execute("SELECT 42").fetchall() == [(42,)]
    duckdb.from_shared_connection(exported).close()


@pytest.mark.parametrize("method", ["execute", "fetchall", "close"])
def test_exported_connection_stays_on_owner_thread_after_export_gc(method):
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)
    del exported
    gc.collect()
    producer.execute("SELECT 42")
    args = ["SELECT 1"] if method == "execute" else []
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pytest.raises(duckdb.ConnectionException, match="exporting thread"),
    ):
        pool.submit(getattr(producer, method), *args).result(timeout=5)
    assert producer.fetchall() == [(42,)]
    producer.close()


def test_import_refuses_a_preexported_connection_from_its_query_callback():
    producer = duckdb.connect()
    try:
        producer.execute("SET threads = 1")
        exported = duckdb.export_connection(producer)

        def import_during_query():
            return duckdb.from_shared_connection(exported) is producer

        producer.create_function("import_during_query", import_during_query, [], "BOOLEAN")
        with pytest.raises(duckdb.InvalidInputException, match="code running inside a query"):
            producer.execute("SELECT import_during_query()").fetchall()
        assert producer.execute("SELECT 42").fetchall() == [(42,)]
    finally:
        producer.close()


def test_closed_export_does_not_retain_catalog_callables():
    producer = duckdb.connect()
    exported = duckdb.export_connection(producer)

    def keeps_export_alive():
        return exported["address"]

    retained = weakref.ref(keeps_export_alive)
    producer.create_function("keeps_export_alive", keeps_export_alive, [], "VARCHAR")
    del keeps_export_alive
    producer.close()
    gc.collect()
    assert retained() is None
