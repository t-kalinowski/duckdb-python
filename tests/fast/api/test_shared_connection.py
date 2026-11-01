import ctypes

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
