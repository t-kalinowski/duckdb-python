import gc
import weakref

import duckdb


def test_udf_survives_registering_connection_gc():
    def increment(value):
        return value + 1

    retained = weakref.ref(increment)
    connection = duckdb.connect()
    connection.create_function("increment", increment, ["BIGINT"], "BIGINT")
    cursor = connection.cursor()
    del increment, connection
    gc.collect()

    assert retained() is not None
    assert cursor.execute("SELECT increment(41)").fetchall() == [(42,)]
    cursor.close()
    gc.collect()
    assert retained() is None
