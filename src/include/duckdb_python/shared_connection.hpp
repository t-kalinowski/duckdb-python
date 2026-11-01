#pragma once

#include "duckdb_python/duckdb_shared_connection.h"
#include "duckdb_python/registered_py_object.hpp"

#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace duckdb {

class Connection;
class DuckDB;
class ClientContext;
class DuckDBPyConnection;
class PythonConnectionExport;

nb::dict ExportSharedConnection(const std::shared_ptr<DuckDBPyConnection> &connection);
void CloseSharedConnectionExports(DuckDBPyConnection &connection);

//! A producer-owned native session. The Python owner keeps the export alive;
//! only the producer closes or destroys the underlying connection.
class SharedConnection {
public:
	static std::shared_ptr<SharedConnection> Import(const nb::dict &exported);
	~SharedConnection();

	Connection &GetConnection() const;
	DuckDB &GetDatabase() const;
	bool IsClosed() const;
	void Check() const;
	void Begin();
	void End();
	void Close();
	void Track(const std::shared_ptr<DuckDBPyConnection> &connection);
	nb::dict Export() const;
	std::shared_ptr<DuckDBPyConnection> OriginalPythonConnection() const;

private:
	SharedConnection(const duckdb_shared_connection &api, nb::object owner);
	static const char *BeforeClose(void *consumer);
	void CheckThread() const;

	duckdb_shared_connection api;
	const duckdb_shared_connection *descriptor;
	PyObjectHolder owner;
	std::thread::id thread;
	std::weak_ptr<SharedConnection> self;
	std::vector<std::weak_ptr<DuckDBPyConnection>> connections;
	ClientContext *context = nullptr;
	uint64_t subscription = 0;
	bool subscribed = false;
	bool closed = false;
	std::string close_error;
};

} // namespace duckdb
