#include "duckdb_python/shared_connection.hpp"

#include "duckdb_python/engine/result.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyutil.hpp"

#include <algorithm>
#include <charconv>

namespace duckdb {

SharedConnection::SharedConnection(const duckdb_shared_connection &api_p, nb::object owner_p)
    : api(api_p), owner(std::move(owner_p)), thread(std::this_thread::get_id()) {
}

std::shared_ptr<SharedConnection> SharedConnection::Import(const nb::dict &exported) {
	if (!exported.contains("address") || !exported.contains("owner") || exported["owner"].is_none()) {
		throw InvalidInputException("A shared connection export requires address and owner");
	}
	std::string address;
	if (!nb::try_cast(exported["address"], address)) {
		throw InvalidInputException("The shared connection address must be a decimal string");
	}
	uintptr_t pointer = 0;
	auto parsed = std::from_chars(address.data(), address.data() + address.size(), pointer);
	if (parsed.ec != std::errc() || parsed.ptr != address.data() + address.size() || pointer == 0) {
		throw InvalidInputException("Invalid shared connection address");
	}
	// Like Arrow's pointer import, the address is trusted in-process input. Its
	// owner must preserve the descriptor; arbitrary addresses cannot be validated.
	auto &api = *reinterpret_cast<const duckdb_shared_connection *>(pointer);
	if (api.version != DUCKDB_SHARED_CONNECTION_VERSION || api.struct_size < sizeof(duckdb_shared_connection)) {
		throw InvalidInputException("Unsupported shared connection protocol");
	}
	if (!api.abi_id || !*api.abi_id || SharedConnectionABI().empty() || SharedConnectionABI() != api.abi_id) {
		throw InvalidInputException("Incompatible shared connection native ABI");
	}
	if (!api.get_connection || !api.get_database || !api.is_valid || !api.close || !api.begin_operation ||
	    !api.end_operation || !api.subscribe_close || !api.unsubscribe_close) {
		throw InvalidInputException("Incomplete shared connection export");
	}
	auto shared =
	    std::shared_ptr<SharedConnection>(new SharedConnection(api, nb::borrow<nb::object>(exported["owner"])));
	shared->self = shared;
	shared->Check();
	shared->context = shared->GetConnection().context.get();
	if (auto error = api.subscribe_close(api.context, BeforeClose, &shared->self, &shared->subscription)) {
		throw ConnectionException(error);
	}
	shared->subscribed = true;
	return shared;
}

SharedConnection::~SharedConnection() {
	if (subscribed) {
		PyUtil::GilReleaseIfHeld release;
		api.unsubscribe_close(api.context, subscription);
	}
}

void SharedConnection::CheckThread() const {
	if (std::this_thread::get_id() != thread) {
		throw ConnectionException("Shared connection must be used on its importing thread");
	}
}

bool SharedConnection::IsClosed() const {
	CheckThread();
	return closed || !api.is_valid(api.context);
}

void SharedConnection::Check() const {
	if (IsClosed()) {
		throw ConnectionException("Shared connection is closed");
	}
}

Connection &SharedConnection::GetConnection() const {
	Check();
	auto connection = static_cast<Connection *>(api.get_connection(api.context));
	if (!connection) {
		throw ConnectionException("Shared connection is closed");
	}
	return *connection;
}

DuckDB &SharedConnection::GetDatabase() const {
	Check();
	auto database = static_cast<DuckDB *>(api.get_database(api.context));
	if (!database) {
		throw ConnectionException("Shared connection database is closed");
	}
	return *database;
}

void SharedConnection::Begin() {
	Check();
	if (auto error = api.begin_operation(api.context)) {
		throw ConnectionException(error);
	}
}

void SharedConnection::End() {
	api.end_operation(api.context);
}

void SharedConnection::Close() {
	CheckThread();
	if (!IsClosed()) {
		if (auto error = api.close(api.context)) {
			throw ConnectionException(error);
		}
	}
}

void SharedConnection::Track(const std::shared_ptr<DuckDBPyConnection> &connection) {
	connections.erase(
	    std::remove_if(connections.begin(), connections.end(), [](auto &entry) { return entry.expired(); }),
	    connections.end());
	connections.emplace_back(connection);
}

const char *SharedConnection::BeforeClose(void *consumer) {
	auto retained = static_cast<std::weak_ptr<SharedConnection> *>(consumer)->lock();
	if (!retained) {
		return nullptr;
	}
	auto &shared = *retained;
	try {
		shared.CheckThread();
		shared.closed = true;
		// The producer notifies outside engine locks. End every Python result on
		// this session before it destroys the Connection, releasing its transaction.
		PyUtil::GilReleaseIfHeld release;
		for (auto &result : engine::Result::Live()) {
			shared_ptr<ClientContext> context;
			try {
				context = result->TryGetContext();
			} catch (ConnectionException &) {
				continue;
			}
			if (context.get() == shared.context || result->Sharing().get() == &shared) {
				result->CloseWithError(std::make_exception_ptr(ConnectionException("Shared connection is closed")));
			}
		}
		{
			nb::gil_scoped_acquire gil;
			// Native cursors own separate connections. Close them too, so their
			// transactions cannot outlive the producer through a borrowed database.
			for (auto &weak : shared.connections) {
				if (auto connection = weak.lock()) {
					connection->Close();
				}
			}
		}
		return nullptr;
	} catch (std::exception &error) {
		shared.closed = false;
		shared.close_error = error.what();
		return shared.close_error.c_str();
	} catch (...) {
		shared.closed = false;
		shared.close_error = "Failed to release shared connection results";
		return shared.close_error.c_str();
	}
}

} // namespace duckdb
