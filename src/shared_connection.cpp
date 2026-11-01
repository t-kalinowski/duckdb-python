#include "duckdb_python/shared_connection.hpp"

#include "duckdb_python/engine/result.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyutil.hpp"

#include <algorithm>
#include <charconv>
#include <map>
#include <mutex>

namespace duckdb {

class PythonConnectionExport {
public:
	explicit PythonConnectionExport(std::shared_ptr<DuckDBPyConnection> connection_p)
	    : connection(std::move(connection_p)), database(connection->con.ShareDatabase()),
	      native(&connection->con.GetConnection()), thread(std::this_thread::get_id()), abi(SharedConnectionABI()) {
		api = {DUCKDB_SHARED_CONNECTION_VERSION,
		       sizeof(duckdb_shared_connection),
		       abi.c_str(),
		       this,
		       GetConnection,
		       GetDatabase,
		       IsValid,
		       Close,
		       Begin,
		       End,
		       Subscribe,
		       Unsubscribe};
	}

	static std::shared_ptr<DuckDBPyConnection> Original(const duckdb_shared_connection &api) {
		return api.get_connection == GetConnection ? Get(api.context).connection : nullptr;
	}

	nb::dict Export(const std::shared_ptr<PythonConnectionExport> &self) {
		Check();
		nb::dict result;
		result["address"] = std::to_string(reinterpret_cast<uintptr_t>(&api));
		result["owner"] = nb::capsule(new std::shared_ptr<PythonConnectionExport>(self), [](void *pointer) noexcept {
			delete static_cast<std::shared_ptr<PythonConnectionExport> *>(pointer);
		});
		return result;
	}

	void NotifyClose() {
		CheckThread();
		if (closed) {
			return;
		}
		PyUtil::GilReleaseIfHeld release;
		// The native cursor may be closing because its own producer already
		// invalidated its borrowed database. No producer scope is needed to close.
		engine::ConnectionClaim claim(*native->context);
		std::lock_guard<std::recursive_mutex> guard(subscribers_lock);
		closing = true;
		try {
			for (auto &result : engine::Result::Live()) {
				shared_ptr<ClientContext> context;
				try {
					context = result->TryGetContext();
				} catch (ConnectionException &) {
					continue;
				}
				if (context.get() == native->context.get()) {
					result->CloseWithError(std::make_exception_ptr(ConnectionException("Shared connection is closed")));
				}
			}
			vector<uint64_t> ids;
			for (auto &entry : subscribers) {
				ids.push_back(entry.first);
			}
			for (auto id : ids) {
				auto found = subscribers.find(id);
				if (found != subscribers.end()) {
					auto subscriber = found->second;
					if (auto failure = subscriber.callback(subscriber.consumer)) {
						throw ConnectionException(failure);
					}
				}
			}
			// Foreign results may retain the ClientContext after Connection destruction.
			// End its transaction now so those retained views cannot keep write locks.
			if (!native->IsAutoCommit()) {
				native->Rollback();
			}
			// Consumers keep their own database facade if they need it after close.
			// Keeping ours could retain a catalog UDF that captures this export.
			database.reset();
			closed = true;
			closing = false;
		} catch (...) {
			closing = false;
			throw;
		}
	}

private:
	struct Subscriber {
		duckdb_shared_connection_close_callback callback;
		void *consumer;
	};

	static PythonConnectionExport &Get(void *context) {
		return *static_cast<PythonConnectionExport *>(context);
	}

	void CheckThread() const {
		if (std::this_thread::get_id() != thread) {
			throw ConnectionException("Shared connection must be used on its exporting thread");
		}
	}

	void Check() const {
		CheckThread();
		if (closed || closing || connection->con.ConnectionIsClosed()) {
			throw ConnectionException("Shared connection is closed");
		}
	}

	template <class FUNCTION>
	static const char *Try(void *context, FUNCTION function) {
		auto &self = Get(context);
		try {
			self.Check();
			function(self);
			return nullptr;
		} catch (std::exception &error) {
			self.error = error.what();
		} catch (...) {
			self.error = "Shared Python connection operation failed";
		}
		return self.error.c_str();
	}

	static int IsValid(void *context) {
		try {
			Get(context).Check();
			return 1;
		} catch (...) {
			return 0;
		}
	}

	static void *GetConnection(void *context) {
		return IsValid(context) ? Get(context).native : nullptr;
	}

	static void *GetDatabase(void *context) {
		return IsValid(context) ? Get(context).database.get() : nullptr;
	}

	static const char *Close(void *context) {
		return Try(context, [](PythonConnectionExport &self) {
			nb::gil_scoped_acquire gil;
			self.connection->Close();
		});
	}

	static const char *Begin(void *context) {
		return Try(context, [](PythonConnectionExport &self) {
			nb::gil_scoped_acquire gil;
			DuckDBPyConnection::ConnectionLockGuard guard(*self.connection);
			if (self.operation) {
				throw ConnectionException("Shared Python connection is already executing an operation");
			}
			if (self.connection->con.HasResult()) {
				self.connection->con.GetResult().Supersede();
				self.connection->con.SetResult(nullptr);
			}
			self.Check();
			self.operation = make_uniq<engine::ConnectionClaim>(*self.native->context, self.connection->con.Sharing());
		});
	}

	static void End(void *context) {
		Get(context).operation.reset();
	}

	static const char *Subscribe(void *context, duckdb_shared_connection_close_callback callback, void *consumer,
	                             uint64_t *subscription) {
		return Try(context, [&](PythonConnectionExport &self) {
			// Import admission must not run or discard an existing result.
			self.connection->con.ThrowIfBusy();
			PyUtil::GilReleaseIfHeld release;
			std::lock_guard<std::recursive_mutex> guard(self.subscribers_lock);
			*subscription = ++self.next_subscription;
			self.subscribers.emplace(*subscription, Subscriber {callback, consumer});
		});
	}

	static void Unsubscribe(void *context, uint64_t subscription) {
		auto &self = Get(context);
		PyUtil::GilReleaseIfHeld release;
		std::lock_guard<std::recursive_mutex> guard(self.subscribers_lock);
		self.subscribers.erase(subscription);
	}

	std::shared_ptr<DuckDBPyConnection> connection;
	shared_ptr<DuckDB> database;
	Connection *native;
	std::thread::id thread;
	string abi;
	duckdb_shared_connection api;
	string error;
	bool closing = false;
	bool closed = false;
	unique_ptr<engine::ConnectionClaim> operation;
	std::recursive_mutex subscribers_lock;
	std::map<uint64_t, Subscriber> subscribers;
	uint64_t next_subscription = 0;
};

nb::dict ExportSharedConnection(const std::shared_ptr<DuckDBPyConnection> &connection) {
	DuckDBPyConnection::ConnectionLockGuard guard(*connection);
	connection->con.GetState().ConfineToCurrentThread();
	if (connection->con.IsBorrowed()) {
		return connection->con.Sharing()->Export();
	}
	auto shared = connection->shared_export.lock();
	if (!shared) {
		shared = std::make_shared<PythonConnectionExport>(connection);
		connection->shared_export = shared;
	}
	return shared->Export(shared);
}

void CloseSharedConnectionExports(DuckDBPyConnection &connection) {
	if (auto shared = connection.shared_export.lock()) {
		shared->NotifyClose();
	}
}

SharedConnection::SharedConnection(const duckdb_shared_connection &api_p, nb::object owner_p)
    : api(api_p), descriptor(&api_p), owner(std::move(owner_p)), thread(std::this_thread::get_id()) {
}

nb::dict SharedConnection::Export() const {
	Check();
	nb::dict result;
	result["address"] = std::to_string(reinterpret_cast<uintptr_t>(descriptor));
	result["owner"] = owner.obj;
	return result;
}

std::shared_ptr<DuckDBPyConnection> SharedConnection::OriginalPythonConnection() const {
	Check();
	return PythonConnectionExport::Original(api);
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
