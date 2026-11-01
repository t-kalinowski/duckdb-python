/* Shared native DuckDB connection export, protocol version 1.
 *
 * The producer owns all native objects. The consumer must keep the export's
 * language-level owner alive until all borrowed objects have been released.
 * Import is only supported when both bindings report the same native ABI.
 * All callbacks use the producer's library and must not throw across this ABI.
 * An error is a producer-owned UTF-8 string, valid until its next callback.
 */
#ifndef DUCKDB_SHARED_CONNECTION_H
#define DUCKDB_SHARED_CONNECTION_H

#include <stdint.h>

#define DUCKDB_SHARED_CONNECTION_VERSION 1

typedef const char *(*duckdb_shared_connection_close_callback)(void *consumer);

typedef struct duckdb_shared_connection {
	uint64_t version;
	uint64_t struct_size;
	const char *abi_id;
	void *context;
	void *(*get_connection)(void *context);
	void *(*get_database)(void *context);
	int (*is_valid)(void *context);
	const char *(*close)(void *context);
	/* A scope runs on the producer's owning thread. It refuses reentrancy and
	 * supplies the producer's replacement-scan context and object retention. */
	const char *(*begin_operation)(void *context);
	void (*end_operation)(void *context);
	/* Notify before destroying the connection, outside engine locks. A refusal
	 * leaves the connection open. Unsubscribe may run on a finalizer thread. */
	const char *(*subscribe_close)(void *context, duckdb_shared_connection_close_callback callback, void *consumer,
	                               uint64_t *subscription);
	void (*unsubscribe_close)(void *context, uint64_t subscription);
} duckdb_shared_connection;

#ifdef __cplusplus
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/tableref.hpp"
#include <string>

#define DUCKDB_SHARED_STRINGIFY_IMPL(x) #x
#define DUCKDB_SHARED_STRINGIFY(x)      DUCKDB_SHARED_STRINGIFY_IMPL(x)

namespace duckdb {
// A consuming binding can install this state to capture replacement scans
// while binding a lazy query and recall them when that query executes. Producers
// participate without knowing the consumer's representation of a relation.
class SharedConnectionScanState : public ClientContextState {
public:
	static const char *Key() {
		return "duckdb_shared_connection_scans_v1";
	}
	virtual unique_ptr<TableRef> RecallResolved(const string &name) = 0;
	virtual void RecordResolved(const string &name, TableRef &table) = 0;
	virtual bool PinnedExclusively() = 0;
};

static inline std::string SharedConnectionABI() {
	std::string id = std::string(DuckDB::SourceID()) + "/" + DuckDB::Platform();
#if defined(__clang__)
	id += "/clang-" DUCKDB_SHARED_STRINGIFY(__clang_major__) "." DUCKDB_SHARED_STRINGIFY(__clang_minor__);
#elif defined(__GNUC__)
	id += "/gcc-" DUCKDB_SHARED_STRINGIFY(__GNUC__) "." DUCKDB_SHARED_STRINGIFY(__GNUC_MINOR__);
#elif defined(_MSC_VER)
	id += "/msvc-" DUCKDB_SHARED_STRINGIFY(_MSC_VER);
#else
	return std::string();
#endif
#if defined(_LIBCPP_VERSION)
	id += "/libc++-" DUCKDB_SHARED_STRINGIFY(_LIBCPP_VERSION) "-" DUCKDB_SHARED_STRINGIFY(_LIBCPP_ABI_VERSION);
#elif defined(__GLIBCXX__)
	id += "/libstdc++-" DUCKDB_SHARED_STRINGIFY(__GLIBCXX__) "-" DUCKDB_SHARED_STRINGIFY(_GLIBCXX_USE_CXX11_ABI);
#elif defined(_MSVC_STL_VERSION)
	id += "/msvc-stl-" DUCKDB_SHARED_STRINGIFY(_MSVC_STL_VERSION);
#else
	return std::string();
#endif
#if defined(_GLIBCXX_DEBUG)
	id += "/glibcxx-debug";
#endif
#if defined(_DLL)
	id += "/dynamic-crt";
#elif defined(_MSC_VER)
	id += "/static-crt";
#endif
#if defined(_DEBUG)
	id += "/debug-crt";
#endif
#if defined(_ITERATOR_DEBUG_LEVEL)
	id += "/iter-" DUCKDB_SHARED_STRINGIFY(_ITERATOR_DEBUG_LEVEL);
#endif
#if defined(DUCKDB_DEBUG_MODE)
	id += "/duckdb-debug";
#endif
	id += "/" + std::to_string(sizeof(void *)) + "/" + std::to_string(sizeof(Connection)) + "/" +
	      std::to_string(alignof(Connection)) + "/" + std::to_string(sizeof(ClientContext)) + "/" +
	      std::to_string(alignof(ClientContext));
	return id;
}
} // namespace duckdb
#endif
#endif
