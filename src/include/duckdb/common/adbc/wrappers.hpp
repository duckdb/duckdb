//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/wrappers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb_adbc {
struct DuckDBAdbcStreamWrapper;
} // namespace duckdb_adbc

namespace duckdb {

struct DuckDBAdbcConnectionWrapper {
	duckdb_connection connection;
	unordered_map<string, string> options;
	//! A cancel and a max_execution_time timeout raise the same exception type, and errors_as_json rewrites the
	//! message that would tell them apart. Cleared when a statement starts executing
	atomic<bool> cancel_requested {false};

	//! Register a stream wrapper so it can be materialized if another query runs on this connection.
	void RegisterStream(duckdb_adbc::DuckDBAdbcStreamWrapper *stream);
	//! Unregister a stream wrapper.
	void UnregisterStream(duckdb_adbc::DuckDBAdbcStreamWrapper *stream);
	//! Materialize all active streams, fetching remaining data into memory.
	void MaterializeStreams();
	//! Ends every open stream on this connection, which then reports `reason` once its materialized arrays are read
	void CloseStreams(const char *reason);
	//! Detach all streams from this connection and clear the list (called on connection release).
	void DetachAndClearStreams();

private:
	mutex stream_mutex;
	vector<duckdb_adbc::DuckDBAdbcStreamWrapper *> active_streams;
};
} // namespace duckdb
