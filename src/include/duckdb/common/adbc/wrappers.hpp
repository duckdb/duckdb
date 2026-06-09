//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/wrappers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
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

	//! Register a stream wrapper so it can be materialized if another query runs on this connection.
	void RegisterStream(duckdb_adbc::DuckDBAdbcStreamWrapper *stream);
	//! Unregister a stream wrapper.
	void UnregisterStream(duckdb_adbc::DuckDBAdbcStreamWrapper *stream);
	//! Materialize all active streams, fetching remaining data into memory.
	void MaterializeStreams();
	//! Detach all streams from this connection and clear the list (called on connection release).
	void DetachAndClearStreams();

private:
	mutex stream_mutex;
	vector<duckdb_adbc::DuckDBAdbcStreamWrapper *> active_streams;
};
} // namespace duckdb
