//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class BufferedFileWriter;
class ClientContext;
class CatalogSearchPath;
class FileOpener;
class QueryProfiler;
class QueryProfilerHistory;
class PreparedStatementData;
class SchemaCatalogEntry;
struct RandomEngine;

struct ClientData {
	ClientData(ClientContext &context);
	~ClientData();

	//! Query profiler
	shared_ptr<QueryProfiler> profiler;
	//! QueryProfiler History
	unique_ptr<QueryProfilerHistory> query_profiler_history;

	//! The set of temporary objects that belong to this client
	unique_ptr<SchemaCatalogEntry> temporary_objects;
	//! The set of bound prepared statements that belong to this client
	unordered_map<string, shared_ptr<PreparedStatementData>> prepared_statements;

	//! The writer used to log queries (if logging is enabled)
	unique_ptr<BufferedFileWriter> log_query_writer;
	//! The random generator used by random(). Its seed value can be set by setseed().
	unique_ptr<RandomEngine> random_engine;

	//! The catalog search path
	const unique_ptr<CatalogSearchPath> catalog_search_path;

	//! The file opener of the client context
	unique_ptr<FileOpener> file_opener;

	//! The file search path
	string file_search_path;

public:
	DUCKDB_API static ClientData &Get(ClientContext &context);
};

} // namespace duckdb
