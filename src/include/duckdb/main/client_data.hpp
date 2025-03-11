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
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine_cache.hpp"

namespace duckdb {
class AttachedDatabase;
class BufferedFileWriter;
class ClientContext;
class CatalogSearchPath;
class FileOpener;
class FileSystem;
class HTTPState;
class QueryProfiler;
class PreparedStatementData;
class SchemaCatalogEntry;
class HTTPLogger;
class RandomEngine;

struct ClientData {
	explicit ClientData(ClientContext &context);
	~ClientData();

	//! Query profiler
	shared_ptr<QueryProfiler> profiler;

	//! HTTP logger
	shared_ptr<HTTPLogger> http_logger;

	//! The set of temporary objects that belong to this client
	shared_ptr<AttachedDatabase> temporary_objects;
	//! The set of bound prepared statements that belong to this client
	case_insensitive_map_t<shared_ptr<PreparedStatementData>> prepared_statements;

	//! The writer used to log queries (if logging is enabled) TODO unify with new logger infra
	unique_ptr<BufferedFileWriter> log_query_writer;

	//! The random generator used by random(). Its seed value can be set by setseed().
	unique_ptr<RandomEngine> random_engine;

	//! The catalog search path
	unique_ptr<CatalogSearchPath> catalog_search_path;

	//! The file opener of the client context
	unique_ptr<FileOpener> file_opener;

	//! The clients' file system wrapper
	unique_ptr<FileSystem> client_file_system;

	//! The file search path
	string file_search_path;

	//! The Max Line Length Size of Last Query Executed on a CSV File. (Only used for testing)
	//! FIXME: this should not be done like this
	bool debug_set_max_line_length = false;
	idx_t debug_max_line_length = 0;

public:
	DUCKDB_API static ClientData &Get(ClientContext &context);
	DUCKDB_API static const ClientData &Get(const ClientContext &context);
};

} // namespace duckdb
