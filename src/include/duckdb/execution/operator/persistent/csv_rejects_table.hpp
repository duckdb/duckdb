//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_rejects_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ReadCSVData;
class TableCatalogEntry;
class ClientContext;
class PhysicalOperator;

class CSVRejectsTable : public ObjectCacheEntry {
public:
	CSVRejectsTable(string rejects_scan, string rejects_error)
	    : count(0), scan_table(std::move(rejects_scan)), errors_table(std::move(rejects_error)) {
	}
	mutex write_lock;
	//! Guards the file indexes below - separate from the write lock, which is held while the tables are written
	mutex file_index_lock;
	idx_t file_index_query_id = DConstants::INVALID_INDEX;
	idx_t next_file_index = 0;
	reference_map_t<const PhysicalOperator, vector<idx_t>> scan_file_indexes;
	string name;
	idx_t count;
	string scan_table;
	string errors_table;

	static shared_ptr<CSVRejectsTable> GetOrCreate(ClientContext &context, const string &rejects_scan,
	                                               const string &rejects_error);

	void InitializeTable(ClientContext &context, const ReadCSVData &options);
	//! Allocate query-wide file indexes on demand, preserving file order within each scan
	idx_t GetFileIndex(idx_t query_id, optional_ptr<const PhysicalOperator> scan_op, idx_t file_index);
	TableCatalogEntry &GetErrorsTable(ClientContext &context);
	TableCatalogEntry &GetScansTable(ClientContext &context);

	static string ObjectType() {
		return "csv_rejects_table_cache";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Rejects table records the overall error counts, which is relatively small and should not be evicted.
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

private:
	//! Current File Index being used in the query
	//! Current Query ID being executed
};

} // namespace duckdb
