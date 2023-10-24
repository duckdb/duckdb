#pragma once

#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct ReadCSVData;
class TableCatalogEntry;
class ClientContext;

class CSVRejectsTable : public ObjectCacheEntry {
public:
	CSVRejectsTable(string name) : name(name), count(0) {
	}
	~CSVRejectsTable() override = default;
	mutex write_lock;
	string name;
	idx_t count;

	static shared_ptr<CSVRejectsTable> GetOrCreate(ClientContext &context, const string &name);

	void InitializeTable(ClientContext &context, const ReadCSVData &options);
	TableCatalogEntry &GetTable(ClientContext &context);

public:
	static string ObjectType() {
		return "csv_rejects_table_cache";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};

} // namespace duckdb
