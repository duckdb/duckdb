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
	explicit CSVRejectsTable(string name_p) : name(std::move(name_p)), count(0) {
	}
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
