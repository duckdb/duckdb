#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#endif

namespace duckdb {

struct ReadCSVData;

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
