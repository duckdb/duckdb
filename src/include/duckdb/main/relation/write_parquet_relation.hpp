//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/write_parquet_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class WriteParquetRelation : public Relation {
public:
	WriteParquetRelation(shared_ptr<Relation> child, string parquet_file,
	                     case_insensitive_map_t<vector<Value>> options);

	shared_ptr<Relation> child;
	string parquet_file;
	vector<ColumnDefinition> columns;
	case_insensitive_map_t<vector<Value>> options;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace duckdb
