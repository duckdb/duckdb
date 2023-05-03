//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/write_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class WriteCSVRelation : public Relation {
public:
	WriteCSVRelation(shared_ptr<Relation> child, string csv_file, case_insensitive_map_t<vector<Value>> options);

	shared_ptr<Relation> child;
	string csv_file;
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
