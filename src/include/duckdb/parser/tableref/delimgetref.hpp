//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/delimgetref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class DelimGetRef : public TableRef {

public:
	explicit DelimGetRef(const vector<LogicalType> &types_p) : TableRef(TableReferenceType::DELIM_GET), types(types_p) {
		for (idx_t i = 0; i < types.size(); i++) {
			string column_name = "__internal_delim_get_" + std::to_string(i);
			internal_aliases.emplace_back(column_name);
		}
	}

	vector<string> internal_aliases;
	vector<LogicalType> types;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
