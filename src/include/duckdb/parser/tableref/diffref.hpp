//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/diffref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {

//! DIFF old, new [KEY (columns)] [CELLS] - a semantic diff of two relations (see bind_diffref.cpp)
class DiffRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::DIFF_REF;

public:
	DiffRef();

	//! The two relations being compared (a subquery or a base table each)
	unique_ptr<TableRef> old_side;
	unique_ptr<TableRef> new_side;
	//! The declared key columns - empty means the key is guessed
	vector<string> key;
	//! Whether to return the changed cells instead of the summary
	bool cells = false;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
