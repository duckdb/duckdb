//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/explain_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

//! Represents the plan output of an EXPLAIN query.
class ExplainRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::EXPLAIN;

public:
	ExplainRef();

	unique_ptr<QueryNode> query;
	string format = "default";

public:
	string ToString() const override;
	bool Equals(const TableRef &other) const override;
	unique_ptr<TableRef> Copy() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
