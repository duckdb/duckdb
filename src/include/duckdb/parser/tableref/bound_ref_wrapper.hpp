//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/bound_ref_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

//! Represents an already bound table ref - used during binding only
class BoundRefWrapper : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BOUND_TABLE_REF;

public:
	BoundRefWrapper(BoundStatement bound_ref_p, shared_ptr<Binder> binder_p);

	//! The bound reference object
	BoundStatement bound_ref;
	//! The binder that was used to bind this table ref
	shared_ptr<Binder> binder;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;
	unique_ptr<TableRef> Copy() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
