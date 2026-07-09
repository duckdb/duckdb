//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/feature_at_version_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {

//! "FEATURE <name> AT VERSION <n>", usable both as a table reference (inside FROM) and — wrapped in
//! "SELECT * FROM ..." — as a standalone statement. Resolved at bind time to the denormalized store filtered
//! to the requested version; a dedicated type (rather than a table function call) so the resolution can only
//! be reached through this syntax, not by a user-typed function call of the same name.
class FeatureAtVersionRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::FEATURE_AT_VERSION;

public:
	FeatureAtVersionRef() : TableRef(TableReferenceType::FEATURE_AT_VERSION) {
	}

	//! The feature name.
	string feature_name;
	//! The requested retained version.
	int64_t version = 0;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
