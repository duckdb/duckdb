//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/serve_feature_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/feature_serve.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

//! "SERVE FEATURE(S) <name>[, ...] FOR <spine> [ENTITY ...] [ASOF <col>]", usable both as a table reference
//! (inside FROM, so WHERE / GROUP BY / JOIN compose around it) and — wrapped in "SELECT * FROM ..." — as a
//! standalone statement. Resolved at bind time to a subquery that joins the spine against each feature's
//! denormalized store; a dedicated type (rather than a table function call) so the resolution can only be
//! reached through this syntax, not by a user-typed function call of the same name.
class ServeFeatureRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SERVE_FEATURE;

public:
	ServeFeatureRef() : TableRef(TableReferenceType::SERVE_FEATURE) {
	}

	//! The features to serve, in output order.
	vector<ServeFeatureRequest> features;
	//! The spine table name.
	string spine_table;
	//! Optional global ENTITY override (spine column name) applied to every feature.
	string spine_entity_override;
	//! Optional ASOF column on the spine driving time-travel; empty means serve the latest version.
	string spine_asof_column;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
