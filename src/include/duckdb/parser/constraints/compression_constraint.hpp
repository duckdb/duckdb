//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/compression_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/enums/compression_type.hpp"

namespace duckdb {

class CompressionConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::COMPRESSION;

public:
	DUCKDB_API CompressionConstraint(string column_name, CompressionType compression_type);
	DUCKDB_API ~CompressionConstraint() override;

	//! Column name this constraint pertains to
	string column_name;
	//! The compression type
	CompressionType compression_type;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
