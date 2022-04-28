//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/compression_type.hpp"

namespace duckdb {

//! A generated column of a table.
class GeneratedColumnDefinition {
public:
	DUCKDB_API GeneratedColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> expression);

	//! The name of the entry
	string name;
	//! The index of the generated column in the table
	idx_t oid;
	//! The value type of the generated column
	LogicalType type;
	//! The expression run when this generated column is used
	unique_ptr<ParsedExpression> expression;
	//! Compression Type used for this generated column
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO;

public:
	DUCKDB_API GeneratedColumnDefinition Copy() const;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static GeneratedColumnDefinition Deserialize(Deserializer &source);
};

} // namespace duckdb
