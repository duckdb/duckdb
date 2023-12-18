//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct CreateIndexTypeInfo : public CreateInfo {
	explicit CreateIndexTypeInfo(string name_p, string schema = DEFAULT_SCHEMA);

	// The index type name
	string name;

public:
	unique_ptr<CreateInfo> Copy() const override;

public:
	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
