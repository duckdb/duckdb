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
	explicit CreateIndexTypeInfo(string index_type_name_p, string schema = DEFAULT_SCHEMA);

	string index_type_name;

	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
