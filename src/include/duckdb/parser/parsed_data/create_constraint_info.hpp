//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_constraint_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CreateConstraintInfo : public CreateInfo {
	CreateConstraintInfo() : CreateInfo(CatalogType::CONSTRAINT_ENTRY) {
	}

	//! Name of the Constraint
	string constraint_name;
	//! The table to create the index on
	unique_ptr<BaseTableRef> table;

protected:
	void SerializeInternal(Serializer &serializer) const override;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;

	static unique_ptr<CreateConstraintInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
