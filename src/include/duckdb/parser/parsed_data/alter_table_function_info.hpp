//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Alter Table Function
//===--------------------------------------------------------------------===//
enum class AlterTableFunctionType : uint8_t { INVALID = 0, ADD_FUNCTION_OVERLOADS = 1 };

struct AlterTableFunctionInfo : public AlterInfo {
	AlterTableFunctionInfo(AlterTableFunctionType type, AlterEntryData data);
	~AlterTableFunctionInfo() override;

	AlterTableFunctionType alter_table_function_type;

public:
	CatalogType GetCatalogType() const override;
};

//===--------------------------------------------------------------------===//
// AddTableFunctionOverloadInfo
//===--------------------------------------------------------------------===//
struct AddTableFunctionOverloadInfo : public AlterTableFunctionInfo {
	AddTableFunctionOverloadInfo(AlterEntryData data, TableFunctionSet new_overloads);
	~AddTableFunctionOverloadInfo() override;

	TableFunctionSet new_overloads;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
