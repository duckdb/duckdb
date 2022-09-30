//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Alter Table
//===--------------------------------------------------------------------===//
enum class AlterFunctionType : uint8_t { INVALID = 0, ADD_FUNCTION_OVERLOADS = 1 };

struct AlterFunctionInfo : public AlterInfo {
	AlterFunctionInfo(AlterFunctionType type, string schema, string name, bool if_exists);
	virtual ~AlterFunctionInfo() override;

	AlterFunctionType alter_function_type;

public:
	CatalogType GetCatalogType() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader);
};

//===--------------------------------------------------------------------===//
// AddFunctionOverloadInfo
//===--------------------------------------------------------------------===//
struct AddFunctionOverloadInfo : public AlterFunctionInfo {
	AddFunctionOverloadInfo(string schema, string name, bool if_exists, ScalarFunctionSet new_overloads);
	~AddFunctionOverloadInfo() override;

	ScalarFunctionSet new_overloads;

public:
	unique_ptr<AlterInfo> Copy() const override;
};

} // namespace duckdb
