//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {
struct CreateScalarFunctionInfo;

//===--------------------------------------------------------------------===//
// Alter Scalar Function
//===--------------------------------------------------------------------===//
enum class AlterScalarFunctionType : uint8_t { INVALID = 0, ADD_FUNCTION_OVERLOADS = 1 };

struct AlterScalarFunctionInfo : public AlterInfo {
	AlterScalarFunctionInfo(AlterScalarFunctionType type, AlterEntryData data);
	~AlterScalarFunctionInfo() override;

	AlterScalarFunctionType alter_scalar_function_type;

public:
	CatalogType GetCatalogType() const override;
};

//===--------------------------------------------------------------------===//
// AddScalarFunctionOverloadInfo
//===--------------------------------------------------------------------===//
struct AddScalarFunctionOverloadInfo : public AlterScalarFunctionInfo {
	AddScalarFunctionOverloadInfo(AlterEntryData data, unique_ptr<CreateScalarFunctionInfo> new_overloads);
	~AddScalarFunctionOverloadInfo() override;

	unique_ptr<CreateScalarFunctionInfo> new_overloads;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
