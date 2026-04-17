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
enum class AlterScalarFunctionType : uint8_t {
	INVALID = 0,
	ADD_FUNCTION_OVERLOADS = 1,
	RENAME_SCALAR_FUNCTION = 2
};

struct AlterScalarFunctionInfo : public AlterInfo {
	AlterScalarFunctionInfo(AlterScalarFunctionType type, AlterEntryData data);
	~AlterScalarFunctionInfo() override;

	AlterScalarFunctionType alter_scalar_function_type;

public:
	CatalogType GetCatalogType() const override;
};

//===--------------------------------------------------------------------===//
// RenameScalarFunctionInfo
//===--------------------------------------------------------------------===//
// Used for ALTER FUNCTION ... RENAME TO ... Note: in SereneDB user-defined
// functions may be stored as either scalar or table macros; the binder/catalog
// skip the usual entry-type lookup for this info so the schema handler can
// resolve either kind by name.
struct RenameScalarFunctionInfo : public AlterScalarFunctionInfo {
	RenameScalarFunctionInfo(AlterEntryData data, string new_name);
	~RenameScalarFunctionInfo() override;

	string new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

private:
	RenameScalarFunctionInfo();
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
