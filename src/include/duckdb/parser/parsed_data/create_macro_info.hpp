//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_macro_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/macro_function.hpp"

namespace duckdb {

struct CreateMacroInfo : public CreateFunctionInfo {
	CreateMacroInfo();
	CreateMacroInfo(CatalogType type);

	unique_ptr<MacroFunction> function;

public:
	unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API static unique_ptr<CreateMacroInfo> Deserialize(Deserializer &deserializer);

protected:
	void SerializeInternal(Serializer &) const override;
};

} // namespace duckdb
