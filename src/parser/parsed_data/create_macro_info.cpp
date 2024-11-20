#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

CreateMacroInfo::CreateMacroInfo(CatalogType type) : CreateFunctionInfo(type, INVALID_SCHEMA) {
}

CreateMacroInfo::CreateMacroInfo(CatalogType type, unique_ptr<MacroFunction> function,
                                 vector<unique_ptr<MacroFunction>> extra_functions)
    : CreateFunctionInfo(type, INVALID_SCHEMA) {
	macros.push_back(std::move(function));
	for (auto &macro : extra_functions) {
		macros.push_back(std::move(macro));
	}
}

string CreateMacroInfo::ToString() const {
	string result;
	for (auto &function : macros) {
		if (!result.empty()) {
			result += ", ";
		}
		result += function->ToSQL();
	}
	// prefix with CREATE MACRO
	string prefix = "CREATE MACRO ";
	if (!catalog.empty()) {
		prefix += KeywordHelper::WriteOptionallyQuoted(catalog);
		prefix += ".";
	}
	if (!schema.empty()) {
		prefix += KeywordHelper::WriteOptionallyQuoted(schema);
		prefix += ".";
	}
	prefix += KeywordHelper::WriteOptionallyQuoted(name);
	result = prefix + " " + result + ";";
	return result;
}

unique_ptr<CreateInfo> CreateMacroInfo::Copy() const {
	auto result = make_uniq<CreateMacroInfo>(type);
	for (auto &macro : macros) {
		result->macros.push_back(macro->Copy());
	}
	result->name = name;
	CopyFunctionProperties(*result);
	return std::move(result);
}

vector<unique_ptr<MacroFunction>> CreateMacroInfo::GetAllButFirstFunction() const {
	vector<unique_ptr<MacroFunction>> result;
	for (idx_t i = 1; i < macros.size(); i++) {
		result.push_back(macros[i]->Copy());
	}
	return result;
}

} // namespace duckdb
