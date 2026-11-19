#include "duckdb.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

// An extension that sets a default schema it never creates and then declares a language: the language registers in
// the main schema as for any other extension

static void MissingSchemaLangDirect(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("missinglang cannot be called directly");
}

static void MissingSchemaLangCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement("'missinglang: " + input.source + "'");
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(language_missing_schema, loader) {
	loader.UseDefaultSchema("never_created_schema");
	ScalarFunction function("missinglang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        MissingSchemaLangDirect, nullptr, nullptr, nullptr, LogicalType::ANY);
	function.SetCallReplaceCallback(MissingSchemaLangCallReplace);
	ScalarFunctionSet language("missinglang");
	language.AddFunction(std::move(function));
	language.DeclareLanguage();
	loader.RegisterFunction(std::move(language));
}
}
