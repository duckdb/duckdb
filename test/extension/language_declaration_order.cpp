#include "duckdb.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

// An extension with a dedicated schema that registers an overload of a language before declaring it: the
// declaration is rejected, since member calls would never select the earlier overload

static void LateLangDirect(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("latelang cannot be called directly");
}

static void LateLangCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement("'latelang'");
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(language_declaration_order, loader) {
	loader.UseDedicatedSchemaForExtension("order_schema");
	ScalarFunction early("latelang", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                     LogicalType::VARCHAR, LateLangDirect);
	early.SetCallReplaceCallback(LateLangCallReplace);
	loader.RegisterFunction(std::move(early));

	ScalarFunction declared("latelang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        LateLangDirect, nullptr, nullptr, nullptr, LogicalType::ANY);
	declared.SetCallReplaceCallback(LateLangCallReplace);
	ScalarFunctionSet language("latelang");
	language.AddFunction(std::move(declared));
	language.DeclareLanguage();
	loader.RegisterFunction(std::move(language));
}
}
