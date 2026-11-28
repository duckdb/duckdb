#include "duckdb.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

using namespace duckdb;

static void LoadableExtensionExplicitSchema(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello from the explicit_extension_schema schema!"), count_t(args.size()));
}

static void SchemaLangCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement(ConstantExpression::FromValue(Value("schemalang: " + input.source)));
}

static void SchemaLangCreateValidate(ScalarFunctionCreateValidateInput &input) {
	for (auto &member : input.members) {
		if (StringUtil::Contains(member.source, "invalid")) {
			throw BinderException("schemalang: rejected %s", input.function_name.Name());
		}
	}
}

static void SchemaLangIntegerCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement(ConstantExpression::FromValue(Value("schemalang integer: " + input.source)));
}

static void UnrelatedPlang(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("explicit_schema plang"), count_t(args.size()));
}

static void SchemaLangDirect(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("schemalang cannot be called directly");
}

static void LoadableExtensionExplicitSchemaFunInit(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    ScalarFunction("dedicated_schema_function", {}, LogicalType::VARCHAR, LoadableExtensionExplicitSchema));
	// a function language registered by an extension with a dedicated schema
	ScalarFunction schemalang("schemalang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          SchemaLangDirect, nullptr, nullptr, nullptr, LogicalType::ANY);
	schemalang.SetCallReplaceCallback(SchemaLangCallReplace);
	ScalarFunctionSet schemalang_set("schemalang");
	schemalang_set.AddFunction(std::move(schemalang));
	schemalang_set.DeclareLanguage();
	schemalang_set.SetCreateValidateCallback(SchemaLangCreateValidate);
	loader.RegisterFunction(std::move(schemalang_set));

	// an overload added without the declaration joins the language in the main schema instead of creating
	// explicit_schema.schemalang
	ScalarFunction schemalang_integer("schemalang", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                  LogicalType::VARCHAR, SchemaLangDirect);
	schemalang_integer.SetCallReplaceCallback(SchemaLangIntegerCallReplace);
	ScalarFunctionSet schemalang_integer_set("schemalang");
	schemalang_integer_set.AddFunction(std::move(schemalang_integer));
	loader.RegisterFunction(std::move(schemalang_integer_set));

	// a function that shares its name with another extension's language stays in this extension's schema
	loader.RegisterFunction(ScalarFunction("plang", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                       LogicalType::VARCHAR, UnrelatedPlang));

	// AddFunctionOverload carries the declaration and the validator of the set it adds
	ScalarFunction schemalang_late("schemalang_late", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               SchemaLangDirect, nullptr, nullptr, nullptr, LogicalType::ANY);
	schemalang_late.SetCallReplaceCallback(SchemaLangCallReplace);
	ScalarFunctionSet schemalang_late_set("schemalang_late");
	schemalang_late_set.AddFunction(std::move(schemalang_late));
	schemalang_late_set.DeclareLanguage();
	loader.RegisterFunction(std::move(schemalang_late_set));
	ScalarFunction schemalang_late_integer("schemalang_late",
	                                       vector<LogicalType> {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                       LogicalType::VARCHAR, SchemaLangDirect);
	schemalang_late_integer.SetCallReplaceCallback(SchemaLangIntegerCallReplace);
	ScalarFunctionSet schemalang_late_overloads("schemalang_late");
	schemalang_late_overloads.AddFunction(std::move(schemalang_late_integer));
	schemalang_late_overloads.DeclareLanguage();
	schemalang_late_overloads.SetCreateValidateCallback(SchemaLangCreateValidate);
	loader.AddFunctionOverload(std::move(schemalang_late_overloads));
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(explicit_extension_schema, loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance()).GetCallbackManager();
	// set the schema for the extension
	loader.UseDedicatedSchemaForExtension("explicit_schema");
	LoadableExtensionExplicitSchemaFunInit(loader);
}
}
