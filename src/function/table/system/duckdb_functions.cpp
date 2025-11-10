#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
constexpr const char *AggregateFunctionCatalogEntry::Name;

struct DuckDBFunctionsData : public GlobalTableFunctionState {
	DuckDBFunctionsData() : window_iterator(WindowExpression::WindowFunctions()), offset(0), offset_in_entry(0) {
	}

	vector<reference<CatalogEntry>> entries;
	const WindowFunctionDefinition *window_iterator;
	idx_t offset;
	idx_t offset_in_entry;
};

static unique_ptr<FunctionData> DuckDBFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("function_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("alias_of");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("function_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("return_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parameters");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("parameter_types");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("varargs");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("macro_definition");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("has_side_effects");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("function_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("examples");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("stability");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("categories");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	return nullptr;
}

static void ExtractFunctionsFromSchema(ClientContext &context, SchemaCatalogEntry &schema,
                                       DuckDBFunctionsData &result) {
	schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY,
	            [&](CatalogEntry &entry) { result.entries.push_back(entry); });
	schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY,
	            [&](CatalogEntry &entry) { result.entries.push_back(entry); });
	schema.Scan(context, CatalogType::PRAGMA_FUNCTION_ENTRY,
	            [&](CatalogEntry &entry) { result.entries.push_back(entry); });
}

unique_ptr<GlobalTableFunctionState> DuckDBFunctionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBFunctionsData>();

	// scan all the schemas for tables and collect them and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		ExtractFunctionsFromSchema(context, schema.get(), *result);
	};

	std::sort(result->entries.begin(), result->entries.end(),
	          [&](reference<CatalogEntry> a, reference<CatalogEntry> b) {
		          return (int32_t)a.get().type < (int32_t)b.get().type;
	          });
	return std::move(result);
}

Value FunctionStabilityToValue(FunctionStability stability) {
	switch (stability) {
	case FunctionStability::VOLATILE:
		return Value("VOLATILE");
	case FunctionStability::CONSISTENT:
		return Value("CONSISTENT");
	case FunctionStability::CONSISTENT_WITHIN_QUERY:
		return Value("CONSISTENT_WITHIN_QUERY");
	default:
		throw InternalException("Unsupported FunctionStability");
	}
}

struct ScalarFunctionExtractor {
	static idx_t FunctionCount(ScalarFunctionCatalogEntry &entry) {
		return entry.functions.Size();
	}

	static Value GetFunctionType() {
		return Value("scalar");
	}

	static Value GetReturnType(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value(entry.functions.GetFunctionByOffset(offset).GetReturnType().ToString());
	}

	static vector<Value> GetParameters(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions.GetFunctionByOffset(offset).arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		return results;
	}

	static Value GetParameterTypes(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);
		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back(fun.arguments[i].ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return fun.arguments;
	}

	static Value GetVarArgs(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return !fun.HasVarArgs() ? Value() : Value(fun.varargs.ToString());
	}

	static Value GetMacroDefinition(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value IsVolatile(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value::BOOLEAN(entry.functions.GetFunctionByOffset(offset).GetStability() ==
		                      FunctionStability::VOLATILE);
	}

	static Value ResultType(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return FunctionStabilityToValue(entry.functions.GetFunctionByOffset(offset).GetStability());
	}
};

namespace {

struct WindowFunctionCatalogEntry : CatalogEntry {
public:
	WindowFunctionCatalogEntry(const SchemaCatalogEntry &schema, const string &name, vector<LogicalType> arguments,
	                           LogicalType return_type)
	    : CatalogEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, name, 0), schema(schema), arguments(std::move(arguments)),
	      return_type(std::move(return_type)) {
		internal = true;
	}

public:
	const SchemaCatalogEntry &schema;
	vector<LogicalType> arguments;
	LogicalType return_type;
	vector<FunctionDescription> descriptions;
	string alias_of;
};

} // namespace

struct WindowFunctionExtractor {
	static idx_t FunctionCount(WindowFunctionCatalogEntry &entry) {
		return 1;
	}

	static Value GetFunctionType() {
		//! FIXME: should be 'window' but requires adapting generation scripts
		return Value("aggregate");
	}

	static Value GetReturnType(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return Value(entry.return_type.ToString());
	}

	static vector<Value> GetParameters(WindowFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		return results;
	}

	static Value GetParameterTypes(WindowFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.arguments.size(); i++) {
			results.emplace_back(entry.arguments[i].ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return entry.arguments;
	}

	static Value GetVarArgs(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetMacroDefinition(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value IsVolatile(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return Value::BOOLEAN(false);
	}

	static Value ResultType(WindowFunctionCatalogEntry &entry, idx_t offset) {
		return FunctionStabilityToValue(FunctionStability::CONSISTENT);
	}
};

struct AggregateFunctionExtractor {
	static idx_t FunctionCount(AggregateFunctionCatalogEntry &entry) {
		return entry.functions.Size();
	}

	static Value GetFunctionType() {
		return Value("aggregate");
	}

	static Value GetReturnType(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value(entry.functions.GetFunctionByOffset(offset).GetReturnType().ToString());
	}

	static vector<Value> GetParameters(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions.GetFunctionByOffset(offset).arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		return results;
	}

	static Value GetParameterTypes(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);
		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back(fun.arguments[i].ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return fun.arguments;
	}

	static Value GetVarArgs(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return !fun.HasVarArgs() ? Value() : Value(fun.varargs.ToString());
	}

	static Value GetMacroDefinition(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value IsVolatile(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value::BOOLEAN(entry.functions.GetFunctionByOffset(offset).GetStability() ==
		                      FunctionStability::VOLATILE);
	}

	static Value ResultType(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return FunctionStabilityToValue(entry.functions.GetFunctionByOffset(offset).GetStability());
	}
};

struct MacroExtractor {
	static idx_t FunctionCount(ScalarMacroCatalogEntry &entry) {
		return entry.macros.size();
	}

	static Value GetFunctionType() {
		return Value("macro");
	}

	static Value GetReturnType(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static vector<Value> GetParameters(ScalarMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto &macro_entry = *entry.macros[offset];
		for (auto &param : macro_entry.parameters) {
			D_ASSERT(param->GetExpressionType() == ExpressionType::COLUMN_REF);
			auto &colref = param->Cast<ColumnRefExpression>();
			results.emplace_back(colref.GetColumnName());
		}
		return results;
	}

	static Value GetParameterTypes(ScalarMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto &macro_entry = *entry.macros[offset];
		for (idx_t i = 0; i < macro_entry.parameters.size(); i++) {
			const auto has_type = !macro_entry.types.empty() && macro_entry.types[i] != LogicalType::UNKNOWN;
			results.emplace_back(has_type ? macro_entry.types[i].ToString() : Value(LogicalType::VARCHAR));
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(ScalarMacroCatalogEntry &entry, idx_t offset) {
		vector<LogicalType> results;
		auto &macro_entry = *entry.macros[offset];
		for (idx_t i = 0; i < macro_entry.parameters.size(); i++) {
			results.emplace_back(macro_entry.types.empty() ? LogicalType::UNKNOWN : macro_entry.types[i]);
		}
		return results;
	}

	static Value GetVarArgs(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetMacroDefinition(ScalarMacroCatalogEntry &entry, idx_t offset) {
		auto &macro_entry = *entry.macros[offset];
		D_ASSERT(macro_entry.type == MacroType::SCALAR_MACRO);
		auto &func = macro_entry.Cast<ScalarMacroFunction>();
		return func.expression->ToString();
	}

	static Value IsVolatile(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value ResultType(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct TableMacroExtractor {
	static idx_t FunctionCount(TableMacroCatalogEntry &entry) {
		return entry.macros.size();
	}

	static Value GetFunctionType() {
		return Value("table_macro");
	}

	static Value GetReturnType(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static vector<Value> GetParameters(TableMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto &macro_entry = *entry.macros[offset];
		for (auto &param : macro_entry.parameters) {
			D_ASSERT(param->GetExpressionType() == ExpressionType::COLUMN_REF);
			auto &colref = param->Cast<ColumnRefExpression>();
			results.emplace_back(colref.GetColumnName());
		}
		return results;
	}

	static Value GetParameterTypes(TableMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto &macro_entry = *entry.macros[offset];
		for (idx_t i = 0; i < macro_entry.parameters.size(); i++) {
			const auto has_type = !macro_entry.types.empty() && macro_entry.types[i] != LogicalType::UNKNOWN;
			results.emplace_back(has_type ? macro_entry.types[i].ToString() : Value(LogicalType::VARCHAR));
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(TableMacroCatalogEntry &entry, idx_t offset) {
		vector<LogicalType> results;
		auto &macro_entry = *entry.macros[offset];
		for (idx_t i = 0; i < macro_entry.parameters.size(); i++) {
			results.emplace_back(macro_entry.types.empty() ? LogicalType::UNKNOWN : macro_entry.types[i]);
		}
		return results;
	}

	static Value GetVarArgs(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetMacroDefinition(TableMacroCatalogEntry &entry, idx_t offset) {
		auto &macro_entry = *entry.macros[offset];
		if (macro_entry.type == MacroType::TABLE_MACRO) {
			auto &func = macro_entry.Cast<TableMacroFunction>();
			return func.query_node->ToString();
		}
		return Value();
	}

	static Value IsVolatile(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value ResultType(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct TableFunctionExtractor {
	static idx_t FunctionCount(TableFunctionCatalogEntry &entry) {
		return entry.functions.Size();
	}

	static Value GetFunctionType() {
		return Value("table");
	}

	static Value GetReturnType(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static vector<Value> GetParameters(TableFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);
		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		for (auto &param : fun.named_parameters) {
			results.emplace_back(param.first);
		}
		return results;
	}

	static Value GetParameterTypes(TableFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);

		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back(fun.arguments[i].ToString());
		}
		for (auto &param : fun.named_parameters) {
			results.emplace_back(param.second.ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(TableFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return fun.arguments;
	}

	static Value GetVarArgs(TableFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return !fun.HasVarArgs() ? Value() : Value(fun.varargs.ToString());
	}

	static Value GetMacroDefinition(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value IsVolatile(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value ResultType(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct PragmaFunctionExtractor {
	static idx_t FunctionCount(PragmaFunctionCatalogEntry &entry) {
		return entry.functions.Size();
	}

	static Value GetFunctionType() {
		return Value("pragma");
	}

	static Value GetReturnType(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static vector<Value> GetParameters(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);

		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		for (auto &param : fun.named_parameters) {
			results.emplace_back(param.first);
		}
		return results;
	}

	static Value GetParameterTypes(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		auto fun = entry.functions.GetFunctionByOffset(offset);

		for (idx_t i = 0; i < fun.arguments.size(); i++) {
			results.emplace_back(fun.arguments[i].ToString());
		}
		for (auto &param : fun.named_parameters) {
			results.emplace_back(param.second.ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, std::move(results));
	}

	static vector<LogicalType> GetParameterLogicalTypes(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return fun.arguments;
	}

	static Value GetVarArgs(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		auto fun = entry.functions.GetFunctionByOffset(offset);
		return !fun.HasVarArgs() ? Value() : Value(fun.varargs.ToString());
	}

	static Value GetMacroDefinition(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value IsVolatile(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value ResultType(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

static vector<Value> ToValueVector(vector<string> &string_vector) {
	vector<Value> result;
	for (string &str : string_vector) {
		result.emplace_back(Value(str));
	}
	return result;
}

template <class T, class OP>
static Value GetParameterNames(CatalogEntry &entry, idx_t function_idx, FunctionDescription &function_description,
                               Value &parameter_types) {
	vector<Value> parameter_names;
	if (!function_description.parameter_names.empty()) {
		for (idx_t param_idx = 0; param_idx < ListValue::GetChildren(parameter_types).size(); param_idx++) {
			if (param_idx < function_description.parameter_names.size()) {
				parameter_names.emplace_back(function_description.parameter_names[param_idx]);
			} else {
				parameter_names.emplace_back("col" + to_string(param_idx));
			}
		}
	} else {
		// fallback
		auto &function = entry.Cast<T>();
		parameter_names = OP::GetParameters(function, function_idx);
	}
	return Value::LIST(LogicalType::VARCHAR, parameter_names);
}

// returns values:
// 0: exact type match; N: match using N <ANY> values; Invalid(): no match
static optional_idx CalcDescriptionSpecificity(FunctionDescription &description,
                                               const vector<LogicalType> &parameter_types) {
	if (description.parameter_types.size() != parameter_types.size()) {
		return optional_idx::Invalid();
	}
	idx_t any_count = 0;
	for (idx_t i = 0; i < description.parameter_types.size(); i++) {
		if (description.parameter_types[i].id() == LogicalTypeId::ANY) {
			any_count++;
		} else if (description.parameter_types[i] != parameter_types[i]) {
			return optional_idx::Invalid();
		}
	}
	return any_count;
}

// Find FunctionDescription object with matching number of arguments and types
static optional_idx GetFunctionDescriptionIndex(vector<FunctionDescription> &function_descriptions,
                                                vector<LogicalType> &function_parameter_types) {
	if (function_descriptions.size() == 1) {
		// one description, use it even if nr of parameters don't match
		idx_t nr_function_parameters = function_parameter_types.size();
		for (idx_t i = 0; i < function_descriptions[0].parameter_types.size(); i++) {
			if (i < nr_function_parameters && function_descriptions[0].parameter_types[i] != LogicalTypeId::ANY &&
			    function_descriptions[0].parameter_types[i] != function_parameter_types[i]) {
				return optional_idx::Invalid();
			}
		}
		return optional_idx(0);
	}

	// multiple descriptions, search most specific description
	optional_idx best_description_idx;
	// specificity_score: 0: exact type match; N: match using N <ANY> values; Invalid(): no match
	optional_idx best_specificity_score;
	optional_idx specificity_score;
	for (idx_t descr_idx = 0; descr_idx < function_descriptions.size(); descr_idx++) {
		specificity_score = CalcDescriptionSpecificity(function_descriptions[descr_idx], function_parameter_types);
		if (specificity_score.IsValid() &&
		    (!best_specificity_score.IsValid() || specificity_score.GetIndex() < best_specificity_score.GetIndex())) {
			best_specificity_score = specificity_score;
			best_description_idx = descr_idx;
		}
	}
	return best_description_idx;
}

template <class T, class OP>
bool ExtractFunctionData(CatalogEntry &entry, idx_t function_idx, DataChunk &output, idx_t output_offset) {
	auto &function = entry.Cast<T>();
	vector<LogicalType> parameter_types_vector = OP::GetParameterLogicalTypes(function, function_idx);
	Value parameter_types_value = OP::GetParameterTypes(function, function_idx);
	optional_idx description_idx = GetFunctionDescriptionIndex(function.descriptions, parameter_types_vector);
	FunctionDescription function_description =
	    description_idx.IsValid() ? function.descriptions[description_idx.GetIndex()] : FunctionDescription();

	idx_t col = 0;

	// database_name, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, Value(function.schema.catalog.GetName()));

	// database_oid, BIGINT
	output.SetValue(col++, output_offset, Value::BIGINT(NumericCast<int64_t>(function.schema.catalog.GetOid())));

	// schema_name, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, Value(function.schema.name));

	// function_name, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, Value(function.name));

	// alias_of, LogicalType::VARCHAR
	output.SetValue(col++, output_offset,
	                function.alias_of.empty() || function.alias_of == function.name ? Value()
	                                                                                : Value(function.alias_of));

	// function_type, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, Value(OP::GetFunctionType()));

	// function_description, LogicalType::VARCHAR
	output.SetValue(col++, output_offset,
	                (function_description.description.empty()) ? Value() : Value(function_description.description));

	// comment, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, function.comment);

	// tags, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)
	output.SetValue(col++, output_offset, Value::MAP(function.tags));

	// return_type, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, OP::GetReturnType(function, function_idx));

	// parameters, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(col++, output_offset,
	                GetParameterNames<T, OP>(function, function_idx, function_description, parameter_types_value));

	// parameter_types, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(col++, output_offset, parameter_types_value);

	// varargs, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, OP::GetVarArgs(function, function_idx));

	// macro_definition, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, OP::GetMacroDefinition(function, function_idx));

	// has_side_effects, LogicalType::BOOLEAN
	output.SetValue(col++, output_offset, OP::IsVolatile(function, function_idx));

	// internal, LogicalType::BOOLEAN
	output.SetValue(col++, output_offset, Value::BOOLEAN(function.internal));

	// function_oid, LogicalType::BIGINT
	output.SetValue(col++, output_offset, Value::BIGINT(NumericCast<int64_t>(function.oid)));

	// examples, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(col++, output_offset,
	                Value::LIST(LogicalType::VARCHAR, ToValueVector(function_description.examples)));

	// stability, LogicalType::VARCHAR
	output.SetValue(col++, output_offset, OP::ResultType(function, function_idx));

	// categories, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(col++, output_offset,
	                Value::LIST(LogicalType::VARCHAR, ToValueVector(function_description.categories)));

	return function_idx + 1 == OP::FunctionCount(function);
}

void ExtractWindowFunctionData(ClientContext &context, const WindowFunctionDefinition *it, DataChunk &output,
                               idx_t output_offset) {
	D_ASSERT(it && it->name != nullptr);
	string name(it->name);

	auto &system_catalog = Catalog::GetSystemCatalog(DatabaseInstance::GetDatabase(context));
	string schema_name(DEFAULT_SCHEMA);
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema_name);
	auto &default_schema = system_catalog.GetSchema(context, schema_lookup);

	switch (it->expression_type) {
	case ExpressionType::WINDOW_FILL:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_FIRST_VALUE: {
		WindowFunctionCatalogEntry function(default_schema, name, {LogicalType::TEMPLATE("T")},
		                                    LogicalType::TEMPLATE("T"));
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	case ExpressionType::WINDOW_NTH_VALUE: {
		WindowFunctionCatalogEntry function(default_schema, name, {LogicalType::TEMPLATE("T"), LogicalType::BIGINT},
		                                    LogicalType::TEMPLATE("T"));
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE: {
		WindowFunctionCatalogEntry function(default_schema, name, {}, LogicalType::BIGINT);
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	case ExpressionType::WINDOW_NTILE: {
		WindowFunctionCatalogEntry function(default_schema, name, {LogicalType::BIGINT}, LogicalType::BIGINT);
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST: {
		WindowFunctionCatalogEntry function(default_schema, name, {}, LogicalType::DOUBLE);
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_LEAD: {
		WindowFunctionCatalogEntry function(
		    default_schema, name, {LogicalType::TEMPLATE("T"), LogicalType::BIGINT, LogicalType::TEMPLATE("T")},
		    LogicalType::TEMPLATE("T"));
		ExtractFunctionData<WindowFunctionCatalogEntry, WindowFunctionExtractor>(function, 0, output, output_offset);
		break;
	}
	default:
		throw InternalException("Window function '%s' not implemented", name);
	}
}

static bool Finished(const DuckDBFunctionsData &data) {
	if (data.offset < data.entries.size()) {
		return false;
	}
	if (data.window_iterator->name == nullptr) {
		return true;
	}
	return false;
}

void DuckDBFunctionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBFunctionsData>();
	if (Finished(data)) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset].get().Cast<FunctionEntry>();
		bool finished;

		switch (entry.type) {
		case CatalogType::SCALAR_FUNCTION_ENTRY:
			finished = ExtractFunctionData<ScalarFunctionCatalogEntry, ScalarFunctionExtractor>(
			    entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::AGGREGATE_FUNCTION_ENTRY:
			finished = ExtractFunctionData<AggregateFunctionCatalogEntry, AggregateFunctionExtractor>(
			    entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::TABLE_MACRO_ENTRY:
			finished = ExtractFunctionData<TableMacroCatalogEntry, TableMacroExtractor>(entry, data.offset_in_entry,
			                                                                            output, count);
			break;
		case CatalogType::MACRO_ENTRY:
			finished = ExtractFunctionData<ScalarMacroCatalogEntry, MacroExtractor>(entry, data.offset_in_entry, output,
			                                                                        count);
			break;
		case CatalogType::TABLE_FUNCTION_ENTRY:
			finished = ExtractFunctionData<TableFunctionCatalogEntry, TableFunctionExtractor>(
			    entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::PRAGMA_FUNCTION_ENTRY:
			finished = ExtractFunctionData<PragmaFunctionCatalogEntry, PragmaFunctionExtractor>(
			    entry, data.offset_in_entry, output, count);
			break;
		default:
			throw InternalException("FIXME: unrecognized function type in duckdb_functions");
		}
		if (finished) {
			// finished with this function, move to the next function
			data.offset++;
			data.offset_in_entry = 0;
		} else {
			// more functions remain
			data.offset_in_entry++;
		}
		count++;
	}
	while (data.window_iterator->name != nullptr && count < STANDARD_VECTOR_SIZE) {
		ExtractWindowFunctionData(context, data.window_iterator, output, count);
		count++;
		data.window_iterator++;
	}
	output.SetCardinality(count);
}

void DuckDBFunctionsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_functions", {}, DuckDBFunctionsFunction, DuckDBFunctionsBind, DuckDBFunctionsInit));
}

} // namespace duckdb
