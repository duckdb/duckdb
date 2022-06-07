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
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBFunctionsData : public GlobalTableFunctionState {
	DuckDBFunctionsData() : offset(0), offset_in_entry(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t offset_in_entry;
};

static unique_ptr<FunctionData> DuckDBFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("function_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("function_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

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

	return nullptr;
}

static void ExtractFunctionsFromSchema(ClientContext &context, SchemaCatalogEntry &schema,
                                       DuckDBFunctionsData &result) {
	schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY,
	            [&](CatalogEntry *entry) { result.entries.push_back(entry); });
	schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY,
	            [&](CatalogEntry *entry) { result.entries.push_back(entry); });
	schema.Scan(context, CatalogType::PRAGMA_FUNCTION_ENTRY,
	            [&](CatalogEntry *entry) { result.entries.push_back(entry); });
}

unique_ptr<GlobalTableFunctionState> DuckDBFunctionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBFunctionsData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		ExtractFunctionsFromSchema(context, *schema, *result);
	};
	ExtractFunctionsFromSchema(context, *ClientData::Get(context).temporary_objects, *result);

	std::sort(result->entries.begin(), result->entries.end(),
	          [&](CatalogEntry *a, CatalogEntry *b) { return (int)a->type < (int)b->type; });
	return move(result);
}

struct ScalarFunctionExtractor {
	static idx_t FunctionCount(ScalarFunctionCatalogEntry &entry) {
		return entry.functions.size();
	}

	static Value GetFunctionType() {
		return Value("scalar");
	}

	static Value GetFunctionDescription(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value(entry.functions[offset].return_type.ToString());
	}

	static Value GetParameters(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back(entry.functions[offset].arguments[i].ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return entry.functions[offset].varargs.id() == LogicalTypeId::INVALID
		           ? Value()
		           : Value(entry.functions[offset].varargs.ToString());
	}

	static Value GetMacroDefinition(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value HasSideEffects(ScalarFunctionCatalogEntry &entry, idx_t offset) {
		return Value::BOOLEAN(entry.functions[offset].has_side_effects);
	}
};

struct AggregateFunctionExtractor {
	static idx_t FunctionCount(AggregateFunctionCatalogEntry &entry) {
		return entry.functions.size();
	}

	static Value GetFunctionType() {
		return Value("aggregate");
	}

	static Value GetFunctionDescription(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value(entry.functions[offset].return_type.ToString());
	}

	static Value GetParameters(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back(entry.functions[offset].arguments[i].ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return entry.functions[offset].varargs.id() == LogicalTypeId::INVALID
		           ? Value()
		           : Value(entry.functions[offset].varargs.ToString());
	}

	static Value GetMacroDefinition(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value HasSideEffects(AggregateFunctionCatalogEntry &entry, idx_t offset) {
		return Value::BOOLEAN(entry.functions[offset].has_side_effects);
	}
};

struct MacroExtractor {
	static idx_t FunctionCount(ScalarMacroCatalogEntry &entry) {
		return 1;
	}

	static Value GetFunctionType() {
		return Value("macro");
	}

	static Value GetFunctionDescription(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetParameters(ScalarMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (auto &param : entry.function->parameters) {
			D_ASSERT(param->type == ExpressionType::COLUMN_REF);
			auto &colref = (ColumnRefExpression &)*param;
			results.emplace_back(colref.GetColumnName());
		}
		for (auto &param_entry : entry.function->default_parameters) {
			results.emplace_back(param_entry.first);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(ScalarMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.function->parameters.size(); i++) {
			results.emplace_back(LogicalType::VARCHAR);
		}
		for (idx_t i = 0; i < entry.function->default_parameters.size(); i++) {
			results.emplace_back(LogicalType::VARCHAR);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetMacroDefinition(ScalarMacroCatalogEntry &entry, idx_t offset) {
		D_ASSERT(entry.function->type == MacroType::SCALAR_MACRO);
		auto &func = (ScalarMacroFunction &)*entry.function;
		return func.expression->ToString();
	}

	static Value HasSideEffects(ScalarMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct TableMacroExtractor {
	static idx_t FunctionCount(TableMacroCatalogEntry &entry) {
		return 1;
	}

	static Value GetFunctionType() {
		return Value("table_macro");
	}

	static Value GetFunctionDescription(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetParameters(TableMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (auto &param : entry.function->parameters) {
			D_ASSERT(param->type == ExpressionType::COLUMN_REF);
			auto &colref = (ColumnRefExpression &)*param;
			results.emplace_back(colref.GetColumnName());
		}
		for (auto &param_entry : entry.function->default_parameters) {
			results.emplace_back(param_entry.first);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(TableMacroCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.function->parameters.size(); i++) {
			results.emplace_back(LogicalType::VARCHAR);
		}
		for (idx_t i = 0; i < entry.function->default_parameters.size(); i++) {
			results.emplace_back(LogicalType::VARCHAR);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetMacroDefinition(TableMacroCatalogEntry &entry, idx_t offset) {
		if (entry.function->type == MacroType::SCALAR_MACRO) {
			auto &func = (ScalarMacroFunction &)*entry.function;
			return func.expression->ToString();
		}
		return Value();
	}

	static Value HasSideEffects(TableMacroCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct TableFunctionExtractor {
	static idx_t FunctionCount(TableFunctionCatalogEntry &entry) {
		return entry.functions.size();
	}

	static Value GetFunctionType() {
		return Value("table");
	}

	static Value GetFunctionDescription(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetParameters(TableFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		for (auto &param : entry.functions[offset].named_parameters) {
			results.emplace_back(param.first);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(TableFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back(entry.functions[offset].arguments[i].ToString());
		}
		for (auto &param : entry.functions[offset].named_parameters) {
			results.emplace_back(param.second.ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(TableFunctionCatalogEntry &entry, idx_t offset) {
		return entry.functions[offset].varargs.id() == LogicalTypeId::INVALID
		           ? Value()
		           : Value(entry.functions[offset].varargs.ToString());
	}

	static Value GetMacroDefinition(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value HasSideEffects(TableFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

struct PragmaFunctionExtractor {
	static idx_t FunctionCount(PragmaFunctionCatalogEntry &entry) {
		return entry.functions.size();
	}

	static Value GetFunctionType() {
		return Value("pragma");
	}

	static Value GetFunctionDescription(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetReturnType(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value GetParameters(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back("col" + to_string(i));
		}
		for (auto &param : entry.functions[offset].named_parameters) {
			results.emplace_back(param.first);
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetParameterTypes(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		vector<Value> results;
		for (idx_t i = 0; i < entry.functions[offset].arguments.size(); i++) {
			results.emplace_back(entry.functions[offset].arguments[i].ToString());
		}
		for (auto &param : entry.functions[offset].named_parameters) {
			results.emplace_back(param.second.ToString());
		}
		return Value::LIST(LogicalType::VARCHAR, move(results));
	}

	static Value GetVarArgs(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return entry.functions[offset].varargs.id() == LogicalTypeId::INVALID
		           ? Value()
		           : Value(entry.functions[offset].varargs.ToString());
	}

	static Value GetMacroDefinition(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}

	static Value HasSideEffects(PragmaFunctionCatalogEntry &entry, idx_t offset) {
		return Value();
	}
};

template <class T, class OP>
bool ExtractFunctionData(StandardEntry *entry, idx_t function_idx, DataChunk &output, idx_t output_offset) {
	auto &function = (T &)*entry;
	// schema_name, LogicalType::VARCHAR
	output.SetValue(0, output_offset, Value(entry->schema->name));

	// function_name, LogicalType::VARCHAR
	output.SetValue(1, output_offset, Value(entry->name));

	// function_type, LogicalType::VARCHAR
	output.SetValue(2, output_offset, Value(OP::GetFunctionType()));

	// function_description, LogicalType::VARCHAR
	output.SetValue(3, output_offset, OP::GetFunctionDescription(function, function_idx));

	// return_type, LogicalType::VARCHAR
	output.SetValue(4, output_offset, OP::GetReturnType(function, function_idx));

	// parameters, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(5, output_offset, OP::GetParameters(function, function_idx));

	// parameter_types, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(6, output_offset, OP::GetParameterTypes(function, function_idx));

	// varargs, LogicalType::VARCHAR
	output.SetValue(7, output_offset, OP::GetVarArgs(function, function_idx));

	// macro_definition, LogicalType::VARCHAR
	output.SetValue(8, output_offset, OP::GetMacroDefinition(function, function_idx));

	// has_side_effects, LogicalType::BOOLEAN
	output.SetValue(9, output_offset, OP::HasSideEffects(function, function_idx));

	return function_idx + 1 == OP::FunctionCount(function);
}

void DuckDBFunctionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBFunctionsData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];
		auto standard_entry = (StandardEntry *)entry;
		bool finished = false;

		switch (entry->type) {
		case CatalogType::SCALAR_FUNCTION_ENTRY:
			finished = ExtractFunctionData<ScalarFunctionCatalogEntry, ScalarFunctionExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::AGGREGATE_FUNCTION_ENTRY:
			finished = ExtractFunctionData<AggregateFunctionCatalogEntry, AggregateFunctionExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::TABLE_MACRO_ENTRY:
			finished = ExtractFunctionData<TableMacroCatalogEntry, TableMacroExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
			break;

		case CatalogType::MACRO_ENTRY:
			finished = ExtractFunctionData<ScalarMacroCatalogEntry, MacroExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::TABLE_FUNCTION_ENTRY:
			finished = ExtractFunctionData<TableFunctionCatalogEntry, TableFunctionExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
			break;
		case CatalogType::PRAGMA_FUNCTION_ENTRY:
			finished = ExtractFunctionData<PragmaFunctionCatalogEntry, PragmaFunctionExtractor>(
			    standard_entry, data.offset_in_entry, output, count);
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
	output.SetCardinality(count);
}

void DuckDBFunctionsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_functions", {}, DuckDBFunctionsFunction, DuckDBFunctionsBind, DuckDBFunctionsInit));
}

} // namespace duckdb
