#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct PragmaFunctionsData : public FunctionOperatorData {
	PragmaFunctionsData() : offset(0), offset_in_entry(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t offset_in_entry;
};

static unique_ptr<FunctionData> pragma_functions_bind(ClientContext &context, vector<Value> &inputs,
                                                      unordered_map<string, Value> &named_parameters,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("parameters");
	child_list_t<LogicalType> child_types;
	child_types.push_back(std::make_pair("", LogicalType::VARCHAR));
	LogicalType param_types(LogicalTypeId::LIST, move(child_types));
	return_types.push_back(move(param_types));

	names.push_back("varargs");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("return_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("side_effects");
	return_types.push_back(LogicalType::BOOLEAN);

	return nullptr;
}

unique_ptr<FunctionOperatorData> pragma_functions_init(ClientContext &context, const FunctionData *bind_data,
                                                       vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto result = make_unique<PragmaFunctionsData>();

	Catalog::GetCatalog(context).schemas->Scan(context, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY,
		             [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	});

	return move(result);
}

void AddFunction(BaseScalarFunction &f, idx_t &count, DataChunk &output, bool is_aggregate) {
	output.SetValue(0, count, Value(f.name));
	output.SetValue(1, count, Value(is_aggregate ? "AGGREGATE" : "SCALAR"));
	if (!ListVector::HasEntry(output.data[2])) {
		ListVector::SetEntry(output.data[2], make_unique<ChunkCollection>());
	}
	auto &cc = ListVector::GetEntry(output.data[2]);
	auto result_data = FlatVector::GetData<list_entry_t>(output.data[2]);
	result_data[count].offset = cc.Count();
	result_data[count].length = f.arguments.size();
	string parameters;
	vector<LogicalType> types {LogicalType::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(types);
	for (idx_t i = 0; i < f.arguments.size(); i++) {
		chunk.data[0].SetValue(chunk.size(), Value(f.arguments[i].ToString()));
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			cc.Append(chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		cc.Append(chunk);
	}
	output.SetValue(3, count, f.varargs.id() != LogicalTypeId::INVALID ? Value(f.varargs.ToString()) : Value());
	output.SetValue(4, count, f.return_type.ToString());
	output.SetValue(5, count, Value::BOOLEAN(f.has_side_effects));

	count++;
}

static void pragma_functions(ClientContext &context, const FunctionData *bind_data,
                             FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (PragmaFunctionsData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && data.offset < data.entries.size()) {
		auto &entry = data.entries[data.offset];
		switch (entry->type) {
		case CatalogType::SCALAR_FUNCTION_ENTRY: {
			auto &func = (ScalarFunctionCatalogEntry &)*entry;
			if (data.offset_in_entry >= func.functions.size()) {
				data.offset++;
				data.offset_in_entry = 0;
				break;
			}
			AddFunction(func.functions[data.offset_in_entry++], count, output, false);
			break;
		}
		case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
			auto &aggr = (AggregateFunctionCatalogEntry &)*entry;
			if (data.offset_in_entry >= aggr.functions.size()) {
				data.offset++;
				data.offset_in_entry = 0;
				break;
			}
			AddFunction(aggr.functions[data.offset_in_entry++], count, output, true);
			break;
		}
		default:
			data.offset++;
			break;
		}
	}
	output.SetCardinality(count);
}

void PragmaFunctionPragma::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("pragma_functions", {}, pragma_functions, pragma_functions_bind, pragma_functions_init));
}

} // namespace duckdb
