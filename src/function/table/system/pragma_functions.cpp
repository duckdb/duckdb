#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct PragmaFunctionsData : public GlobalTableFunctionState {
	PragmaFunctionsData() : offset(0), offset_in_entry(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t offset_in_entry;
};

static unique_ptr<FunctionData> PragmaFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parameters");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("varargs");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("return_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("side_effects");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> PragmaFunctionsInit(ClientContext &context, TableFunctionInitInput &input) {
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
	auto result_data = FlatVector::GetData<list_entry_t>(output.data[2]);
	result_data[count].offset = ListVector::GetListSize(output.data[2]);
	result_data[count].length = f.arguments.size();
	string parameters;
	for (idx_t i = 0; i < f.arguments.size(); i++) {
		auto val = Value(f.arguments[i].ToString());
		ListVector::PushBack(output.data[2], val);
	}

	output.SetValue(3, count, f.varargs.id() != LogicalTypeId::INVALID ? Value(f.varargs.ToString()) : Value());
	output.SetValue(4, count, f.return_type.ToString());
	output.SetValue(5, count, Value::BOOLEAN(f.side_effects == FunctionSideEffects::HAS_SIDE_EFFECTS));

	count++;
}

static void PragmaFunctionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PragmaFunctionsData &)*data_p.global_state;
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
	    TableFunction("pragma_functions", {}, PragmaFunctionsFunction, PragmaFunctionsBind, PragmaFunctionsInit));
}

} // namespace duckdb
