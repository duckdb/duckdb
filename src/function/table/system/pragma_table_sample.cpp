#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaTableSampleFunctionData : public TableFunctionData {
	explicit PragmaTableSampleFunctionData(CatalogEntry &entry_p) : entry(entry_p) {
	}
	CatalogEntry &entry;
};

struct PragmaTableSampleOperatorData : public GlobalTableFunctionState {
	PragmaTableSampleOperatorData() : sample_offset(0) {
		sample = nullptr;
	}
	idx_t sample_offset;
	unique_ptr<BlockingSample> sample;
};

static unique_ptr<FunctionData> PragmaTableSampleBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {

	// look up the table name in the catalog
	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);

	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qname.catalog, qname.schema, qname.name);
	if (entry.type != CatalogType::TABLE_ENTRY) {
		throw NotImplementedException("Invalid Catalog type passed to table_sample()");
	}
	auto &table_entry = entry.Cast<TableCatalogEntry>();
	auto types = table_entry.GetTypes();
	for (auto &type : types) {
		return_types.push_back(type);
	}
	for (idx_t i = 0; i < types.size(); i++) {
		auto logical_index = LogicalIndex(i);
		auto &col = table_entry.GetColumn(logical_index);
		names.push_back(col.GetName());
	}

	return make_uniq<PragmaTableSampleFunctionData>(entry);
}

unique_ptr<GlobalTableFunctionState> PragmaTableSampleInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaTableSampleOperatorData>();
}

static void PragmaTableSampleTable(ClientContext &context, PragmaTableSampleOperatorData &data,
                                   TableCatalogEntry &table, DataChunk &output) {
	// if table has statistics.
	// copy the sample of statistics into the output chunk
	if (!data.sample) {
		data.sample = table.GetSample();
	}
	if (data.sample) {
		auto sample_chunk = data.sample->GetChunk(data.sample_offset);
		if (sample_chunk) {
			sample_chunk->Copy(output, 0);
			data.sample_offset += sample_chunk->size();
		}
	}
}

static void PragmaTableSampleFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaTableSampleFunctionData>();
	auto &state = data_p.global_state->Cast<PragmaTableSampleOperatorData>();
	switch (bind_data.entry.type) {
	case CatalogType::TABLE_ENTRY:
		PragmaTableSampleTable(context, state, bind_data.entry.Cast<TableCatalogEntry>(), output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_sample");
	}
}

void PragmaTableSample::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_table_sample", {LogicalType::VARCHAR}, PragmaTableSampleFunction,
	                              PragmaTableSampleBind, PragmaTableSampleInit));
}

} // namespace duckdb
