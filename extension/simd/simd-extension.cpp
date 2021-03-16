#include "simd-extension.hpp"
#include "iostream"
#include "cpu_info.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include <duckdb/common/string_util.hpp>
#include "duckdb/parser/parsed_data/drop_info.hpp"

#ifdef DUCKDB_X86_64
#include "avx2_functions.hpp"
#include "avx512f_functions.hpp"
#endif

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#endif

namespace duckdb {

struct PragmaCpuFeaturesOutputOperatorData : public FunctionOperatorData {
	explicit PragmaCpuFeaturesOutputOperatorData(idx_t rows) : rows(rows) {
	}
	idx_t rows;
};

struct PragmaCpuFeaturesOutputData : public TableFunctionData {
	explicit PragmaCpuFeaturesOutputData() {
	}
	CpuInfo cpuInfo;
};

static unique_ptr<FunctionData> PragmaCpuFeaturesOutputBind(ClientContext &context, vector<Value> &inputs,
                                                            unordered_map<string, Value> &named_parameters,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("CPU_FEATURE");
	return_types.push_back(LogicalType::VARCHAR);

	return make_unique<PragmaCpuFeaturesOutputData>();
}

unique_ptr<FunctionOperatorData> PragmaCpuFeaturesOutputInit(ClientContext &context, const FunctionData *bind_data,
                                                             vector<column_t> &column_ids,
                                                             TableFilterCollection *filters) {
	return make_unique<PragmaCpuFeaturesOutputOperatorData>(1024);
}

static void PragmaCpuFeaturesOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                            FunctionOperatorData *operator_state, DataChunk &output) {
	auto &state = (PragmaCpuFeaturesOutputOperatorData &)*operator_state;
	auto &data = (PragmaCpuFeaturesOutputData &)*bind_data_p;
	if (state.rows > 0) {
		int index = 0;
		for (auto feature : data.cpuInfo.GetAvailFeatures()) {
			output.SetValue(0, index++, CPUFeatureToString(feature));
		}
		state.rows = 0;
		output.SetCardinality(index);
	} else {
		output.SetCardinality(0);
	}
}

static void PragmaSetCPUFeature(ClientContext &context, const FunctionParameters &parameters) {
	string feature_s = StringUtil::Upper(parameters.values[0].ToString());
	CPUFeature feature = DUCKDB_CPU_FALLBACK;
	CpuInfo cpuInfo;
	if (table.find(feature_s) != table.end()) {
		feature = table.find(feature_s)->second;
		if (cpuInfo.HasFeature(feature)) {
			cpuInfo.SetFeature(context, feature);
		}
	} else {
		throw ParserException("%s is not supported", feature);
	}
}

void SIMDExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	CpuInfo cpu_info;
	switch (cpu_info.GetBestFeature()) {
#ifdef DUCKDB_X86_64
	case CPUFeature::DUCKDB_CPU_FEATURE_X86_AVX2: {
		CreateScalarFunctionInfo info(AVX2Functions::GetAddFunctions());
		DropInfo dropInfo;
		dropInfo.name = "+";
		dropInfo.schema = "main";
		dropInfo.type = CatalogType::SCALAR_FUNCTION_ENTRY;
		catalog.DropEntry(*con.context, &dropInfo);
		catalog.CreateFunction(*con.context, &info);
	}
	case CPUFeature::DUCKDB_CPU_FEATURE_X86_AVX512F:
		AVX512fFunctions::GetFunctions();
		break;
#elif defined(DUCKDB_ARM)
#endif
	default:
		break;
	}

	auto set_cpu_func = PragmaFunction::PragmaAssignment("set_cpu_feature", PragmaSetCPUFeature, LogicalType::VARCHAR);
	CreatePragmaFunctionInfo info(set_cpu_func);
	catalog.CreatePragmaFunction(*con.context, &info);

	auto cpu_features = TableFunction("pragma_cpu_features_output", {}, PragmaCpuFeaturesOutputFunction,
	                                  PragmaCpuFeaturesOutputBind, PragmaCpuFeaturesOutputInit);
	CreateTableFunctionInfo info_cpu(cpu_features);
	catalog.CreateTableFunction(*con.context, &info_cpu);

	con.Commit();
}

} // namespace duckdb
