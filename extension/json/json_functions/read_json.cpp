#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"

namespace duckdb {

unique_ptr<FunctionData> ReadJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	// First bind default params
	auto result = JSONScanData::Bind(context, input);
	auto &bind_data = (JSONScanData &)*result;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_json \"columns\" parameter requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_json \"columns\" parameter type specification must be VARCHAR");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			D_ASSERT(names.size() == return_types.size());
			if (names.empty()) {
				throw BinderException("read_json \"columns\" parameter needs at least one column");
			}
			bind_data.names = names;
		} else if (loption == "auto_detect") {
			bind_data.auto_detect = BooleanValue::Get(kv.second);
		} else if (loption == "sample_size") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data.sample_size = NumericLimits<idx_t>::Maximum();
			} else if (arg > 0) {
				bind_data.sample_size = arg;
			} else {
				throw BinderException(
				    "read_json \"sample_size\" parameter must be positive, or -1 to sample the entire file");
			}
		}
	}

	if (!bind_data.names.empty()) {
		bind_data.auto_detect = false; // override auto-detect when columns are specified
	} else if (!bind_data.auto_detect) {
		throw BinderException("read_json \"columns\" parameter is required when auto_detect is false");
	}

	if (bind_data.auto_detect) {
		// TODO: detect the schemaaaaa
	}

	auto &transform_options = bind_data.transform_options;
	transform_options.strict_cast = !bind_data.ignore_errors;
	transform_options.error_duplicate_key = !bind_data.ignore_errors;
	transform_options.error_missing_key = false;
	transform_options.error_unknown_key = bind_data.auto_detect; // Might still be set to false if we do proj pushdown

	return result;
}

void AutoDetect(ClientContext &context, JSONScanData &bind_data) {
	//	BufferedJSONReader reader(context, bind_data.options, 0, bind_data.file_paths[0]);
	//	reader.OpenJSONFile();
}

static void ReadJSONFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = ((JSONGlobalTableFunctionState &)*data_p.global_state).state;
	auto &lstate = ((JSONLocalTableFunctionState &)*data_p.local_state).state;
	D_ASSERT(output.ColumnCount() == gstate.bind_data.names.size());

	// Fetch next lines
	const auto count = lstate.ReadNext(gstate);
	const auto objects = lstate.objects;

	vector<Vector *> result_vectors;
	result_vectors.reserve(output.ColumnCount());
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		result_vectors.push_back(&output.data[col_idx]);
	}

	// TODO: if errors occur during transformation, we don't have line number information
	JSONTransform::TransformObject(objects, lstate.GetAllocator(), count, gstate.bind_data.names, result_vectors,
	                               gstate.bind_data.transform_options);
	output.SetCardinality(count);
}

TableFunction GetReadJSONTableFunction(bool list_parameter, shared_ptr<JSONScanInfo> function_info) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction table_function({parameter}, ReadJSONFunction, ReadJSONBind, JSONGlobalTableFunctionState::Init,
	                             JSONLocalTableFunctionState::Init);

	JSONScan::TableFunctionDefaults(table_function);
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;

	table_function.projection_pushdown = true;

	table_function.function_info = std::move(function_info);

	return table_function;
}

CreateTableFunctionInfo JSONFunctions::GetReadJSONFunction() {
	TableFunctionSet function_set("read_json");
	auto function_info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::UNSTRUCTURED, false);
	function_set.AddFunction(GetReadJSONTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo JSONFunctions::GetReadNDJSONFunction() {
	TableFunctionSet function_set("read_ndjson");
	auto function_info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED, false);
	function_set.AddFunction(GetReadJSONTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb
