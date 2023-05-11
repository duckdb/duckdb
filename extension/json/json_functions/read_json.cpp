#include "duckdb/common/multi_file_reader.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_structure.hpp"
#include "json_transform.hpp"

namespace duckdb {

void JSONScan::AutoDetect(ClientContext &context, JSONScanData &bind_data, vector<LogicalType> &return_types,
                          vector<string> &names) {
	auto original_scan_type = bind_data.type;
	bind_data.type = JSONScanType::SAMPLE; // Set scan type to sample for the auto-detect, we restore it later
	JSONScanGlobalState gstate(context, bind_data);
	JSONScanLocalState lstate(context, gstate);
	ArenaAllocator allocator(BufferAllocator::Get(context));

	// Read for the specified sample size
	JSONStructureNode node;
	bool more_than_one = false;
	Vector string_vector(LogicalType::VARCHAR);
	idx_t remaining = bind_data.sample_size;
	while (remaining != 0) {
		allocator.Reset();

		if (gstate.file_index >= 10) {
			// We really shouldn't open more than 10 files when sampling
			break;
		}

		auto read_count = lstate.ReadNext(gstate);
		if (lstate.scan_count > 1) {
			more_than_one = true;
		}
		if (read_count == 0) {
			break;
		}
		idx_t next = MinValue<idx_t>(read_count, remaining);
		yyjson_val **values;
		if (bind_data.record_type == JSONRecordType::ARRAY_OF_RECORDS ||
		    bind_data.record_type == JSONRecordType::ARRAY_OF_JSON) {
			values = lstate.array_values;
		} else {
			values = lstate.values;
		}
		for (idx_t i = 0; i < next; i++) {
			if (values[i]) {
				JSONStructure::ExtractStructure(values[i], node);
			}
		}
		if (!node.ContainsVarchar()) { // Can't refine non-VARCHAR types
			continue;
		}
		node.InitializeCandidateTypes(bind_data.max_depth);
		node.RefineCandidateTypes(values, next, string_vector, allocator, bind_data.date_format_map);
		remaining -= next;
	}
	bind_data.type = original_scan_type;

	// Convert structure to logical type
	auto type = JSONStructure::StructureToType(context, node, bind_data.max_depth);

	// Detect record type
	if (bind_data.record_type == JSONRecordType::AUTO) {
		switch (type.id()) {
		case LogicalTypeId::STRUCT:
			bind_data.record_type = JSONRecordType::RECORDS;
			break;
		case LogicalTypeId::LIST: {
			if (more_than_one) {
				bind_data.record_type = JSONRecordType::JSON;
			} else {
				type = ListType::GetChildType(type);
				if (type.id() == LogicalTypeId::STRUCT) {
					bind_data.record_type = JSONRecordType::ARRAY_OF_RECORDS;
				} else {
					bind_data.record_type = JSONRecordType::ARRAY_OF_JSON;
				}
			}
			break;
		}
		default:
			bind_data.record_type = JSONRecordType::JSON;
		}
	}

	// Detect return type
	if (bind_data.auto_detect) {
		bind_data.transform_options.date_format_map = &bind_data.date_format_map;
		if (type.id() != LogicalTypeId::STRUCT) {
			return_types.emplace_back(type);
			names.emplace_back("json");
		} else {
			const auto &child_types = StructType::GetChildTypes(type);
			return_types.reserve(child_types.size());
			names.reserve(child_types.size());
			for (auto &child_type : child_types) {
				return_types.emplace_back(child_type.second);
				names.emplace_back(child_type.first);
			}
		}
	}

	for (auto &reader : gstate.json_readers) {
		if (reader->IsOpen()) {
			reader->Reset();
		}
	}
	bind_data.stored_readers = std::move(gstate.json_readers);
}

void JSONScan::InitializeBindData(ClientContext &context, JSONScanData &bind_data,
                                  const named_parameter_map_t &named_parameters, vector<string> &names,
                                  vector<LogicalType> &return_types) {
	for (auto &kv : named_parameters) {
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
		} else if (loption == "maximum_depth") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data.max_depth = NumericLimits<idx_t>::Maximum();
			} else {
				bind_data.max_depth = arg;
			}
		} else if (loption == "dateformat" || loption == "date_format") {
			auto format_string = StringValue::Get(kv.second);
			if (StringUtil::Lower(format_string) == "iso") {
				format_string = "%Y-%m-%d";
			}
			bind_data.date_format = format_string;

			StrpTimeFormat format;
			auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
			}
		} else if (loption == "timestampformat" || loption == "timestamp_format") {
			auto format_string = StringValue::Get(kv.second);
			if (StringUtil::Lower(format_string) == "iso") {
				format_string = "%Y-%m-%dT%H:%M:%S.%fZ";
			}
			bind_data.timestamp_format = format_string;

			StrpTimeFormat format;
			auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw InvalidInputException("Could not parse TIMESTAMPFORMAT: %s", error.c_str());
			}
		} else if (loption == "json_format") {
			auto arg = StringValue::Get(kv.second);
			if (arg == "records") {
				bind_data.record_type = JSONRecordType::RECORDS;
			} else if (arg == "array_of_records") {
				bind_data.record_type = JSONRecordType::ARRAY_OF_RECORDS;
			} else if (arg == "values") {
				bind_data.record_type = JSONRecordType::JSON;
			} else if (arg == "array_of_values") {
				bind_data.record_type = JSONRecordType::ARRAY_OF_JSON;
			} else if (arg == "auto") {
				bind_data.record_type = JSONRecordType::AUTO;
			} else {
				throw InvalidInputException("\"json_format\" must be one of ['records', 'array_of_records', 'json', "
				                            "'array_of_json', 'auto']");
			}
		}
	}
}

unique_ptr<FunctionData> ReadJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	// First bind default params
	auto result = JSONScanData::Bind(context, input);
	auto &bind_data = (JSONScanData &)*result;

	JSONScan::InitializeBindData(context, bind_data, input.named_parameters, names, return_types);

	if (!bind_data.names.empty()) {
		bind_data.auto_detect = false; // override auto_detect when columns are specified
	} else if (!bind_data.auto_detect) {
		throw BinderException("read_json \"columns\" parameter is required when auto_detect is false");
	}

	bind_data.InitializeFormats();

	if (bind_data.auto_detect || bind_data.record_type == JSONRecordType::AUTO) {
		JSONScan::AutoDetect(context, bind_data, return_types, names);
		bind_data.names = names;
	}

	auto &transform_options = bind_data.transform_options;
	transform_options.strict_cast = !bind_data.ignore_errors;
	transform_options.error_duplicate_key = !bind_data.ignore_errors;
	transform_options.error_missing_key = false;
	transform_options.error_unknown_key = bind_data.auto_detect && !bind_data.ignore_errors;
	transform_options.delay_error = true;

	return result;
}

static void ReadJSONFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = ((JSONGlobalTableFunctionState &)*data_p.global_state).state;
	auto &lstate = ((JSONLocalTableFunctionState &)*data_p.local_state).state;

	const auto count = lstate.ReadNext(gstate);
	yyjson_val **values;
	if (gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_RECORDS ||
	    gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_JSON) {
		values = lstate.array_values;
	} else {
		D_ASSERT(gstate.bind_data.record_type != JSONRecordType::AUTO);
		values = lstate.values;
	}
	output.SetCardinality(count);

	vector<Vector *> result_vectors;
	result_vectors.reserve(output.ColumnCount());
	for (auto &valid_col_idx : gstate.bind_data.valid_cols) {
		result_vectors.push_back(&output.data[valid_col_idx]);
	}
	D_ASSERT(result_vectors.size() == gstate.bind_data.names.size());

	// Pass current reader to transform options so we can get line number information if an error occurs
	bool success;
	if (gstate.bind_data.record_type == JSONRecordType::RECORDS ||
	    gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_RECORDS) {
		success = JSONTransform::TransformObject(values, lstate.GetAllocator(), count, gstate.bind_data.names,
		                                         result_vectors, lstate.transform_options);
	} else {
		success = JSONTransform::Transform(values, lstate.GetAllocator(), *result_vectors[0], count,
		                                   lstate.transform_options);
	}

	if (!success) {
		string hint =
		    gstate.bind_data.auto_detect
		        ? "\nTry increasing 'sample_size', reducing 'maximum_depth', specifying 'columns', 'lines' or "
		          "'json_format' manually, or setting 'ignore_errors' to true."
		        : "\nTry setting 'auto_detect' to true, specifying 'lines' or 'json_format' manually, or setting "
		          "'ignore_errors' to true.";
		lstate.ThrowTransformError(lstate.transform_options.object_index,
		                           lstate.transform_options.error_message + hint);
	}
}

TableFunction JSONFunctions::GetReadJSONTableFunction(shared_ptr<JSONScanInfo> function_info) {
	TableFunction table_function({LogicalType::VARCHAR}, ReadJSONFunction, ReadJSONBind,
	                             JSONGlobalTableFunctionState::Init, JSONLocalTableFunctionState::Init);

	JSONScan::TableFunctionDefaults(table_function);
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["date_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestamp_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["json_format"] = LogicalType::VARCHAR;

	table_function.projection_pushdown = true;
	// TODO: might be able to do filter pushdown/prune too

	table_function.function_info = std::move(function_info);

	return table_function;
}

TableFunctionSet CreateJSONFunctionInfo(string name, shared_ptr<JSONScanInfo> info, bool auto_function = false) {
	auto table_function = JSONFunctions::GetReadJSONTableFunction(std::move(info));
	table_function.name = std::move(name);
	if (auto_function) {
		table_function.named_parameters["maximum_depth"] = LogicalType::BIGINT;
	}
	return MultiFileReader::CreateFunctionSet(table_function);
}

TableFunctionSet JSONFunctions::GetReadJSONFunction() {
	auto info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::UNSTRUCTURED, JSONRecordType::RECORDS, false);
	return CreateJSONFunctionInfo("read_json", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadNDJSONFunction() {
	auto info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED,
	                                      JSONRecordType::RECORDS, false);
	return CreateJSONFunctionInfo("read_ndjson", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadJSONAutoFunction() {
	auto info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, JSONRecordType::AUTO, true);
	return CreateJSONFunctionInfo("read_json_auto", std::move(info), true);
}

TableFunctionSet JSONFunctions::GetReadNDJSONAutoFunction() {
	auto info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED, JSONRecordType::AUTO, true);
	return CreateJSONFunctionInfo("read_ndjson_auto", std::move(info), true);
}

} // namespace duckdb
