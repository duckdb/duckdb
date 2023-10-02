#include "duckdb/common/multi_file_reader.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_structure.hpp"
#include "json_transform.hpp"

namespace duckdb {

void JSONScan::AutoDetect(ClientContext &context, JSONScanData &bind_data, vector<LogicalType> &return_types,
                          vector<string> &names) {
	// Change scan type during detection
	bind_data.type = JSONScanType::SAMPLE;

	// These are used across files (if union_by_name)
	JSONStructureNode node;
	ArenaAllocator allocator(BufferAllocator::Get(context));
	Vector string_vector(LogicalType::VARCHAR);

	// Loop through the files (if union_by_name, else just sample the first file)
	idx_t remaining = bind_data.sample_size;
	for (idx_t file_idx = 0; file_idx < bind_data.files.size(); file_idx++) {
		// Create global/local state and place the reader in the right field
		JSONScanGlobalState gstate(context, bind_data);
		JSONScanLocalState lstate(context, gstate);
		if (file_idx == 0) {
			gstate.json_readers.emplace_back(bind_data.initial_reader.get());
		} else {
			gstate.json_readers.emplace_back(bind_data.union_readers[file_idx - 1].get());
		}

		// Read and detect schema
		while (remaining != 0) {
			allocator.Reset();
			auto read_count = lstate.ReadNext(gstate);
			if (read_count == 0) {
				break;
			}

			idx_t next = MinValue<idx_t>(read_count, remaining);
			for (idx_t i = 0; i < next; i++) {
				const auto &val = lstate.values[i];
				if (val) {
					JSONStructure::ExtractStructure(val, node);
				}
			}
			if (!node.ContainsVarchar()) { // Can't refine non-VARCHAR types
				continue;
			}
			node.InitializeCandidateTypes(bind_data.max_depth);
			node.RefineCandidateTypes(lstate.values, next, string_vector, allocator, bind_data.date_format_map);
			remaining -= next;
		}

		if (file_idx == 0 && lstate.total_tuple_count != 0) {
			bind_data.avg_tuple_size = lstate.total_read_size / lstate.total_tuple_count;
		}

		// Close the file and stop detection if not union_by_name
		if (bind_data.options.file_options.union_by_name) {
			// When union_by_name=true we sample sample_size per file
			remaining = bind_data.sample_size;
		} else if (remaining == 0) {
			// When union_by_name=false, we sample sample_size in total (across the first files)
			break;
		}
	}

	// Restore the scan type
	bind_data.type = JSONScanType::READ_JSON;

	// Convert structure to logical type
	auto type = JSONStructure::StructureToType(context, node, bind_data.max_depth);

	// Auto-detect record type
	if (bind_data.options.record_type == JSONRecordType::AUTO_DETECT) {
		if (type.id() == LogicalTypeId::STRUCT) {
			bind_data.options.record_type = JSONRecordType::RECORDS;
		} else {
			bind_data.options.record_type = JSONRecordType::VALUES;
		}
	}

	if (!bind_data.auto_detect) {
		return;
	}

	bind_data.transform_options.date_format_map = &bind_data.date_format_map;

	// Auto-detect columns
	if (bind_data.options.record_type == JSONRecordType::RECORDS) {
		if (type.id() == LogicalTypeId::STRUCT) {
			const auto &child_types = StructType::GetChildTypes(type);
			return_types.reserve(child_types.size());
			names.reserve(child_types.size());
			for (auto &child_type : child_types) {
				return_types.emplace_back(child_type.second);
				names.emplace_back(child_type.first);
			}
		} else {
			throw BinderException("json_read expected records, but got non-record JSON instead."
			                      "\n Try setting records='auto' or records='false'.");
		}
	} else {
		D_ASSERT(bind_data.options.record_type == JSONRecordType::VALUES);
		return_types.emplace_back(type);
		names.emplace_back("json");
	}
}

unique_ptr<FunctionData> ReadJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	// First bind default params
	auto bind_data = make_uniq<JSONScanData>();
	bind_data->Bind(context, input);

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_json \"columns\" parameter requires a struct as input.");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_json \"columns\" parameter type specification must be VARCHAR.");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			D_ASSERT(names.size() == return_types.size());
			if (names.empty()) {
				throw BinderException("read_json \"columns\" parameter needs at least one column.");
			}
			bind_data->names = names;
		} else if (loption == "auto_detect") {
			bind_data->auto_detect = BooleanValue::Get(kv.second);
		} else if (loption == "sample_size") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data->sample_size = NumericLimits<idx_t>::Maximum();
			} else if (arg > 0) {
				bind_data->sample_size = arg;
			} else {
				throw BinderException(
				    "read_json \"sample_size\" parameter must be positive, or -1 to sample the entire file.");
			}
		} else if (loption == "maximum_depth") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data->max_depth = NumericLimits<idx_t>::Maximum();
			} else {
				bind_data->max_depth = arg;
			}
		} else if (loption == "dateformat" || loption == "date_format") {
			auto format_string = StringValue::Get(kv.second);
			if (StringUtil::Lower(format_string) == "iso") {
				format_string = "%Y-%m-%d";
			}
			bind_data->date_format = format_string;

			StrpTimeFormat format;
			auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw BinderException("read_json could not parse \"dateformat\": '%s'.", error.c_str());
			}
		} else if (loption == "timestampformat" || loption == "timestamp_format") {
			auto format_string = StringValue::Get(kv.second);
			if (StringUtil::Lower(format_string) == "iso") {
				format_string = "%Y-%m-%dT%H:%M:%S.%fZ";
			}
			bind_data->timestamp_format = format_string;

			StrpTimeFormat format;
			auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw BinderException("read_json could not parse \"timestampformat\": '%s'.", error.c_str());
			}
		} else if (loption == "records") {
			auto arg = StringValue::Get(kv.second);
			if (arg == "auto") {
				bind_data->options.record_type = JSONRecordType::AUTO_DETECT;
			} else if (arg == "true") {
				bind_data->options.record_type = JSONRecordType::RECORDS;
			} else if (arg == "false") {
				bind_data->options.record_type = JSONRecordType::VALUES;
			} else {
				throw BinderException("read_json requires \"records\" to be one of ['auto', 'true', 'false'].");
			}
		}
	}

	// Specifying column names overrides auto-detect
	if (!return_types.empty()) {
		bind_data->auto_detect = false;
	}

	if (!bind_data->auto_detect) {
		// Need to specify columns if RECORDS and not auto-detecting
		if (return_types.empty()) {
			throw BinderException("read_json requires columns to be specified through the \"columns\" parameter."
			                      "\n Use read_json_auto or set auto_detect=true to automatically guess columns.");
		}
		// If we are reading VALUES, we can only have one column
		if (bind_data->options.record_type == JSONRecordType::VALUES && return_types.size() != 1) {
			throw BinderException("read_json requires a single column to be specified through the \"columns\" "
			                      "parameter when \"records\" is set to 'false'.");
		}
	}

	bind_data->InitializeFormats();

	if (bind_data->auto_detect || bind_data->options.record_type == JSONRecordType::AUTO_DETECT) {
		JSONScan::AutoDetect(context, *bind_data, return_types, names);
		bind_data->names = names;
		D_ASSERT(return_types.size() == names.size());
	}

	bind_data->reader_bind =
	    MultiFileReader::BindOptions(bind_data->options.file_options, bind_data->files, return_types, names);

	auto &transform_options = bind_data->transform_options;
	transform_options.strict_cast = !bind_data->ignore_errors;
	transform_options.error_duplicate_key = !bind_data->ignore_errors;
	transform_options.error_missing_key = false;
	transform_options.error_unknown_key = bind_data->auto_detect && !bind_data->ignore_errors;
	transform_options.delay_error = true;

	if (bind_data->auto_detect) {
		// JSON may contain columns such as "id" and "Id", which are duplicates for us due to case-insensitivity
		// We rename them so we can parse the file anyway. Note that we can't change bind_data->names,
		// because the JSON reader gets columns by exact name, not position
		case_insensitive_map_t<idx_t> name_count_map;
		for (auto &name : names) {
			auto it = name_count_map.find(name);
			if (it == name_count_map.end()) {
				name_count_map[name] = 1;
			} else {
				name = StringUtil::Format("%s_%llu", name, it->second++);
			}
		}
	}

	return std::move(bind_data);
}

static void ReadJSONFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = data_p.local_state->Cast<JSONLocalTableFunctionState>().state;

	const auto count = lstate.ReadNext(gstate);
	yyjson_val **values = lstate.values;
	output.SetCardinality(count);

	if (!gstate.names.empty()) {
		vector<Vector *> result_vectors;
		result_vectors.reserve(gstate.column_indices.size());
		for (const auto &col_idx : gstate.column_indices) {
			result_vectors.emplace_back(&output.data[col_idx]);
		}

		D_ASSERT(gstate.bind_data.options.record_type != JSONRecordType::AUTO_DETECT);
		bool success;
		if (gstate.bind_data.options.record_type == JSONRecordType::RECORDS) {
			success = JSONTransform::TransformObject(values, lstate.GetAllocator(), count, gstate.names, result_vectors,
			                                         lstate.transform_options);
		} else {
			D_ASSERT(gstate.bind_data.options.record_type == JSONRecordType::VALUES);
			success = JSONTransform::Transform(values, lstate.GetAllocator(), *result_vectors[0], count,
			                                   lstate.transform_options);
		}

		if (!success) {
			string hint =
			    gstate.bind_data.auto_detect
			        ? "\nTry increasing 'sample_size', reducing 'maximum_depth', specifying 'columns', 'format' or "
			          "'records' manually, setting 'ignore_errors' to true, or setting 'union_by_name' to true when "
			          "reading multiple files with a different structure."
			        : "\nTry setting 'auto_detect' to true, specifying 'format' or 'records' manually, or setting "
			          "'ignore_errors' to true.";
			lstate.ThrowTransformError(lstate.transform_options.object_index,
			                           lstate.transform_options.error_message + hint);
		}
	}

	if (output.size() != 0) {
		MultiFileReader::FinalizeChunk(gstate.bind_data.reader_bind, lstate.GetReaderData(), output);
	}
}

TableFunction JSONFunctions::GetReadJSONTableFunction(shared_ptr<JSONScanInfo> function_info) {
	TableFunction table_function({LogicalType::VARCHAR}, ReadJSONFunction, ReadJSONBind,
	                             JSONGlobalTableFunctionState::Init, JSONLocalTableFunctionState::Init);
	table_function.name = "read_json";

	JSONScan::TableFunctionDefaults(table_function);
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["date_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestamp_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["records"] = LogicalType::VARCHAR;

	// TODO: might be able to do filter pushdown/prune ?

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
	auto info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::ARRAY, JSONRecordType::RECORDS);
	return CreateJSONFunctionInfo("read_json", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadNDJSONFunction() {
	auto info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED, JSONRecordType::RECORDS);
	return CreateJSONFunctionInfo("read_ndjson", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadJSONAutoFunction() {
	auto info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_json_auto", std::move(info), true);
}

TableFunctionSet JSONFunctions::GetReadNDJSONAutoFunction() {
	auto info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED,
	                                      JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_ndjson_auto", std::move(info), true);
}

} // namespace duckdb
