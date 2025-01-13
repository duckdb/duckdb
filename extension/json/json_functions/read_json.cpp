#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_structure.hpp"
#include "json_transform.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

static inline LogicalType RemoveDuplicateStructKeys(const LogicalType &type, const bool ignore_errors) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		case_insensitive_set_t child_names;
		child_list_t<LogicalType> child_types;
		for (auto &child_type : StructType::GetChildTypes(type)) {
			auto insert_success = child_names.insert(child_type.first).second;
			if (!insert_success) {
				if (ignore_errors) {
					continue;
				}
				throw NotImplementedException(
				    "Duplicate name \"%s\" in struct auto-detected in JSON, try ignore_errors=true", child_type.first);
			} else {
				child_types.emplace_back(child_type.first, RemoveDuplicateStructKeys(child_type.second, ignore_errors));
			}
		}
		return LogicalType::STRUCT(child_types);
	}
	case LogicalTypeId::MAP:
		return LogicalType::MAP(RemoveDuplicateStructKeys(MapType::KeyType(type), ignore_errors),
		                        RemoveDuplicateStructKeys(MapType::ValueType(type), ignore_errors));
	case LogicalTypeId::LIST:
		return LogicalType::LIST(RemoveDuplicateStructKeys(ListType::GetChildType(type), ignore_errors));
	default:
		return type;
	}
}

class JSONSchemaTask : public BaseExecutorTask {
public:
	JSONSchemaTask(TaskExecutor &executor, ClientContext &context_p, JSONScanData &bind_data_p,
	               JSONStructureNode &node_p, const idx_t file_idx_start_p, const idx_t file_idx_end_p)
	    : BaseExecutorTask(executor), context(context_p), bind_data(bind_data_p), node(node_p),
	      file_idx_start(file_idx_start_p), file_idx_end(file_idx_end_p), allocator(BufferAllocator::Get(context)),
	      string_vector(LogicalType::VARCHAR) {
	}

	static idx_t ExecuteInternal(ClientContext &context, JSONScanData &bind_data, JSONStructureNode &node,
	                             const idx_t file_idx, ArenaAllocator &allocator, Vector &string_vector,
	                             idx_t remaining) {
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
			const auto read_count = lstate.ReadNext(gstate);
			if (read_count == 0) {
				break;
			}

			const auto next = MinValue<idx_t>(read_count, remaining);
			for (idx_t i = 0; i < next; i++) {
				const auto &val = lstate.values[i];
				if (val) {
					JSONStructure::ExtractStructure(val, node, true);
				}
			}
			if (!node.ContainsVarchar()) { // Can't refine non-VARCHAR types
				continue;
			}
			node.InitializeCandidateTypes(bind_data.max_depth, bind_data.convert_strings_to_integers);
			node.RefineCandidateTypes(lstate.values, next, string_vector, allocator, bind_data.date_format_map);
			remaining -= next;
		}

		if (file_idx == 0 && lstate.total_tuple_count != 0) {
			bind_data.avg_tuple_size = lstate.total_read_size / lstate.total_tuple_count;
		}

		return remaining;
	}

	void ExecuteTask() override {
		for (idx_t file_idx = file_idx_start; file_idx < file_idx_end; file_idx++) {
			ExecuteInternal(context, bind_data, node, file_idx, allocator, string_vector, bind_data.sample_size);
		}
	}

private:
	ClientContext &context;
	JSONScanData &bind_data;
	JSONStructureNode &node;
	const idx_t file_idx_start;
	const idx_t file_idx_end;

	ArenaAllocator allocator;
	Vector string_vector;
};

void JSONScan::AutoDetect(ClientContext &context, JSONScanData &bind_data, vector<LogicalType> &return_types,
                          vector<string> &names) {
	// Change scan type during detection
	bind_data.type = JSONScanType::SAMPLE;

	JSONStructureNode node;
	if (bind_data.options.file_options.union_by_name) {
		const auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		const auto files_per_task = (bind_data.files.size() + num_threads - 1) / num_threads;
		const auto num_tasks = bind_data.files.size() / files_per_task;
		vector<JSONStructureNode> task_nodes(num_tasks);

		// Same idea as in union_by_name.hpp
		TaskExecutor executor(context);
		for (idx_t task_idx = 0; task_idx < num_tasks; task_idx++) {
			const auto file_idx_start = task_idx * files_per_task;
			auto task = make_uniq<JSONSchemaTask>(executor, context, bind_data, task_nodes[task_idx], file_idx_start,
			                                      file_idx_start + files_per_task);
			executor.ScheduleTask(std::move(task));
		}
		executor.WorkOnTasks();

		// Merge task nodes into one
		for (auto &task_node : task_nodes) {
			JSONStructure::MergeNodes(node, task_node);
		}
	} else {
		ArenaAllocator allocator(BufferAllocator::Get(context));
		Vector string_vector(LogicalType::VARCHAR);
		idx_t remaining = bind_data.sample_size;
		for (idx_t file_idx = 0; file_idx < bind_data.files.size(); file_idx++) {
			remaining = JSONSchemaTask::ExecuteInternal(context, bind_data, node, file_idx, allocator, string_vector,
			                                            remaining);
			if (remaining == 0 || file_idx == bind_data.maximum_sample_files - 1) {
				break; // We sample sample_size in total (across the first maximum_sample_files files)
			}
		}
	}

	// Restore the scan type
	bind_data.type = JSONScanType::READ_JSON;

	// Convert structure to logical type
	auto type = JSONStructure::StructureToType(context, node, bind_data.max_depth, bind_data.field_appearance_threshold,
	                                           bind_data.map_inference_threshold);

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
				return_types.emplace_back(RemoveDuplicateStructKeys(child_type.second, bind_data.ignore_errors));
				names.emplace_back(child_type.first);
			}
		} else {
			throw BinderException("json_read expected records, but got non-record JSON instead."
			                      "\n Try setting records='auto' or records='false'.");
		}
	} else {
		D_ASSERT(bind_data.options.record_type == JSONRecordType::VALUES);
		return_types.emplace_back(RemoveDuplicateStructKeys(type, bind_data.ignore_errors));
		names.emplace_back("json");
	}
}

unique_ptr<FunctionData> ReadJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	// First bind default params
	auto bind_data = make_uniq<JSONScanData>();
	bind_data->Bind(context, input);

	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("Cannot use NULL as function argument");
		}
		auto loption = StringUtil::Lower(kv.first);
		if (kv.second.IsNull()) {
			throw BinderException("read_json parameter \"%s\" cannot be NULL.", loption);
		}
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
				throw BinderException("read_json \"sample_size\" parameter must be positive, or -1 to sample all input "
				                      "files entirely, up to \"maximum_sample_files\" files.");
			}
		} else if (loption == "maximum_depth") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data->max_depth = NumericLimits<idx_t>::Maximum();
			} else {
				bind_data->max_depth = arg;
			}
		} else if (loption == "field_appearance_threshold") {
			auto arg = DoubleValue::Get(kv.second);
			if (arg < 0 || arg > 1) {
				throw BinderException(
				    "read_json_auto \"field_appearance_threshold\" parameter must be between 0 and 1");
			}
			bind_data->field_appearance_threshold = arg;
		} else if (loption == "map_inference_threshold") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data->map_inference_threshold = NumericLimits<idx_t>::Maximum();
			} else if (arg >= 0) {
				bind_data->map_inference_threshold = arg;
			} else {
				throw BinderException("read_json_auto \"map_inference_threshold\" parameter must be 0 or positive, "
				                      "or -1 to disable map inference for consistent objects.");
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
		} else if (loption == "maximum_sample_files") {
			auto arg = BigIntValue::Get(kv.second);
			if (arg == -1) {
				bind_data->maximum_sample_files = NumericLimits<idx_t>::Maximum();
			} else if (arg > 0) {
				bind_data->maximum_sample_files = arg;
			} else {
				throw BinderException("read_json \"maximum_sample_files\" parameter must be positive, or -1 to remove "
				                      "the limit on the number of files used to sample \"sample_size\" rows.");
			}
		} else if (loption == "convert_strings_to_integers") {
			bind_data->convert_strings_to_integers = BooleanValue::Get(kv.second);
		}
	}

	if (bind_data->options.record_type == JSONRecordType::AUTO_DETECT && return_types.size() > 1) {
		// More than one specified column implies records
		bind_data->options.record_type = JSONRecordType::RECORDS;
	}

	// Specifying column names overrides auto-detect
	if (!return_types.empty()) {
		bind_data->auto_detect = false;
	}

	if (!bind_data->auto_detect) {
		// Need to specify columns if RECORDS and not auto-detecting
		if (return_types.empty()) {
			throw BinderException("When auto_detect=false, read_json requires columns to be specified through the "
			                      "\"columns\" parameter.");
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

	SimpleMultiFileList file_list(std::move(bind_data->files));
	MultiFileReader().BindOptions(bind_data->options.file_options, file_list, return_types, names,
	                              bind_data->reader_bind);
	bind_data->files = file_list.GetAllFiles();

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
		case_insensitive_map_t<idx_t> name_collision_count;
		for (auto &col_name : names) {
			// Taken from CSV header_detection.cpp
			while (name_collision_count.find(col_name) != name_collision_count.end()) {
				name_collision_count[col_name] += 1;
				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
			}
			name_collision_count[col_name] = 0;
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
		result_vectors.reserve(gstate.column_ids.size());
		for (const auto &col_idx : gstate.column_ids) {
			result_vectors.emplace_back(&output.data[col_idx]);
		}

		D_ASSERT(gstate.bind_data.options.record_type != JSONRecordType::AUTO_DETECT);
		bool success;
		if (gstate.bind_data.options.record_type == JSONRecordType::RECORDS) {
			success = JSONTransform::TransformObject(values, lstate.GetAllocator(), count, gstate.names, result_vectors,
			                                         lstate.transform_options, gstate.column_indices,
			                                         lstate.transform_options.error_unknown_key);
		} else {
			D_ASSERT(gstate.bind_data.options.record_type == JSONRecordType::VALUES);
			success = JSONTransform::Transform(values, lstate.GetAllocator(), *result_vectors[0], count,
			                                   lstate.transform_options, gstate.column_indices[0]);
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
		MultiFileReader().FinalizeChunk(context, gstate.bind_data.reader_bind, lstate.GetReaderData(), output, nullptr);
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
	table_function.named_parameters["maximum_sample_files"] = LogicalType::BIGINT;

	// TODO: might be able to do filter pushdown/prune ?

	table_function.function_info = std::move(function_info);

	return table_function;
}

TableFunctionSet CreateJSONFunctionInfo(string name, shared_ptr<JSONScanInfo> info) {
	auto table_function = JSONFunctions::GetReadJSONTableFunction(std::move(info));
	table_function.name = std::move(name);
	table_function.named_parameters["maximum_depth"] = LogicalType::BIGINT;
	table_function.named_parameters["field_appearance_threshold"] = LogicalType::DOUBLE;
	table_function.named_parameters["convert_strings_to_integers"] = LogicalType::BOOLEAN;
	table_function.named_parameters["map_inference_threshold"] = LogicalType::BIGINT;
	return MultiFileReader::CreateFunctionSet(table_function);
}

TableFunctionSet JSONFunctions::GetReadJSONFunction() {
	auto info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT,
	                                          JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_json", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadNDJSONFunction() {
	auto info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED,
	                                          JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_ndjson", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadJSONAutoFunction() {
	auto info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT,
	                                          JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_json_auto", std::move(info));
}

TableFunctionSet JSONFunctions::GetReadNDJSONAutoFunction() {
	auto info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED,
	                                          JSONRecordType::AUTO_DETECT, true);
	return CreateJSONFunctionInfo("read_ndjson_auto", std::move(info));
}

} // namespace duckdb
