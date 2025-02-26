#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_structure.hpp"
#include "json_transform.hpp"
#include "json_multi_file_info.hpp"
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
	JSONSchemaTask(TaskExecutor &executor, ClientContext &context_p, MultiFileBindData &bind_data_p,
	               MutableDateFormatMap &date_format_map, JSONStructureNode &node_p, const idx_t file_idx_start_p,
	               const idx_t file_idx_end_p)
	    : BaseExecutorTask(executor), context(context_p), bind_data(bind_data_p), date_format_map(date_format_map),
	      node(node_p), file_idx_start(file_idx_start_p), file_idx_end(file_idx_end_p),
	      allocator(BufferAllocator::Get(context)), string_vector(LogicalType::VARCHAR) {
	}

	static idx_t ExecuteInternal(ClientContext &context, MultiFileBindData &bind_data, JSONStructureNode &node,
	                             const idx_t file_idx, ArenaAllocator &allocator, Vector &string_vector,
	                             idx_t remaining, MutableDateFormatMap &date_format_map) {
		JSONScanGlobalState gstate(context, bind_data);
		JSONScanLocalState lstate(context, gstate);
		optional_ptr<BufferedJSONReader> reader;
		if (bind_data.union_readers[file_idx]) {
			reader = &bind_data.union_readers[file_idx]->reader->Cast<BufferedJSONReader>();
		}
		gstate.json_readers.emplace_back(reader.get());

		auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
		auto &options = json_data.options;
		// Read and detect schema
		while (remaining != 0) {
			allocator.Reset();
			const auto read_count = lstate.ReadNext(gstate);
			if (read_count == 0) {
				break;
			}

			auto &scan_state = lstate.GetScanState();
			const auto next = MinValue<idx_t>(read_count, remaining);
			for (idx_t i = 0; i < next; i++) {
				const auto &val = scan_state.values[i];
				if (val) {
					JSONStructure::ExtractStructure(val, node, true);
				}
			}
			if (!node.ContainsVarchar()) { // Can't refine non-VARCHAR types
				continue;
			}
			node.InitializeCandidateTypes(options.max_depth, options.convert_strings_to_integers);
			node.RefineCandidateTypes(scan_state.values, next, string_vector, allocator, date_format_map);
			remaining -= next;
		}

		if (file_idx == 0 && lstate.total_tuple_count != 0) {
			json_data.avg_tuple_size = lstate.total_read_size / lstate.total_tuple_count;
		}

		return remaining;
	}

	void ExecuteTask() override {
		auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
		auto &options = json_data.options;
		for (idx_t file_idx = file_idx_start; file_idx < file_idx_end; file_idx++) {
			ExecuteInternal(context, bind_data, node, file_idx, allocator, string_vector, options.sample_size,
			                date_format_map);
		}
	}

private:
	ClientContext &context;
	MultiFileBindData &bind_data;
	MutableDateFormatMap &date_format_map;
	JSONStructureNode &node;
	const idx_t file_idx_start;
	const idx_t file_idx_end;

	ArenaAllocator allocator;
	Vector string_vector;
};

void JSONScan::AutoDetect(ClientContext &context, MultiFileBindData &bind_data, vector<LogicalType> &return_types,
                          vector<string> &names) {
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	// Change scan type during detection
	D_ASSERT(json_data.options.type == JSONScanType::READ_JSON);
	json_data.options.type = JSONScanType::SAMPLE;

	MutableDateFormatMap date_format_map(*json_data.date_format_map);
	JSONStructureNode node;
	auto &options = json_data.options;
	auto file_count = bind_data.file_list->GetTotalFileCount();
	if (bind_data.file_options.union_by_name) {
		const auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		const auto files_per_task = (file_count + num_threads - 1) / num_threads;
		const auto num_tasks = file_count / files_per_task;
		vector<JSONStructureNode> task_nodes(num_tasks);

		// Same idea as in union_by_name.hpp
		TaskExecutor executor(context);
		for (idx_t task_idx = 0; task_idx < num_tasks; task_idx++) {
			const auto file_idx_start = task_idx * files_per_task;
			auto task = make_uniq<JSONSchemaTask>(executor, context, bind_data, date_format_map, task_nodes[task_idx],
			                                      file_idx_start, file_idx_start + files_per_task);
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
		idx_t remaining = options.sample_size;
		for (idx_t file_idx = 0; file_idx < file_count; file_idx++) {
			remaining = JSONSchemaTask::ExecuteInternal(context, bind_data, node, file_idx, allocator, string_vector,
			                                            remaining, date_format_map);
			if (remaining == 0 || file_idx == options.maximum_sample_files - 1) {
				break; // We sample sample_size in total (across the first maximum_sample_files files)
			}
		}
	}

	// Restore the scan type
	json_data.options.type = JSONScanType::READ_JSON;

	// Convert structure to logical type
	auto type = JSONStructure::StructureToType(context, node, options.max_depth, options.field_appearance_threshold,
	                                           options.map_inference_threshold);

	// Auto-detect record type
	if (json_data.options.record_type == JSONRecordType::AUTO_DETECT) {
		if (type.id() == LogicalTypeId::STRUCT) {
			json_data.options.record_type = JSONRecordType::RECORDS;
		} else {
			json_data.options.record_type = JSONRecordType::VALUES;
		}
	}

	if (!names.empty()) {
		// COPY - we already have names/types
		return;
	}

	// Auto-detect columns
	if (json_data.options.record_type == JSONRecordType::RECORDS) {
		if (type.id() == LogicalTypeId::STRUCT) {
			const auto &child_types = StructType::GetChildTypes(type);
			return_types.reserve(child_types.size());
			names.reserve(child_types.size());
			for (auto &child_type : child_types) {
				return_types.emplace_back(RemoveDuplicateStructKeys(child_type.second, options.ignore_errors));
				names.emplace_back(child_type.first);
			}
		} else {
			throw BinderException("json_read expected records, but got non-record JSON instead."
			                      "\n Try setting records='auto' or records='false'.");
		}
	} else {
		D_ASSERT(json_data.options.record_type == JSONRecordType::VALUES);
		return_types.emplace_back(RemoveDuplicateStructKeys(type, options.ignore_errors));
		names.emplace_back("json");
	}
}

static void ReadJSONFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = data_p.local_state->Cast<JSONLocalTableFunctionState>().state;

	const auto count = lstate.ReadNext(gstate);
	auto &scan_state = lstate.GetScanState();
	yyjson_val **values = scan_state.values;
	output.SetCardinality(count);

	if (!gstate.names.empty()) {
		vector<Vector *> result_vectors;
		result_vectors.reserve(gstate.column_ids.size());
		for (const auto &col_idx : gstate.column_ids) {
			result_vectors.emplace_back(&output.data[col_idx]);
		}

		D_ASSERT(gstate.json_data.options.record_type != JSONRecordType::AUTO_DETECT);
		bool success;
		if (gstate.json_data.options.record_type == JSONRecordType::RECORDS) {
			success = JSONTransform::TransformObject(values, scan_state.allocator.GetYYAlc(), count, gstate.names,
			                                         result_vectors, lstate.transform_options, gstate.column_indices,
			                                         lstate.transform_options.error_unknown_key);
		} else {
			D_ASSERT(gstate.json_data.options.record_type == JSONRecordType::VALUES);
			success = JSONTransform::Transform(values, scan_state.allocator.GetYYAlc(), *result_vectors[0], count,
			                                   lstate.transform_options, gstate.column_indices[0]);
		}

		if (!success) {
			string hint =
			    gstate.json_data.options.auto_detect
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
	TableFunction table_function({LogicalType::VARCHAR}, ReadJSONFunction,
	                             MultiFileReaderFunction<JSONMultiFileInfo>::MultiFileBind,
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
