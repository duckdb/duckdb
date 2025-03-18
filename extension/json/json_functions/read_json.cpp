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

struct AutoDetectState {
	AutoDetectState(ClientContext &context_p, MultiFileBindData &bind_data_p, const vector<string> &files,
	                MutableDateFormatMap &date_format_map)
	    : context(context_p), bind_data(bind_data_p), files(files), date_format_map(date_format_map), files_scanned(0),
	      tuples_scanned(0), bytes_scanned(0), total_file_size(0) {
	}

	ClientContext &context;
	MultiFileBindData &bind_data;
	const vector<string> &files;
	MutableDateFormatMap &date_format_map;
	atomic<idx_t> files_scanned;
	atomic<idx_t> tuples_scanned;
	atomic<idx_t> bytes_scanned;
	atomic<idx_t> total_file_size;
};

class JSONSchemaTask : public BaseExecutorTask {
public:
	JSONSchemaTask(TaskExecutor &executor, AutoDetectState &auto_detect_state, JSONStructureNode &node_p,
	               const idx_t file_idx_start_p, const idx_t file_idx_end_p)
	    : BaseExecutorTask(executor), auto_detect_state(auto_detect_state), node(node_p),
	      file_idx_start(file_idx_start_p), file_idx_end(file_idx_end_p),
	      allocator(BufferAllocator::Get(auto_detect_state.context)), string_vector(LogicalType::VARCHAR) {
	}

	static idx_t ExecuteInternal(AutoDetectState &auto_detect_state, JSONStructureNode &node, const idx_t file_idx,
	                             ArenaAllocator &allocator, Vector &string_vector, idx_t remaining) {
		auto &context = auto_detect_state.context;
		auto &bind_data = auto_detect_state.bind_data;
		auto &files = auto_detect_state.files;
		auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
		auto json_reader = make_shared_ptr<JSONReader>(context, json_data.options, files[file_idx]);
		if (bind_data.union_readers[file_idx]) {
			throw InternalException("Union data already set");
		}
		auto &reader = *json_reader;
		auto union_data = make_uniq<BaseUnionData>(files[file_idx]);
		union_data->reader = std::move(json_reader);
		bind_data.union_readers[file_idx] = std::move(union_data);

		auto &global_allocator = Allocator::Get(context);
		idx_t buffer_capacity = json_data.options.maximum_object_size * 2;
		JSONReaderScanState scan_state(context, global_allocator, buffer_capacity);
		auto &options = json_data.options;
		// Read and detect schema
		idx_t total_tuple_count = 0;
		idx_t total_read_size = 0;

		reader.Initialize(global_allocator, buffer_capacity);
		reader.InitializeScan(scan_state, JSONFileReadType::SCAN_ENTIRE_FILE);

		auto file_size = reader.GetFileHandle().GetHandle().GetFileSize();
		while (remaining != 0) {
			allocator.Reset();
			auto buffer_offset_before = scan_state.buffer_offset;
			auto read_count = reader.Scan(scan_state);
			if (read_count == 0) {
				break;
			}
			total_read_size += scan_state.buffer_offset - buffer_offset_before;
			total_tuple_count += read_count;

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
			node.RefineCandidateTypes(scan_state.values, next, string_vector, allocator,
			                          auto_detect_state.date_format_map);
			remaining -= next;
		}
		auto_detect_state.total_file_size += file_size;
		auto_detect_state.bytes_scanned += total_read_size;
		auto_detect_state.tuples_scanned += total_tuple_count;
		++auto_detect_state.files_scanned;

		return remaining;
	}

	void ExecuteTask() override {
		auto &json_data = auto_detect_state.bind_data.bind_data->Cast<JSONScanData>();
		auto &options = json_data.options;
		for (idx_t file_idx = file_idx_start; file_idx < file_idx_end; file_idx++) {
			ExecuteInternal(auto_detect_state, node, file_idx, allocator, string_vector, options.sample_size);
		}
	}

private:
	AutoDetectState &auto_detect_state;
	JSONStructureNode &node;
	const idx_t file_idx_start;
	const idx_t file_idx_end;

	ArenaAllocator allocator;
	Vector string_vector;
};

void JSONScan::AutoDetect(ClientContext &context, MultiFileBindData &bind_data, vector<LogicalType> &return_types,
                          vector<string> &names) {
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();

	MutableDateFormatMap date_format_map(*json_data.date_format_map);
	JSONStructureNode node;
	auto &options = json_data.options;
	auto files = bind_data.file_list->GetAllFiles();
	auto file_count = files.size();
	bind_data.union_readers.resize(files.empty() ? 0 : files.size());

	AutoDetectState auto_detect_state(context, bind_data, files, date_format_map);
	if (bind_data.file_options.union_by_name) {
		const auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		const auto files_per_task = (file_count + num_threads - 1) / num_threads;
		const auto num_tasks = file_count / files_per_task;
		vector<JSONStructureNode> task_nodes(num_tasks);

		// Same idea as in union_by_name.hpp
		TaskExecutor executor(context);
		for (idx_t task_idx = 0; task_idx < num_tasks; task_idx++) {
			const auto file_idx_start = task_idx * files_per_task;
			auto task = make_uniq<JSONSchemaTask>(executor, auto_detect_state, task_nodes[task_idx], file_idx_start,
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
		idx_t remaining = options.sample_size;
		for (idx_t file_idx = 0; file_idx < file_count; file_idx++) {
			remaining =
			    JSONSchemaTask::ExecuteInternal(auto_detect_state, node, file_idx, allocator, string_vector, remaining);
			if (remaining == 0 || file_idx == options.maximum_sample_files - 1) {
				break; // We sample sample_size in total (across the first maximum_sample_files files)
			}
		}
	}
	// set the max threads/estimated per-file cardinality
	if (auto_detect_state.files_scanned > 0 && auto_detect_state.tuples_scanned > 0) {
		auto average_tuple_size = auto_detect_state.bytes_scanned / auto_detect_state.tuples_scanned;
		json_data.estimated_cardinality_per_file = auto_detect_state.total_file_size / average_tuple_size;
		if (auto_detect_state.files_scanned == 1) {
			json_data.max_threads =
			    MaxValue<idx_t>(auto_detect_state.total_file_size / json_data.options.maximum_object_size, 1);
		}
	}

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

TableFunction JSONFunctions::GetReadJSONTableFunction(shared_ptr<JSONScanInfo> function_info) {
	MultiFileReaderFunction<JSONMultiFileInfo> table_function("read_json");

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

	return static_cast<TableFunction>(table_function);
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
