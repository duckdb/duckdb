#include "json_scan.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "json_multi_file_info.hpp"

namespace duckdb {

JSONScanData::JSONScanData() {
}

void JSONScanData::InitializeFormats() {
	InitializeFormats(options.auto_detect);
}

void JSONScanData::InitializeFormats(bool auto_detect_p) {
	type_id_map_t<vector<StrpTimeFormat>> candidate_formats;
	// Initialize date_format_map if anything was specified
	if (!options.date_format.empty()) {
		DateFormatMap::AddFormat(candidate_formats, LogicalTypeId::DATE, options.date_format);
	}
	if (!options.timestamp_format.empty()) {
		DateFormatMap::AddFormat(candidate_formats, LogicalTypeId::TIMESTAMP, options.timestamp_format);
	}

	if (auto_detect_p) {
		static const type_id_map_t<vector<const char *>> FORMAT_TEMPLATES = {
		    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
		    {LogicalTypeId::TIMESTAMP,
		     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
		      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
		      "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"}},
		};

		// Populate possible date/timestamp formats, assume this is consistent across columns
		for (auto &kv : FORMAT_TEMPLATES) {
			const auto &logical_type = kv.first;
			if (DateFormatMap::HasFormats(candidate_formats, logical_type)) {
				continue; // Already populated
			}
			const auto &format_strings = kv.second;
			for (auto &format_string : format_strings) {
				DateFormatMap::AddFormat(candidate_formats, logical_type, format_string);
			}
		}
	}
	date_format_map = make_uniq<DateFormatMap>(std::move(candidate_formats));
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, const JSONScanData &json_data_p,
                                         idx_t total_file_count)
    : json_data(json_data_p), transform_options(json_data.transform_options), allocator(BufferAllocator::Get(context)),
      buffer_capacity(json_data.options.maximum_object_size * 2),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()),
      enable_parallel_scans(total_file_count < system_threads) {
}

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : scan_state(context, gstate.allocator, gstate.buffer_capacity) {
}

JSONGlobalTableFunctionState::JSONGlobalTableFunctionState(ClientContext &context, const JSONScanData &json_data,
                                                           idx_t total_file_count)
    : state(context, json_data, total_file_count) {
}

JSONLocalTableFunctionState::JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate)
    : state(context, gstate) {
}

idx_t JSONScanLocalState::Read() {
	return scan_state.current_reader->Scan(scan_state);
}

void JSONScanLocalState::ParseJSON(char *const json_start, const idx_t json_size, const idx_t remaining) {
	scan_state.current_reader->ParseJSON(scan_state, json_start, json_size, remaining);
}

bool JSONScanLocalState::TryInitializeScan(JSONScanGlobalState &gstate, JSONReader &reader) {
	// try to initialize a scan in the given reader
	// three scenarios:
	// scenario 1 - unseekable file - Read from the file and setup the buffers
	// scenario 2 - seekable file - get the position from the file to read and return
	// scenario 3 - entire file readers - if we are reading an entire file at once, do not do anything here, except for
	// setting up the basics
	auto read_type = JSONFileReadType::SCAN_PARTIAL;
	if (!gstate.enable_parallel_scans || reader.GetFormat() != JSONFormat::NEWLINE_DELIMITED) {
		read_type = JSONFileReadType::SCAN_ENTIRE_FILE;
	}
	if (read_type == JSONFileReadType::SCAN_ENTIRE_FILE) {
		if (gstate.file_is_assigned) {
			return false;
		}
		gstate.file_is_assigned = true;
	}
	return reader.InitializeScan(scan_state, read_type);
}

void JSONScanLocalState::AddTransformError(idx_t object_index, const string &error_message) {
	scan_state.current_reader->AddTransformError(scan_state, object_index, error_message);
}

void JSONScan::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p, const TableFunction &) {
	throw NotImplementedException("JSONScan Serialize not implemented");
}

unique_ptr<FunctionData> JSONScan::Deserialize(Deserializer &deserializer, TableFunction &) {
	throw NotImplementedException("JSONScan Deserialize not implemented");
}

void JSONScan::TableFunctionDefaults(TableFunction &table_function) {
	table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["format"] = LogicalType::VARCHAR;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;

	table_function.serialize = Serialize;
	table_function.deserialize = Deserialize;

	table_function.projection_pushdown = true;
	table_function.filter_pushdown = false;
	table_function.filter_prune = false;
}

void JSONScan::AddReadJSONParameters(TableFunction &table_function) {
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["geojson"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["date_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestamp_format"] = LogicalType::VARCHAR;
	table_function.named_parameters["records"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_sample_files"] = LogicalType::BIGINT;
}

void JSONScan::AddAutoDetectParameters(TableFunction &table_function) {
	table_function.named_parameters["maximum_depth"] = LogicalType::BIGINT;
	table_function.named_parameters["field_appearance_threshold"] = LogicalType::DOUBLE;
	table_function.named_parameters["convert_strings_to_integers"] = LogicalType::BOOLEAN;
	table_function.named_parameters["map_inference_threshold"] = LogicalType::BIGINT;
}

} // namespace duckdb
