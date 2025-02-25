#include "json_multi_file_info.hpp"
#include "json_scan.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

unique_ptr<BaseFileReaderOptions> JSONMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                       optional_ptr<TableFunctionInfo> info) {
	auto reader_options = make_uniq<JSONFileReaderOptions>();
	if (info) {
		auto &scan_info = info->Cast<JSONScanInfo>();
		reader_options->options.format = scan_info.format;
		reader_options->options.record_type = scan_info.record_type;
		reader_options->options.auto_detect = scan_info.auto_detect;
	} else {
		// COPY
		reader_options->options.record_type = JSONRecordType::RECORDS;
		reader_options->options.format = JSONFormat::NEWLINE_DELIMITED;
	}
	return std::move(reader_options);
}

bool JSONMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &value,
                                    MultiFileReaderOptions &, BaseFileReaderOptions &options_p) {
	auto &reader_options = options_p.Cast<JSONFileReaderOptions>();
	auto &options = reader_options.options;
	auto loption = StringUtil::Lower(key);
	if (loption == "ignore_errors") {
		options.ignore_errors = BooleanValue::Get(value);
		return true;
	}
	if (loption == "maximum_object_size") {
		options.maximum_object_size = MaxValue<idx_t>(UIntegerValue::Get(value), options.maximum_object_size);
		return true;
	}
	if (loption == "format") {
		auto arg = StringUtil::Lower(StringValue::Get(value));
		static const auto FORMAT_OPTIONS =
		    case_insensitive_map_t<JSONFormat> {{"auto", JSONFormat::AUTO_DETECT},
		                                        {"unstructured", JSONFormat::UNSTRUCTURED},
		                                        {"newline_delimited", JSONFormat::NEWLINE_DELIMITED},
		                                        {"nd", JSONFormat::NEWLINE_DELIMITED},
		                                        {"array", JSONFormat::ARRAY}};
		auto lookup = FORMAT_OPTIONS.find(arg);
		if (lookup == FORMAT_OPTIONS.end()) {
			vector<string> valid_options;
			for (auto &pair : FORMAT_OPTIONS) {
				valid_options.push_back(StringUtil::Format("'%s'", pair.first));
			}
			throw BinderException("format must be one of [%s], not '%s'", StringUtil::Join(valid_options, ", "), arg);
		}
		options.format = lookup->second;
		return true;
	}
	if (loption == "compression") {
		options.compression = EnumUtil::FromString<FileCompressionType>(StringUtil::Upper(StringValue::Get(value)));
		return true;
	}
	if (loption == "columns") {
		auto &child_type = value.type();
		if (child_type.id() != LogicalTypeId::STRUCT) {
			throw BinderException("read_json \"columns\" parameter requires a struct as input.");
		}
		auto &struct_children = StructValue::GetChildren(value);
		D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &name = StructType::GetChildName(child_type, i);
			auto &val = struct_children[i];
			options.name_list.push_back(name);
			if (val.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("read_json \"columns\" parameter type specification must be VARCHAR.");
			}
			options.sql_type_list.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
		}
		D_ASSERT(options.name_list.size() == options.sql_type_list.size());
		if (options.name_list.empty()) {
			throw BinderException("read_json \"columns\" parameter needs at least one column.");
		}
		return true;
	}
	if (loption == "auto_detect") {
		options.auto_detect = BooleanValue::Get(value);
		return true;
	}
	if (loption == "sample_size") {
		auto arg = BigIntValue::Get(value);
		if (arg == -1) {
			options.sample_size = NumericLimits<idx_t>::Maximum();
		} else if (arg > 0) {
			options.sample_size = arg;
		} else {
			throw BinderException("read_json \"sample_size\" parameter must be positive, or -1 to sample all input "
			                      "files entirely, up to \"maximum_sample_files\" files.");
		}
		return true;
	}
	if (loption == "maximum_depth") {
		auto arg = BigIntValue::Get(value);
		if (arg == -1) {
			options.max_depth = NumericLimits<idx_t>::Maximum();
		} else {
			options.max_depth = arg;
		}
		return true;
	}
	if (loption == "field_appearance_threshold") {
		auto arg = DoubleValue::Get(value);
		if (arg < 0 || arg > 1) {
			throw BinderException("read_json_auto \"field_appearance_threshold\" parameter must be between 0 and 1");
		}
		options.field_appearance_threshold = arg;
		return true;
	}
	if (loption == "map_inference_threshold") {
		auto arg = BigIntValue::Get(value);
		if (arg == -1) {
			options.map_inference_threshold = NumericLimits<idx_t>::Maximum();
		} else if (arg >= 0) {
			options.map_inference_threshold = arg;
		} else {
			throw BinderException("read_json_auto \"map_inference_threshold\" parameter must be 0 or positive, "
			                      "or -1 to disable map inference for consistent objects.");
		}
		return true;
	}
	if (loption == "dateformat" || loption == "date_format") {
		auto format_string = StringValue::Get(value);
		if (StringUtil::Lower(format_string) == "iso") {
			format_string = "%Y-%m-%d";
		}
		options.date_format = format_string;

		StrpTimeFormat format;
		auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
		if (!error.empty()) {
			throw BinderException("read_json could not parse \"dateformat\": '%s'.", error.c_str());
		}
		return true;
	}
	if (loption == "timestampformat" || loption == "timestamp_format") {
		auto format_string = StringValue::Get(value);
		if (StringUtil::Lower(format_string) == "iso") {
			format_string = "%Y-%m-%dT%H:%M:%S.%fZ";
		}
		options.timestamp_format = format_string;

		StrpTimeFormat format;
		auto error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
		if (!error.empty()) {
			throw BinderException("read_json could not parse \"timestampformat\": '%s'.", error.c_str());
		}
		return true;
	}
	if (loption == "records") {
		auto arg = StringValue::Get(value);
		if (arg == "auto") {
			options.record_type = JSONRecordType::AUTO_DETECT;
		} else if (arg == "true") {
			options.record_type = JSONRecordType::RECORDS;
		} else if (arg == "false") {
			options.record_type = JSONRecordType::VALUES;
		} else {
			throw BinderException("read_json requires \"records\" to be one of ['auto', 'true', 'false'].");
		}
		return true;
	}
	if (loption == "maximum_sample_files") {
		auto arg = BigIntValue::Get(value);
		if (arg == -1) {
			options.maximum_sample_files = NumericLimits<idx_t>::Maximum();
		} else if (arg > 0) {
			options.maximum_sample_files = arg;
		} else {
			throw BinderException("read_json \"maximum_sample_files\" parameter must be positive, or -1 to remove "
			                      "the limit on the number of files used to sample \"sample_size\" rows.");
		}
		return true;
	}
	if (loption == "convert_strings_to_integers") {
		options.convert_strings_to_integers = BooleanValue::Get(value);
		return true;
	}
	return false;
}

static void JSONCheckSingleParameter(const string &key, const vector<Value> &values) {
	if (values.size() == 1) {
		return;
	}
	throw BinderException("COPY (FORMAT JSON) parameter %s expects a single argument.", key);
}

bool JSONMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                        BaseFileReaderOptions &options_p, vector<string> &expected_names,
                                        vector<LogicalType> &expected_types) {
	auto &reader_options = options_p.Cast<JSONFileReaderOptions>();
	auto &options = reader_options.options;
	const auto &loption = StringUtil::Lower(key);
	if (loption == "dateformat" || loption == "date_format") {
		JSONCheckSingleParameter(key, values);
		options.date_format = StringValue::Get(values.back());
		return true;
	}
	if (loption == "timestampformat" || loption == "timestamp_format") {
		JSONCheckSingleParameter(key, values);
		options.timestamp_format = StringValue::Get(values.back());
		return true;
	}
	if (loption == "auto_detect") {
		if (values.empty()) {
			options.auto_detect = true;
		} else {
			JSONCheckSingleParameter(key, values);
			options.auto_detect = BooleanValue::Get(values.back().DefaultCastAs(LogicalTypeId::BOOLEAN));
		}
		return true;
	}
	if (loption == "compression") {
		JSONCheckSingleParameter(key, values);
		options.compression =
		    EnumUtil::FromString<FileCompressionType>(StringUtil::Upper(StringValue::Get(values.back())));
		return true;
	}
	if (loption == "array") {
		if (values.empty()) {
			options.format = JSONFormat::ARRAY;
		} else {
			JSONCheckSingleParameter(key, values);
			if (BooleanValue::Get(values.back().DefaultCastAs(LogicalTypeId::BOOLEAN))) {
				options.format = JSONFormat::ARRAY;
			}
		}
		return true;
	}
	return false;
}

unique_ptr<TableFunctionData> JSONMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                    unique_ptr<BaseFileReaderOptions> options) {
	auto &reader_options = options->Cast<JSONFileReaderOptions>();
	auto json_data = make_uniq<JSONScanData>();
	json_data->options = std::move(reader_options.options);
	return std::move(json_data);
}

void JSONMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                   MultiFileBindData &bind_data) {
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	json_data.InitializeReaders(context, bind_data);

	auto &options = json_data.options;
	names = options.name_list;
	return_types = options.sql_type_list;
	if (options.record_type == JSONRecordType::AUTO_DETECT && return_types.size() > 1) {
		// More than one specified column implies records
		options.record_type = JSONRecordType::RECORDS;
	}

	// Specifying column names overrides auto-detect
	if (!return_types.empty()) {
		options.auto_detect = false;
	}

	if (!options.auto_detect) {
		// Need to specify columns if RECORDS and not auto-detecting
		if (return_types.empty()) {
			throw BinderException("When auto_detect=false, read_json requires columns to be specified through the "
			                      "\"columns\" parameter.");
		}
		// If we are reading VALUES, we can only have one column
		if (json_data.options.record_type == JSONRecordType::VALUES && return_types.size() != 1) {
			throw BinderException("read_json requires a single column to be specified through the \"columns\" "
			                      "parameter when \"records\" is set to 'false'.");
		}
	}

	json_data.InitializeFormats();

	if (options.auto_detect || options.record_type == JSONRecordType::AUTO_DETECT) {
		JSONScan::AutoDetect(context, bind_data, return_types, names);
		D_ASSERT(return_types.size() == names.size());
	}
	json_data.key_names = names;

	bind_data.multi_file_reader->BindOptions(bind_data.file_options, *bind_data.file_list, return_types, names,
	                                         bind_data.reader_bind);

	auto &transform_options = json_data.transform_options;
	transform_options.strict_cast = !options.ignore_errors;
	transform_options.error_duplicate_key = !options.ignore_errors;
	transform_options.error_missing_key = false;
	transform_options.error_unknown_key = options.auto_detect && !options.ignore_errors;
	transform_options.delay_error = true;

	if (options.auto_detect) {
		// JSON may contain columns such as "id" and "Id", which are duplicates for us due to case-insensitivity
		// We rename them so we can parse the file anyway. Note that we can't change json_data.key_names,
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
}

void JSONMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
}

void JSONMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
                                         const vector<string> &expected_names,
                                         const vector<LogicalType> &expected_types) {
	auto &reader_options = options_p.Cast<JSONFileReaderOptions>();
	auto &options = reader_options.options;
	options.name_list = expected_names;
	options.sql_type_list = expected_types;
	if (options.auto_detect && options.format != JSONFormat::ARRAY) {
		options.format = JSONFormat::AUTO_DETECT;
	}
}

void JSONMultiFileInfo::GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data,
                                          virtual_column_map_t &result) {
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
}

} // namespace duckdb
