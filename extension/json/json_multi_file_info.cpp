#include "json_multi_file_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

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

void JSONMultiFileInfo::GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data,
                                          virtual_column_map_t &result) {
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
}

} // namespace duckdb
