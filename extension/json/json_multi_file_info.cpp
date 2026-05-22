#include "json_multi_file_info.hpp"
#include "json_common.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {

namespace {

// pk-lookup state for read_json -- single file, random access by byte offset.
struct JSONLookupGlobalState : public GlobalTableFunctionState {
	JSONLookupGlobalState(ClientContext &context, Allocator &allocator_p, idx_t buffer_capacity)
	    : scan_state(context, allocator_p, buffer_capacity) {
	}

	shared_ptr<JSONReader> reader;
	JSONReaderScanState scan_state;
	JSONTransformOptions transform_options;
	vector<string> names;
	vector<column_t> column_ids;
	vector<ColumnIndex> column_indices;
	//! INVALID_INDEX when file_row_number is not projected.
	idx_t file_row_number_idx = DConstants::INVALID_INDEX;
	JSONRecordType record_type = JSONRecordType::AUTO_DETECT;
};

unique_ptr<GlobalTableFunctionState> JSONLookupInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData>();
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	auto &allocator = BufferAllocator::Get(context);
	D_ASSERT(json_data.options.type == JSONScanType::READ_JSON);
	const idx_t buffer_capacity = json_data.options.maximum_object_size * 2 + YYJSON_PADDING_SIZE;

	auto state = make_uniq<JSONLookupGlobalState>(context, allocator, buffer_capacity);
	state->transform_options = json_data.transform_options;
	state->record_type = json_data.options.record_type;

	const auto &file = bind_data.file_list->GetFirstFile();
	state->reader = make_shared_ptr<JSONReader>(context, json_data.options, file);
	state->reader->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(bind_data.names, bind_data.types);
	state->reader->OpenJSONFile();

	for (idx_t col_idx = 0; col_idx < input.column_indexes.size(); col_idx++) {
		auto &column_index = input.column_indexes[col_idx];
		const auto col_id = column_index.GetPrimaryIndex();
		if (bind_data.reader_bind.filename_idx.IsValid() && col_id == bind_data.reader_bind.filename_idx.GetIndex()) {
			continue;
		}
		if (IsVirtualColumn(col_id)) {
			if (col_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				state->file_row_number_idx = col_idx;
			}
			continue;
		}
		bool skip = false;
		for (const auto &hp : bind_data.reader_bind.hive_partitioning_indexes) {
			if (col_id == hp.index) {
				skip = true;
				break;
			}
		}
		if (skip) {
			continue;
		}
		state->names.push_back(json_data.key_names[col_id]);
		state->column_ids.push_back(col_idx);
		state->column_indices.push_back(column_index);
	}
	if (state->names.size() < json_data.key_names.size() || bind_data.file_options.union_by_name) {
		state->transform_options.error_unknown_key = false;
	}
	return std::move(state);
}

struct JSONObjectsLookupGlobalState : public GlobalTableFunctionState {
	JSONObjectsLookupGlobalState(ClientContext &context, Allocator &allocator, idx_t buffer_capacity)
	    : scan_state(context, allocator, buffer_capacity) {
	}

	shared_ptr<JSONReader> reader;
	JSONReaderScanState scan_state;
	idx_t json_col_idx = DConstants::INVALID_INDEX;
	idx_t file_row_number_idx = DConstants::INVALID_INDEX;
};

unique_ptr<GlobalTableFunctionState> JSONObjectsLookupInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MultiFileBindData>();
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	auto &allocator = BufferAllocator::Get(context);
	D_ASSERT(json_data.options.type == JSONScanType::READ_JSON_OBJECTS);

	const idx_t buffer_capacity = json_data.options.maximum_object_size * 2 + YYJSON_PADDING_SIZE;
	auto state = make_uniq<JSONObjectsLookupGlobalState>(context, allocator, buffer_capacity);

	const auto &file = bind_data.file_list->GetFirstFile();
	state->reader = make_shared_ptr<JSONReader>(context, json_data.options, file);
	state->reader->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(bind_data.names, bind_data.types);
	state->reader->OpenJSONFile();

	for (idx_t col_idx = 0; col_idx < input.column_indexes.size(); ++col_idx) {
		const auto col_id = input.column_indexes[col_idx].GetPrimaryIndex();
		if (IsVirtualColumn(col_id)) {
			if (col_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				state->file_row_number_idx = col_idx;
			}
			continue;
		}
		if (state->json_col_idx == DConstants::INVALID_INDEX) {
			state->json_col_idx = col_idx;
		}
	}
	return std::move(state);
}

void JSONObjectsLookupScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<JSONObjectsLookupGlobalState>();
	if (data.pk_lookups.empty()) {
		return;
	}
	D_ASSERT(data.pk_lookups.size() <= STANDARD_VECTOR_SIZE);
	D_ASSERT(data.pk_output_positions.size() == data.pk_lookups.size());

	const idx_t count = data.pk_lookups.size();
	auto &reader = *gstate.reader;
	auto &scan_state = gstate.scan_state;
	// The prior batch's output has been drained downstream; release the
	// buffers backing its string_t descriptors before starting a new batch.
	reader.ClearLookupBuffers(scan_state);
	scan_state.allocator.Reset();

	string_t *json_strings = nullptr;
	ValidityMask *json_validity = nullptr;
	if (gstate.json_col_idx != DConstants::INVALID_INDEX) {
		auto &out_vec = output.data[gstate.json_col_idx];
		json_strings = FlatVector::GetDataMutable<string_t>(out_vec);
		json_validity = &FlatVector::ValidityMutable(out_vec);
	}
	int64_t *frn_data = nullptr;
	ValidityMask *frn_validity = nullptr;
	if (gstate.file_row_number_idx != DConstants::INVALID_INDEX) {
		auto &frn_vec = output.data[gstate.file_row_number_idx];
		frn_data = FlatVector::GetDataMutable<int64_t>(frn_vec);
		frn_validity = &FlatVector::ValidityMutable(frn_vec);
	}

	for (idx_t pk_idx = 0; pk_idx < count; ++pk_idx) {
		const idx_t row = data.pk_output_positions[pk_idx];
		const auto pk = data.pk_lookups[pk_idx];
		if (reader.FetchRow(scan_state, UnsafeNumericCast<idx_t>(pk))) {
			if (json_strings) {
				json_strings[row] = string_t(scan_state.units[0].pointer, scan_state.units[0].size);
			}
			if (frn_data) {
				frn_data[row] = pk;
			}
		} else {
			if (json_validity) {
				json_validity->SetInvalid(row);
			}
			if (frn_validity) {
				frn_validity->SetInvalid(row);
			}
		}
	}
}

// Per-pk Transform (count=1) rather than one batched Transform: in glob mode
// other files have already written rows we don't own, and a batched call
// would have to pass vals[i] = nullptr for those, clobbering them with NULLs.
void JSONLookupScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<JSONLookupGlobalState>();
	if (data.pk_lookups.empty()) {
		return;
	}
	D_ASSERT(data.pk_lookups.size() <= STANDARD_VECTOR_SIZE);
	D_ASSERT(data.pk_output_positions.size() == data.pk_lookups.size());
	D_ASSERT(gstate.record_type != JSONRecordType::AUTO_DETECT);

	auto &reader = *gstate.reader;
	auto &scan_state = gstate.scan_state;
	// Prior batch's output drained downstream; release its buffers + yyjson nodes.
	reader.ClearLookupBuffers(scan_state);
	scan_state.allocator.Reset();
	auto *alc = scan_state.allocator.GetYYAlc();

	const idx_t ncols = gstate.column_ids.size();
	vector<Vector> slices;
	slices.reserve(ncols);
	for (idx_t c = 0; c < ncols; ++c) {
		slices.emplace_back(output.data[gstate.column_ids[c]].GetType());
	}
	vector<Vector *> result_vectors(ncols);
	for (idx_t c = 0; c < ncols; ++c) {
		result_vectors[c] = &slices[c];
	}

	int64_t *frn_data = nullptr;
	ValidityMask *frn_validity = nullptr;
	if (gstate.file_row_number_idx != DConstants::INVALID_INDEX) {
		auto &frn_vec = output.data[gstate.file_row_number_idx];
		frn_data = FlatVector::GetDataMutable<int64_t>(frn_vec);
		frn_validity = &FlatVector::ValidityMutable(frn_vec);
	}

	const idx_t count = data.pk_lookups.size();
	for (idx_t pk_idx = 0; pk_idx < count; ++pk_idx) {
		const idx_t row = data.pk_output_positions[pk_idx];
		const auto pk = data.pk_lookups[pk_idx];

		yyjson_val *vals[1];
		if (reader.FetchRow(scan_state, UnsafeNumericCast<idx_t>(pk))) {
			vals[0] = scan_state.values[0];
			if (frn_data) {
				frn_data[row] = pk;
			}
		} else {
			// nullptr -> Transform SetInvalid at the sliced row.
			vals[0] = nullptr;
			if (frn_validity) {
				frn_validity->SetInvalid(row);
			}
		}

		if (ncols == 0) {
			continue;
		}
		for (idx_t c = 0; c < ncols; ++c) {
			slices[c].Slice(output.data[gstate.column_ids[c]], row, row + 1);
		}

		if (gstate.record_type == JSONRecordType::RECORDS) {
			JSONTransform::TransformObject(vals, alc, 1, gstate.names, result_vectors, gstate.transform_options,
			                               gstate.column_indices, gstate.transform_options.error_unknown_key);
		} else {
			D_ASSERT(gstate.record_type == JSONRecordType::VALUES);
			D_ASSERT(ncols == 1);
			optional_ptr<const ColumnIndex> column_index =
			    gstate.column_indices.empty() ? nullptr : &gstate.column_indices[0];
			JSONTransform::Transform(vals, alc, *result_vectors[0], 1, gstate.transform_options, column_index);
		}
		for (idx_t c = 0; c < ncols; ++c) {
			if (!FlatVector::Validity(slices[c]).RowIsValid(0)) {
				FlatVector::SetNull(output.data[gstate.column_ids[c]], row, true);
			}
		}
	}
}

} // namespace

TableFunction MakeJSONObjectsLookupTableFunction() {
	TableFunction fn;
	fn.init_global = JSONObjectsLookupInitGlobal;
	fn.function = JSONObjectsLookupScan;
	return fn;
}

TableFunction MakeJSONLookupTableFunction() {
	TableFunction fn;
	fn.init_global = JSONLookupInitGlobal;
	fn.function = JSONLookupScan;
	return fn;
}

unique_ptr<MultiFileReaderInterface> JSONMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<JSONMultiFileInfo>();
}

void JSONMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
	// file_row_number = byte offset of the record start; usable as an exact-seek key.
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
}

unique_ptr<BaseFileReaderOptions> JSONMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                       optional_ptr<TableFunctionInfo> info) {
	auto reader_options = make_uniq<JSONFileReaderOptions>();
	auto &options = reader_options->options;
	if (info) {
		auto &scan_info = info->Cast<JSONScanInfo>();
		options.type = scan_info.type;
		options.format = scan_info.format;
		options.record_type = scan_info.record_type;
		options.auto_detect = scan_info.auto_detect;
		if (scan_info.type == JSONScanType::READ_JSON_OBJECTS) {
			// read_json_objects always emits a single JSON column called "json"
			options.sql_type_list.push_back(LogicalType::JSON());
			options.name_list.emplace_back("json");
		}
	} else {
		// COPY
		options.type = JSONScanType::READ_JSON;
		options.record_type = JSONRecordType::RECORDS;
		options.format = JSONFormat::AUTO_DETECT;
		options.auto_detect = false;
	}
	return std::move(reader_options);
}

bool JSONMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &value, MultiFileOptions &,
                                    BaseFileReaderOptions &options_p) {
	auto &reader_options = options_p.Cast<JSONFileReaderOptions>();
	auto &options = reader_options.options;
	if (value.IsNull()) {
		throw BinderException("Cannot use NULL as argument to key %s", key);
	}
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
			if (val.IsNull()) {
				throw BinderException("read_json \"columns\" parameter type specification cannot be NULL.");
			}
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
			options.format = JSONFormat::NEWLINE_DELIMITED;
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
			} else {
				// Default to newline-delimited otherwise
				options.format = JSONFormat::NEWLINE_DELIMITED;
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
	transform_options.date_format_map = json_data.date_format_map.get();
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
	bool reuse_readers = true;
	for (auto &union_reader : bind_data.union_readers) {
		if (!union_reader || !union_reader->reader) {
			// not all readers have been initialized - don't re-use any
			reuse_readers = false;
			break;
		}
		auto &json_reader = union_reader->reader->Cast<JSONReader>();
		if (!json_reader.IsOpen()) {
			// no open file-handle - close
			reuse_readers = false;
		}
	}
	if (!reuse_readers) {
		bind_data.union_readers.clear();
	} else {
		// re-use readers
		for (auto &union_reader : bind_data.union_readers) {
			auto &json_reader = union_reader->reader->Cast<JSONReader>();
			union_reader->names = names;
			union_reader->types = return_types;
			union_reader->reader->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(names, return_types);
			json_reader.Reset();
		}
	}
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

unique_ptr<GlobalTableFunctionState> JSONMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                              MultiFileBindData &bind_data,
                                                                              MultiFileGlobalState &global_state) {
	auto json_state = make_uniq<JSONGlobalTableFunctionState>(context, bind_data);
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();

	auto &gstate = json_state->state;
	// Perform projection pushdown
	for (idx_t col_idx = 0; col_idx < global_state.column_indexes.size(); col_idx++) {
		auto &column_index = global_state.column_indexes[col_idx];
		const auto &col_id = column_index.GetPrimaryIndex();

		// Skip any multi-file reader / row id stuff
		if (bind_data.reader_bind.filename_idx.IsValid() && col_id == bind_data.reader_bind.filename_idx.GetIndex()) {
			continue;
		}
		if (IsVirtualColumn(col_id)) {
			// Record the output-chunk slot for file_row_number so ReadJSONFunction
			// can fill it with byte offsets after the transform. Other virtual
			// columns (filename, file_index, ...) are still skipped.
			if (col_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				gstate.file_row_number_idx = col_idx;
			}
			continue;
		}
		bool skip = false;
		for (const auto &hive_partitioning_index : bind_data.reader_bind.hive_partitioning_indexes) {
			if (col_id == hive_partitioning_index.index) {
				skip = true;
				break;
			}
		}
		if (skip) {
			continue;
		}

		gstate.names.push_back(json_data.key_names[col_id]);
		gstate.column_ids.push_back(col_idx);
		gstate.column_indices.push_back(column_index);
	}
	if (gstate.names.size() < json_data.key_names.size() || bind_data.file_options.union_by_name) {
		// If we are auto-detecting, but don't need all columns present in the file,
		// then we don't need to throw an error if we encounter an unseen column
		gstate.transform_options.error_unknown_key = false;
	}
	return std::move(json_state);
}

unique_ptr<LocalTableFunctionState> JSONMultiFileInfo::InitializeLocalState(ExecutionContext &context,
                                                                            GlobalTableFunctionState &global_state) {
	auto &gstate = global_state.Cast<JSONGlobalTableFunctionState>();
	auto result = make_uniq<JSONLocalTableFunctionState>(context.client, gstate.state);

	// Copy the transform options / date format map because we need to do thread-local stuff
	result->state.transform_options = gstate.state.transform_options;

	return std::move(result);
}

double JSONReader::GetProgressInFile(ClientContext &context) {
	return GetProgress();
}

shared_ptr<BaseFileReader> JSONMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           BaseUnionData &union_data,
                                                           const MultiFileBindData &bind_data_p) {
	auto &json_data = bind_data_p.bind_data->Cast<JSONScanData>();
	auto reader = make_shared_ptr<JSONReader>(context, json_data.options, union_data.GetFileName());
	reader->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(union_data.names, union_data.types);
	return std::move(reader);
}

shared_ptr<BaseFileReader> JSONMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           const OpenFileInfo &file, idx_t file_idx,
                                                           const MultiFileBindData &bind_data) {
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	auto reader = make_shared_ptr<JSONReader>(context, json_data.options, file.path);
	reader->columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(bind_data.names, bind_data.types);
	return std::move(reader);
}

void JSONReader::PrepareReader(ClientContext &context, GlobalTableFunctionState &gstate_p) {
	auto &gstate = gstate_p.Cast<JSONGlobalTableFunctionState>().state;
	if (gstate.enable_parallel_scans) {
		// if we are doing parallel scans we need to open the file here
		Initialize(gstate.allocator, gstate.buffer_capacity);
	}
}

bool JSONReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                   LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = lstate_p.Cast<JSONLocalTableFunctionState>().state;

	lstate.GetScanState().ResetForNextBuffer();
	return lstate.TryInitializeScan(gstate, *this);
}

void ReadJSONFunction(ClientContext &context, JSONReader &json_reader, JSONScanGlobalState &gstate,
                      JSONScanLocalState &lstate, DataChunk &output) {
	auto &scan_state = lstate.GetScanState();
	D_ASSERT(RefersToSameObject(json_reader, *scan_state.current_reader));

	const auto count = lstate.Read();
	yyjson_val **values = scan_state.values;

	// Use gstate.column_ids (real-column output slots) rather than
	// json_reader.column_ids (which may include virtual columns like
	// file_row_number appended by the multi-file column mapper -- those are
	// filled post-transform below, not by JSONTransform).
	if (!gstate.names.empty()) {
		vector<Vector *> result_vectors;
		result_vectors.reserve(gstate.column_ids.size());
		for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
			result_vectors.emplace_back(&output.data[gstate.column_ids[i]]);
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
			lstate.AddTransformError(lstate.transform_options.object_index,
			                         lstate.transform_options.error_message + hint);
			return;
		}
	}
	if (json_reader.file_row_number_local_idx != DConstants::INVALID_INDEX) {
		auto &frn_vec = output.data[json_reader.file_row_number_local_idx];
		auto *frn_data = FlatVector::GetDataMutable<int64_t>(frn_vec);
		const auto *buffer_handle = scan_state.current_buffer_handle.get();
		const auto *data_base =
		    buffer_handle ? scan_state.buffer_ptr + buffer_handle->buffer_start : scan_state.buffer_ptr;
		const auto file_base = buffer_handle ? buffer_handle->file_start : idx_t {0};
		for (idx_t i = 0; i < count; ++i) {
			const auto local = UnsafeNumericCast<idx_t>(scan_state.units[i].pointer - data_base);
			frn_data[i] = UnsafeNumericCast<int64_t>(file_base + local);
		}
	}
	output.SetChildCardinality(count);
}

void ReadJSONObjectsFunction(ClientContext &context, JSONReader &json_reader, JSONScanGlobalState &gstate,
                             JSONScanLocalState &lstate, DataChunk &output) {
	// Fetch next lines
	auto &scan_state = lstate.GetScanState();
	D_ASSERT(RefersToSameObject(json_reader, *scan_state.current_reader));

	const auto count = lstate.Read();
	const auto units = scan_state.units;
	const auto objects = scan_state.values;

	if (!gstate.names.empty()) {
		// gstate.column_ids[0] is the output slot for the json string column;
		// using output.data[0] would be wrong if file_row_number (or any
		// other virtual) is projected ahead of json.
		auto &json_vec = output.data[gstate.column_ids[0]];
		auto strings = FlatVector::Writer<string_t>(json_vec, count);
		for (idx_t i = 0; i < count; i++) {
			if (objects[i]) {
				strings.WriteStringRef(string_t(units[i].pointer, units[i].size));
			} else {
				strings.WriteNull();
			}
		}
	}

	if (json_reader.file_row_number_local_idx != DConstants::INVALID_INDEX) {
		auto &frn_vec = output.data[json_reader.file_row_number_local_idx];
		auto *frn_data = FlatVector::GetDataMutable<int64_t>(frn_vec);
		const auto *buffer_handle = scan_state.current_buffer_handle.get();
		const auto *data_base =
		    buffer_handle ? scan_state.buffer_ptr + buffer_handle->buffer_start : scan_state.buffer_ptr;
		const auto file_base = buffer_handle ? buffer_handle->file_start : idx_t {0};
		for (idx_t i = 0; i < count; ++i) {
			const auto local = UnsafeNumericCast<idx_t>(units[i].pointer - data_base);
			frn_data[i] = UnsafeNumericCast<int64_t>(file_base + local);
		}
	}

	output.SetChildCardinality(count);
}

AsyncResult JSONReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                             LocalTableFunctionState &local_state, DataChunk &output) {
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	{
		vector<unique_ptr<AsyncTask>> tasks = AsyncResult::GenerateTestTasks();
		if (!tasks.empty()) {
			return AsyncResult(std::move(tasks));
		}
	}
#endif

	auto &gstate = global_state.Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = local_state.Cast<JSONLocalTableFunctionState>().state;
	auto &json_data = gstate.bind_data.bind_data->Cast<JSONScanData>();
	switch (json_data.options.type) {
	case JSONScanType::READ_JSON:
		ReadJSONFunction(context, *this, gstate, lstate, output);
		break;
	case JSONScanType::READ_JSON_OBJECTS:
		ReadJSONObjectsFunction(context, *this, gstate, lstate, output);
		break;
	default:
		throw InternalException("Unsupported scan type for JSONMultiFileInfo::Scan");
	}
	return AsyncResult(output.size() ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED);
}

void JSONReader::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state) {
	auto &gstate = global_state.Cast<JSONGlobalTableFunctionState>().state;
	gstate.file_is_assigned = false;
}

void JSONMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                      LocalTableFunctionState &local_state) {
	auto &lstate = local_state.Cast<JSONLocalTableFunctionState>().state;
	lstate.GetScanState().ResetForNextBuffer();
}

unique_ptr<NodeStatistics> JSONMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	idx_t per_file_cardinality = 42;
	// get the average per-file cardinality from the bind data (if it is set)
	if (json_data.estimated_cardinality_per_file.IsValid()) {
		per_file_cardinality = json_data.estimated_cardinality_per_file.GetIndex();
	}
	return make_uniq<NodeStatistics>(per_file_cardinality * file_count);
}

optional_idx JSONMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
                                           FileExpandResult expand_result) {
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		return optional_idx();
	}
	// get the max threads from the bind data (if it is set)
	auto &json_data = bind_data.bind_data->Cast<JSONScanData>();
	return json_data.max_threads;
}

FileGlobInput JSONMultiFileInfo::GetGlobInput() {
	return FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "json");
}

} // namespace duckdb
