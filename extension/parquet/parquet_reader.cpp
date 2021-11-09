#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "parquet_statistics.hpp"
#include "column_reader.hpp"

#include "boolean_column_reader.hpp"
#include "callback_column_reader.hpp"
#include "decimal_column_reader.hpp"
#include "list_column_reader.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "templated_column_reader.hpp"

#include "thrift_tools.hpp"

#include "parquet_file_metadata_cache.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/pair.hpp"

#include "duckdb/storage/object_cache.hpp"
#endif

#include <sstream>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>

namespace duckdb {

using duckdb_parquet::format::ColumnChunk;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::FieldRepetitionType;
using duckdb_parquet::format::FileMetaData;
using ParquetRowGroup = duckdb_parquet::format::RowGroup;
using duckdb_parquet::format::SchemaElement;
using duckdb_parquet::format::Statistics;
using duckdb_parquet::format::Type;

static unique_ptr<duckdb_apache::thrift::protocol::TProtocol> CreateThriftProtocol(Allocator &allocator,
                                                                                   FileHandle &file_handle) {
	auto transport = make_shared<ThriftFileTransport>(allocator, file_handle);
	return make_unique<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(move(transport));
}

static shared_ptr<ParquetFileMetadataCache> LoadMetadata(Allocator &allocator, FileHandle &file_handle) {
	auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

	auto proto = CreateThriftProtocol(allocator, file_handle);
	auto &transport = ((ThriftFileTransport &)*proto->getTransport());
	auto file_size = transport.GetSize();
	if (file_size < 12) {
		throw InvalidInputException("File '%s' too small to be a Parquet file", file_handle.path);
	}

	ResizeableBuffer buf;
	buf.resize(allocator, 8);
	buf.zero();

	transport.SetLocation(file_size - 8);
	transport.read((uint8_t *)buf.ptr, 8);

	if (strncmp(buf.ptr + 4, "PAR1", 4) != 0) {
		throw InvalidInputException("No magic bytes found at end of file '%s'", file_handle.path);
	}
	// read four-byte footer length from just before the end magic bytes
	auto footer_len = *(uint32_t *)buf.ptr;
	if (footer_len <= 0 || file_size < 12 + footer_len) {
		throw InvalidInputException("Footer length error in file '%s'", file_handle.path);
	}
	auto metadata_pos = file_size - (footer_len + 8);
	transport.SetLocation(metadata_pos);
	transport.Prefetch(metadata_pos, footer_len);

	auto metadata = make_unique<FileMetaData>();
	metadata->read(proto.get());
	return make_shared<ParquetFileMetadataCache>(move(metadata), current_time);
}

LogicalType ParquetReader::DeriveLogicalType(const SchemaElement &s_ele) {
	// inner node
	D_ASSERT(s_ele.__isset.type && s_ele.num_children == 0);
	switch (s_ele.type) {
	case Type::BOOLEAN:
		return LogicalType::BOOLEAN;
	case Type::INT32:
		if (s_ele.__isset.converted_type) {
			switch (s_ele.converted_type) {
			case ConvertedType::DATE:
				return LogicalType::DATE;
			case ConvertedType::UINT_8:
				return LogicalType::UTINYINT;
			case ConvertedType::UINT_16:
				return LogicalType::USMALLINT;
			default:
				return LogicalType::INTEGER;
			}
		}
		return LogicalType::INTEGER;
	case Type::INT64:
		if (s_ele.__isset.converted_type) {
			switch (s_ele.converted_type) {
			case ConvertedType::TIMESTAMP_MICROS:
			case ConvertedType::TIMESTAMP_MILLIS:
				return LogicalType::TIMESTAMP;
			case ConvertedType::UINT_32:
				return LogicalType::UINTEGER;
			case ConvertedType::UINT_64:
				return LogicalType::UBIGINT;
			default:
				return LogicalType::BIGINT;
			}
		}
		return LogicalType::BIGINT;

	case Type::INT96: // always a timestamp it would seem
		return LogicalType::TIMESTAMP;
	case Type::FLOAT:
		return LogicalType::FLOAT;
	case Type::DOUBLE:
		return LogicalType::DOUBLE;
	case Type::BYTE_ARRAY:
	case Type::FIXED_LEN_BYTE_ARRAY:
		if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
			return LogicalType::INVALID;
		}
		if (s_ele.__isset.converted_type) {
			switch (s_ele.converted_type) {
			case ConvertedType::DECIMAL:
				if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && s_ele.__isset.scale && s_ele.__isset.type_length) {
					return LogicalType::DECIMAL(s_ele.precision, s_ele.scale);
				}
				return LogicalType::INVALID;

			case ConvertedType::UTF8:
				return LogicalType::VARCHAR;
			default:
				return LogicalType::BLOB;
			}
		}
		if (parquet_options.binary_as_string) {
			return LogicalType::VARCHAR;
		}
		return LogicalType::BLOB;
	default:
		return LogicalType::INVALID;
	}
}

unique_ptr<ColumnReader> ParquetReader::CreateReaderRecursive(const FileMetaData *file_meta_data, idx_t depth,
                                                              idx_t max_define, idx_t max_repeat,
                                                              idx_t &next_schema_idx, idx_t &next_file_idx) {
	D_ASSERT(file_meta_data);
	D_ASSERT(next_schema_idx < file_meta_data->schema.size());
	auto &s_ele = file_meta_data->schema[next_schema_idx];
	auto this_idx = next_schema_idx;

	if (s_ele.__isset.repetition_type) {
		if (s_ele.repetition_type != FieldRepetitionType::REQUIRED) {
			max_define++;
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			max_repeat++;
		}
	}

	if (!s_ele.__isset.type) { // inner node
		if (s_ele.num_children == 0) {
			throw std::runtime_error("Node has no children but should");
		}
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<ColumnReader>> child_readers;

		idx_t c_idx = 0;
		while (c_idx < (idx_t)s_ele.num_children) {
			next_schema_idx++;

			auto &child_ele = file_meta_data->schema[next_schema_idx];

			auto child_reader = CreateReaderRecursive(file_meta_data, depth + 1, max_define, max_repeat,
			                                          next_schema_idx, next_file_idx);
			child_types.push_back(make_pair(child_ele.name, child_reader->Type()));
			child_readers.push_back(move(child_reader));

			c_idx++;
		}
		D_ASSERT(!child_types.empty());
		unique_ptr<ColumnReader> result;
		LogicalType result_type;
		// if we only have a single child no reason to create a struct ay
		if (child_types.size() > 1 || depth == 0) {
			result_type = LogicalType::STRUCT(move(child_types));
			result = make_unique<StructColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                         move(child_readers));
		} else {
			// if we have a struct with only a single type, pull up
			result_type = child_types[0].second;
			result = move(child_readers[0]);
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			result_type = LogicalType::LIST(result_type);
			return make_unique<ListColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                     move(result));
		}
		return result;
	} else { // leaf node
		// TODO check return value of derive type or should we only do this on read()
		return ColumnReader::CreateReader(*this, DeriveLogicalType(s_ele), s_ele, next_file_idx++, max_define,
		                                  max_repeat);
	}
}

// TODO we don't need readers for columns we are not going to read ay
unique_ptr<ColumnReader> ParquetReader::CreateReader(const duckdb_parquet::format::FileMetaData *file_meta_data) {
	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	auto ret = CreateReaderRecursive(file_meta_data, 0, 0, 0, next_schema_idx, next_file_idx);
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	D_ASSERT(file_meta_data->row_groups.empty() || next_file_idx == file_meta_data->row_groups[0].columns.size());
	return ret;
}

void ParquetReader::InitializeSchema(const vector<LogicalType> &expected_types_p, const string &initial_filename_p) {
	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		throw FormatException("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw FormatException("Need at least one non-root column in the file");
	}

	bool has_expected_types = !expected_types_p.empty();
	auto root_reader = CreateReader(file_meta_data);

	auto &root_type = root_reader->Type();
	auto &child_types = StructType::GetChildTypes(root_type);
	D_ASSERT(root_type.id() == LogicalTypeId::STRUCT);
	if (has_expected_types && child_types.size() != expected_types_p.size()) {
		throw FormatException("column count mismatch");
	}
	idx_t col_idx = 0;
	for (auto &type_pair : child_types) {
		if (has_expected_types && expected_types_p[col_idx] != type_pair.second) {
			if (initial_filename_p.empty()) {
				throw FormatException("column \"%d\" in parquet file is of type %s, could not auto cast to "
				                      "expected type %s for this column",
				                      col_idx, type_pair.second, expected_types_p[col_idx].ToString());
			} else {
				throw FormatException("schema mismatch in Parquet glob: column \"%d\" in parquet file is of type "
				                      "%s, but in the original file \"%s\" this column is of type \"%s\"",
				                      col_idx, type_pair.second, initial_filename_p,
				                      expected_types_p[col_idx].ToString());
			}
		} else {
			names.push_back(type_pair.first);
			return_types.push_back(type_pair.second);
		}
		col_idx++;
	}
	D_ASSERT(!names.empty());
	D_ASSERT(!return_types.empty());
}

ParquetReader::ParquetReader(Allocator &allocator_p, unique_ptr<FileHandle> file_handle_p,
                             const vector<LogicalType> &expected_types_p, const string &initial_filename_p)
    : allocator(allocator_p) {
	file_name = file_handle_p->path;
	file_handle = move(file_handle_p);
	metadata = LoadMetadata(allocator, *file_handle);
	InitializeSchema(expected_types_p, initial_filename_p);
}

ParquetReader::ParquetReader(ClientContext &context_p, string file_name_p, const vector<LogicalType> &expected_types_p,
                             ParquetOptions parquet_options_p, const string &initial_filename_p)
    : allocator(Allocator::Get(context_p)), file_opener(FileSystem::GetFileOpener(context_p)),
      parquet_options(parquet_options_p) {
	auto &fs = FileSystem::GetFileSystem(context_p);
	file_name = move(file_name_p);
	file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
	                          FileSystem::DEFAULT_COMPRESSION, file_opener);
	// If object cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	auto last_modify_time = fs.GetLastModifiedTime(*file_handle);
	if (!ObjectCache::ObjectCacheEnabled(context_p)) {
		metadata = LoadMetadata(allocator, *file_handle);
	} else {
		metadata =
		    std::dynamic_pointer_cast<ParquetFileMetadataCache>(ObjectCache::GetObjectCache(context_p).Get(file_name));
		if (!metadata || (last_modify_time + 10 >= metadata->read_time)) {
			metadata = LoadMetadata(allocator, *file_handle);
			ObjectCache::GetObjectCache(context_p).Put(file_name, metadata);
		}
	}
	InitializeSchema(expected_types_p, initial_filename_p);
}

ParquetReader::~ParquetReader() {
}

const FileMetaData *ParquetReader::GetFileMetadata() {
	D_ASSERT(metadata);
	D_ASSERT(metadata->metadata);
	return metadata->metadata.get();
}

// TODO also somewhat ugly, perhaps this can be moved to the column reader too
unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(ParquetReader &reader, LogicalType &type,
                                                         column_t file_col_idx, const FileMetaData *file_meta_data) {
	unique_ptr<BaseStatistics> column_stats;
	auto root_reader = reader.CreateReader(file_meta_data);
	auto column_reader = ((StructColumnReader *)root_reader.get())->GetChildReader(file_col_idx);

	for (auto &row_group : file_meta_data->row_groups) {
		auto chunk_stats = column_reader->Stats(row_group.columns);
		if (!chunk_stats) {
			return nullptr;
		}
		if (!column_stats) {
			column_stats = move(chunk_stats);
		} else {
			column_stats->Merge(*chunk_stats);
		}
	}
	return column_stats;
}

const ParquetRowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	D_ASSERT(state.group_idx_list[state.current_group] >= 0 &&
	         state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
	return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
}

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t out_col_idx) {
	auto &group = GetGroup(state);

	auto column_reader = ((StructColumnReader *)state.root_reader.get())->GetChildReader(state.column_ids[out_col_idx]);

	// TODO move this to columnreader too
	if (state.filters) {
		auto stats = column_reader->Stats(group.columns);
		// filters contain output chunk index, not file col idx!
		auto filter_entry = state.filters->filters.find(out_col_idx);
		if (stats && filter_entry != state.filters->filters.end()) {
			bool skip_chunk = false;
			auto &filter = *filter_entry->second;
			auto prune_result = filter.CheckStatistics(*stats);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				skip_chunk = true;
			}
			if (skip_chunk) {
				state.group_offset = group.num_rows;
				return;
				// this effectively will skip this chunk
			}
		}
	}

	state.root_reader->InitializeRead(group.columns, *state.thrift_file_proto);
}

idx_t ParquetReader::NumRows() {
	return GetFileMetadata()->num_rows;
}

idx_t ParquetReader::NumRowGroups() {
	return GetFileMetadata()->row_groups.size();
}

void ParquetReader::InitializeScan(ParquetReaderScanState &state, vector<column_t> column_ids,
                                   vector<idx_t> groups_to_read, TableFilterSet *filters) {
	state.current_group = -1;
	state.finished = false;
	state.column_ids = move(column_ids);
	state.group_offset = 0;
	state.group_idx_list = move(groups_to_read);
	state.filters = filters;
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
	state.file_handle =
	    file_handle->file_system.OpenFile(file_handle->path, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
	                                      FileSystem::DEFAULT_COMPRESSION, file_opener);
	state.thrift_file_proto = CreateThriftProtocol(allocator, *state.file_handle);
	state.root_reader = CreateReader(GetFileMetadata());

	state.define_buf.resize(allocator, STANDARD_VECTOR_SIZE);
	state.repeat_buf.resize(allocator, STANDARD_VECTOR_SIZE);
}

void FilterIsNull(Vector &v, parquet_filter_t &filter_mask, idx_t count) {
	auto &mask = FlatVector::Validity(v);
	if (mask.AllValid()) {
		filter_mask.reset();
	} else {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && !mask.RowIsValid(i);
		}
	}
}

void FilterIsNotNull(Vector &v, parquet_filter_t &filter_mask, idx_t count) {
	auto &mask = FlatVector::Validity(v);
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && mask.RowIsValid(i);
		}
	}
}

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, T constant, parquet_filter_t &filter_mask, idx_t count) {
	D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR); // we just created the damn thing it better be

	auto v_ptr = FlatVector::GetData<T>(v);
	auto &mask = FlatVector::Validity(v);

	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
		}
	}
}

template <class OP>
static void FilterOperationSwitch(Vector &v, Value &constant, parquet_filter_t &filter_mask, idx_t count) {
	if (filter_mask.none() || count == 0) {
		return;
	}
	switch (v.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		TemplatedFilterOperation<bool, OP>(v, constant.value_.boolean, filter_mask, count);
		break;
	case LogicalTypeId::UTINYINT:
		TemplatedFilterOperation<uint8_t, OP>(v, constant.value_.utinyint, filter_mask, count);
		break;
	case LogicalTypeId::USMALLINT:
		TemplatedFilterOperation<uint16_t, OP>(v, constant.value_.usmallint, filter_mask, count);
		break;
	case LogicalTypeId::UINTEGER:
		TemplatedFilterOperation<uint32_t, OP>(v, constant.value_.uinteger, filter_mask, count);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedFilterOperation<uint64_t, OP>(v, constant.value_.ubigint, filter_mask, count);
		break;
	case LogicalTypeId::INTEGER:
		TemplatedFilterOperation<int32_t, OP>(v, constant.value_.integer, filter_mask, count);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedFilterOperation<int64_t, OP>(v, constant.value_.bigint, filter_mask, count);
		break;
	case LogicalTypeId::FLOAT:
		TemplatedFilterOperation<float, OP>(v, constant.value_.float_, filter_mask, count);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedFilterOperation<double, OP>(v, constant.value_.double_, filter_mask, count);
		break;
	case LogicalTypeId::DATE:
		TemplatedFilterOperation<date_t, OP>(v, constant.value_.date, filter_mask, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		TemplatedFilterOperation<timestamp_t, OP>(v, constant.value_.timestamp, filter_mask, count);
		break;
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		TemplatedFilterOperation<string_t, OP>(v, string_t(constant.str_value), filter_mask, count);
		break;
	case LogicalTypeId::DECIMAL:
		switch (v.GetType().InternalType()) {
		case PhysicalType::INT16:
			TemplatedFilterOperation<int16_t, OP>(v, constant.value_.smallint, filter_mask, count);
			break;
		case PhysicalType::INT32:
			TemplatedFilterOperation<int32_t, OP>(v, constant.value_.integer, filter_mask, count);
			break;
		case PhysicalType::INT64:
			TemplatedFilterOperation<int64_t, OP>(v, constant.value_.bigint, filter_mask, count);
			break;
		case PhysicalType::INT128:
			TemplatedFilterOperation<hugeint_t, OP>(v, constant.value_.hugeint, filter_mask, count);
			break;
		default:
			throw InternalException("Unsupported internal type for decimal");
		}
		break;
	default:
		throw NotImplementedException("Unsupported type for filter %s", v.ToString());
	}
}

static void ApplyFilter(Vector &v, TableFilter &filter, parquet_filter_t &filter_mask, idx_t count) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = (ConjunctionAndFilter &)filter;
		for (auto &child_filter : conjunction.child_filters) {
			ApplyFilter(v, *child_filter, filter_mask, count);
		}
		break;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = (ConjunctionOrFilter &)filter;
		for (auto &child_filter : conjunction.child_filters) {
			parquet_filter_t child_mask = filter_mask;
			ApplyFilter(v, *child_filter, child_mask, count);
			filter_mask |= child_mask;
		}
		break;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (ConstantFilter &)filter;
		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			FilterOperationSwitch<Equals>(v, constant_filter.constant, filter_mask, count);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			FilterOperationSwitch<LessThan>(v, constant_filter.constant, filter_mask, count);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			FilterOperationSwitch<LessThanEquals>(v, constant_filter.constant, filter_mask, count);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			FilterOperationSwitch<GreaterThan>(v, constant_filter.constant, filter_mask, count);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			FilterOperationSwitch<GreaterThanEquals>(v, constant_filter.constant, filter_mask, count);
			break;
		default:
			D_ASSERT(0);
		}
		break;
	}
	case TableFilterType::IS_NOT_NULL:
		FilterIsNotNull(v, filter_mask, count);
		break;
	case TableFilterType::IS_NULL:
		FilterIsNull(v, filter_mask, count);
		break;
	default:
		D_ASSERT(0);
		break;
	}
}

void ParquetReader::Scan(ParquetReaderScanState &state, DataChunk &result) {
	while (ScanInternal(state, result)) {
		if (result.size() > 0) {
			break;
		}
		result.Reset();
	}
}

bool ParquetReader::ScanInternal(ParquetReaderScanState &state, DataChunk &result) {
	if (state.finished) {
		return false;
	}

	// see if we have to switch to the next row group in the parquet file
	if (state.current_group < 0 || (int64_t)state.group_offset >= GetGroup(state).num_rows) {
		state.current_group++;
		state.group_offset = 0;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return false;
		}

		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			// this is a special case where we are not interested in the actual contents of the file
			if (state.column_ids[out_col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}

			PrepareRowGroupBuffer(state, out_col_idx);
		}
		return true;
	}

	auto this_output_chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.group_offset);
	result.SetCardinality(this_output_chunk_rows);

	if (this_output_chunk_rows == 0) {
		state.finished = true;
		return false; // end of last group, we are done
	}

	// we evaluate simple table filters directly in this scan so we can skip decoding column data that's never going to
	// be relevant
	parquet_filter_t filter_mask;
	filter_mask.set();

	state.define_buf.zero();
	state.repeat_buf.zero();

	auto define_ptr = (uint8_t *)state.define_buf.ptr;
	auto repeat_ptr = (uint8_t *)state.repeat_buf.ptr;

	auto root_reader = ((StructColumnReader *)state.root_reader.get());

	if (state.filters) {
		vector<bool> need_to_read(result.ColumnCount(), true);

		// first load the columns that are used in filters
		for (auto &filter_col : state.filters->filters) {
			auto file_col_idx = state.column_ids[filter_col.first];

			if (filter_mask.none()) { // if no rows are left we can stop checking filters
				break;
			}

			root_reader->GetChildReader(file_col_idx)
			    ->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result.data[filter_col.first]);

			need_to_read[filter_col.first] = false;

			ApplyFilter(result.data[filter_col.first], *filter_col.second, filter_mask, this_output_chunk_rows);
		}

		// we still may have to read some cols
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			if (!need_to_read[out_col_idx]) {
				continue;
			}
			auto file_col_idx = state.column_ids[out_col_idx];

			if (filter_mask.none()) {
				root_reader->GetChildReader(file_col_idx)->Skip(result.size());
				continue;
			}
			// TODO handle ROWID here, too
			root_reader->GetChildReader(file_col_idx)
			    ->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result.data[out_col_idx]);
		}

		idx_t sel_size = 0;
		for (idx_t i = 0; i < this_output_chunk_rows; i++) {
			if (filter_mask[i]) {
				state.sel.set_index(sel_size++, i);
			}
		}

		result.Slice(state.sel, sel_size);
		result.Verify();

	} else { // #nofilter, just fricking load the data
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			auto file_col_idx = state.column_ids[out_col_idx];

			if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				Value constant_42 = Value::BIGINT(42);
				result.data[out_col_idx].Reference(constant_42);
				continue;
			}

			root_reader->GetChildReader(file_col_idx)
			    ->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result.data[out_col_idx]);
		}
	}

	state.group_offset += this_output_chunk_rows;
	return true;
}

} // namespace duckdb
