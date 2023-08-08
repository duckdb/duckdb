#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "parquet_statistics.hpp"
#include "column_reader.hpp"

#include "boolean_column_reader.hpp"
#include "row_number_column_reader.hpp"
#include "cast_column_reader.hpp"
#include "callback_column_reader.hpp"
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
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/storage/object_cache.hpp"
#endif

#include <sstream>
#include <cassert>
#include <chrono>
#include <cstring>

namespace duckdb {

using duckdb_parquet::format::ColumnChunk;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::FieldRepetitionType;
using duckdb_parquet::format::FileMetaData;
using ParquetRowGroup = duckdb_parquet::format::RowGroup;
using duckdb_parquet::format::SchemaElement;
using duckdb_parquet::format::Statistics;
using duckdb_parquet::format::Type;

static unique_ptr<duckdb_apache::thrift::protocol::TProtocol>
CreateThriftProtocol(Allocator &allocator, FileHandle &file_handle, bool prefetch_mode) {
	auto transport = make_shared<ThriftFileTransport>(allocator, file_handle, prefetch_mode);
	return make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));
}

static shared_ptr<ParquetFileMetadataCache> LoadMetadata(Allocator &allocator, FileHandle &file_handle) {
	auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

	auto proto = CreateThriftProtocol(allocator, file_handle, false);
	auto &transport = reinterpret_cast<ThriftFileTransport &>(*proto->getTransport());
	auto file_size = transport.GetSize();
	if (file_size < 12) {
		throw InvalidInputException("File '%s' too small to be a Parquet file", file_handle.path);
	}

	ResizeableBuffer buf;
	buf.resize(allocator, 8);
	buf.zero();

	transport.SetLocation(file_size - 8);
	transport.read((uint8_t *)buf.ptr, 8);

	if (memcmp(buf.ptr + 4, "PAR1", 4) != 0) {
		if (memcmp(buf.ptr + 4, "PARE", 4) == 0) {
			throw InvalidInputException("Encrypted Parquet files are not supported for file '%s'", file_handle.path);
		}
		throw InvalidInputException("No magic bytes found at end of file '%s'", file_handle.path);
	}
	// read four-byte footer length from just before the end magic bytes
	auto footer_len = *reinterpret_cast<uint32_t *>(buf.ptr);
	if (footer_len == 0 || file_size < 12 + footer_len) {
		throw InvalidInputException("Footer length error in file '%s'", file_handle.path);
	}
	auto metadata_pos = file_size - (footer_len + 8);
	transport.SetLocation(metadata_pos);
	transport.Prefetch(metadata_pos, footer_len);

	auto metadata = make_uniq<FileMetaData>();
	metadata->read(proto.get());
	return make_shared<ParquetFileMetadataCache>(std::move(metadata), current_time);
}

LogicalType ParquetReader::DeriveLogicalType(const SchemaElement &s_ele, bool binary_as_string) {
	// inner node
	if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
		throw IOException("FIXED_LEN_BYTE_ARRAY requires length to be set");
	}
	if (s_ele.__isset.logicalType) {
		if (s_ele.logicalType.__isset.UUID) {
			if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY) {
				return LogicalType::UUID;
			}
		} else if (s_ele.logicalType.__isset.TIMESTAMP) {
			if (s_ele.logicalType.TIMESTAMP.isAdjustedToUTC) {
				return LogicalType::TIMESTAMP_TZ;
			}
			return LogicalType::TIMESTAMP;
		} else if (s_ele.logicalType.__isset.TIME) {
			if (s_ele.logicalType.TIME.isAdjustedToUTC) {
				return LogicalType::TIME_TZ;
			}
			return LogicalType::TIME;
		}
	}
	if (s_ele.__isset.converted_type) {
		switch (s_ele.converted_type) {
		case ConvertedType::INT_8:
			if (s_ele.type == Type::INT32) {
				return LogicalType::TINYINT;
			} else {
				throw IOException("INT8 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_16:
			if (s_ele.type == Type::INT32) {
				return LogicalType::SMALLINT;
			} else {
				throw IOException("INT16 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_32:
			if (s_ele.type == Type::INT32) {
				return LogicalType::INTEGER;
			} else {
				throw IOException("INT32 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_64:
			if (s_ele.type == Type::INT64) {
				return LogicalType::BIGINT;
			} else {
				throw IOException("INT64 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_8:
			if (s_ele.type == Type::INT32) {
				return LogicalType::UTINYINT;
			} else {
				throw IOException("UINT8 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_16:
			if (s_ele.type == Type::INT32) {
				return LogicalType::USMALLINT;
			} else {
				throw IOException("UINT16 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_32:
			if (s_ele.type == Type::INT32) {
				return LogicalType::UINTEGER;
			} else {
				throw IOException("UINT32 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_64:
			if (s_ele.type == Type::INT64) {
				return LogicalType::UBIGINT;
			} else {
				throw IOException("UINT64 converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::DATE:
			if (s_ele.type == Type::INT32) {
				return LogicalType::DATE;
			} else {
				throw IOException("DATE converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::TIMESTAMP_MICROS:
		case ConvertedType::TIMESTAMP_MILLIS:
			if (s_ele.type == Type::INT64) {
				return LogicalType::TIMESTAMP;
			} else {
				throw IOException("TIMESTAMP converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::DECIMAL:
			if (!s_ele.__isset.precision || !s_ele.__isset.scale) {
				throw IOException("DECIMAL requires a length and scale specifier!");
			}
			switch (s_ele.type) {
			case Type::BYTE_ARRAY:
			case Type::FIXED_LEN_BYTE_ARRAY:
			case Type::INT32:
			case Type::INT64:
				return LogicalType::DECIMAL(s_ele.precision, s_ele.scale);
			default:
				throw IOException(
				    "DECIMAL converted type can only be set for value of Type::(FIXED_LEN_)BYTE_ARRAY/INT32/INT64");
			}
		case ConvertedType::UTF8:
		case ConvertedType::ENUM:
			switch (s_ele.type) {
			case Type::BYTE_ARRAY:
			case Type::FIXED_LEN_BYTE_ARRAY:
				return LogicalType::VARCHAR;
			default:
				throw IOException("UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
			}
		case ConvertedType::TIME_MILLIS:
			if (s_ele.type == Type::INT32) {
				return LogicalType::TIME;
			} else {
				throw IOException("TIME_MILLIS converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::TIME_MICROS:
			if (s_ele.type == Type::INT64) {
				return LogicalType::TIME;
			} else {
				throw IOException("TIME_MICROS converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::INTERVAL:
			return LogicalType::INTERVAL;
		case ConvertedType::JSON:
			return LogicalType::VARCHAR;
		case ConvertedType::MAP:
		case ConvertedType::MAP_KEY_VALUE:
		case ConvertedType::LIST:
		case ConvertedType::BSON:
		default:
			throw IOException("Unsupported converted type");
		}
	} else {
		// no converted type set
		// use default type for each physical type
		switch (s_ele.type) {
		case Type::BOOLEAN:
			return LogicalType::BOOLEAN;
		case Type::INT32:
			return LogicalType::INTEGER;
		case Type::INT64:
			return LogicalType::BIGINT;
		case Type::INT96: // always a timestamp it would seem
			return LogicalType::TIMESTAMP;
		case Type::FLOAT:
			return LogicalType::FLOAT;
		case Type::DOUBLE:
			return LogicalType::DOUBLE;
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			if (binary_as_string) {
				return LogicalType::VARCHAR;
			}
			return LogicalType::BLOB;
		default:
			return LogicalType::INVALID;
		}
	}
}

LogicalType ParquetReader::DeriveLogicalType(const SchemaElement &s_ele) {
	return DeriveLogicalType(s_ele, parquet_options.binary_as_string);
}

unique_ptr<ColumnReader> ParquetReader::CreateReaderRecursive(idx_t depth, idx_t max_define, idx_t max_repeat,
                                                              idx_t &next_schema_idx, idx_t &next_file_idx) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(file_meta_data);
	D_ASSERT(next_schema_idx < file_meta_data->schema.size());
	auto &s_ele = file_meta_data->schema[next_schema_idx];
	auto this_idx = next_schema_idx;

	auto repetition_type = FieldRepetitionType::REQUIRED;
	if (s_ele.__isset.repetition_type && this_idx > 0) {
		repetition_type = s_ele.repetition_type;
	}
	if (repetition_type != FieldRepetitionType::REQUIRED) {
		max_define++;
	}
	if (repetition_type == FieldRepetitionType::REPEATED) {
		max_repeat++;
	}
	if (s_ele.__isset.num_children && s_ele.num_children > 0) { // inner node
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<ColumnReader>> child_readers;

		idx_t c_idx = 0;
		while (c_idx < (idx_t)s_ele.num_children) {
			next_schema_idx++;

			auto &child_ele = file_meta_data->schema[next_schema_idx];

			auto child_reader =
			    CreateReaderRecursive(depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx);
			child_types.push_back(make_pair(child_ele.name, child_reader->Type()));
			child_readers.push_back(std::move(child_reader));

			c_idx++;
		}
		D_ASSERT(!child_types.empty());
		unique_ptr<ColumnReader> result;
		LogicalType result_type;

		bool is_repeated = repetition_type == FieldRepetitionType::REPEATED;
		bool is_list = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::LIST;
		bool is_map = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP;
		bool is_map_kv = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP_KEY_VALUE;
		if (!is_map_kv && this_idx > 0) {
			// check if the parent node of this is a map
			auto &p_ele = file_meta_data->schema[this_idx - 1];
			bool parent_is_map = p_ele.__isset.converted_type && p_ele.converted_type == ConvertedType::MAP;
			bool parent_has_children = p_ele.__isset.num_children && p_ele.num_children == 1;
			is_map_kv = parent_is_map && parent_has_children;
		}

		if (is_map_kv) {
			if (child_types.size() != 2) {
				throw IOException("MAP_KEY_VALUE requires two children");
			}
			if (!is_repeated) {
				throw IOException("MAP_KEY_VALUE needs to be repeated");
			}
			result_type = LogicalType::MAP(std::move(child_types[0].second), std::move(child_types[1].second));

			auto struct_reader =
			    make_uniq<StructColumnReader>(*this, ListType::GetChildType(result_type), s_ele, this_idx,
			                                  max_define - 1, max_repeat - 1, std::move(child_readers));
			return make_uniq<ListColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(struct_reader));
		}
		if (child_types.size() > 1 || (!is_list && !is_map && !is_repeated)) {
			result_type = LogicalType::STRUCT(child_types);
			result = make_uniq<StructColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                       std::move(child_readers));
		} else {
			// if we have a struct with only a single type, pull up
			result_type = child_types[0].second;
			result = std::move(child_readers[0]);
		}
		if (is_repeated) {
			result_type = LogicalType::LIST(result_type);
			return make_uniq<ListColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(result));
		}
		return result;
	} else { // leaf node
		if (!s_ele.__isset.type) {
			throw InvalidInputException(
			    "Node has neither num_children nor type set - this violates the Parquet spec (corrupted file)");
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			const auto derived_type = DeriveLogicalType(s_ele);
			auto list_type = LogicalType::LIST(derived_type);

			auto element_reader =
			    ColumnReader::CreateReader(*this, derived_type, s_ele, next_file_idx++, max_define, max_repeat);

			return make_uniq<ListColumnReader>(*this, list_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(element_reader));
		}
		// TODO check return value of derive type or should we only do this on read()
		return ColumnReader::CreateReader(*this, DeriveLogicalType(s_ele), s_ele, next_file_idx++, max_define,
		                                  max_repeat);
	}
}

// TODO we don't need readers for columns we are not going to read ay
unique_ptr<ColumnReader> ParquetReader::CreateReader() {
	auto file_meta_data = GetFileMetadata();
	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	if (file_meta_data->schema.empty()) {
		throw IOException("Parquet reader: no schema elements found");
	}
	if (file_meta_data->schema[0].num_children == 0) {
		throw IOException("Parquet reader: root schema element has no children");
	}
	auto ret = CreateReaderRecursive(0, 0, 0, next_schema_idx, next_file_idx);
	if (ret->Type().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("Root element of Parquet file must be a struct");
	}
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	D_ASSERT(file_meta_data->row_groups.empty() || next_file_idx == file_meta_data->row_groups[0].columns.size());

	auto &root_struct_reader = ret->Cast<StructColumnReader>();
	// add casts if required
	for (auto &entry : reader_data.cast_map) {
		auto column_idx = entry.first;
		auto &expected_type = entry.second;
		auto child_reader = std::move(root_struct_reader.child_readers[column_idx]);
		auto cast_reader = make_uniq<CastColumnReader>(std::move(child_reader), expected_type);
		root_struct_reader.child_readers[column_idx] = std::move(cast_reader);
	}
	if (parquet_options.file_row_number) {
		root_struct_reader.child_readers.push_back(
		    make_uniq<RowNumberColumnReader>(*this, LogicalType::BIGINT, SchemaElement(), next_file_idx, 0, 0));
	}

	return ret;
}

void ParquetReader::InitializeSchema() {
	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		throw FormatException("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw FormatException("Need at least one non-root column in the file");
	}
	root_reader = CreateReader();
	auto &root_type = root_reader->Type();
	auto &child_types = StructType::GetChildTypes(root_type);
	D_ASSERT(root_type.id() == LogicalTypeId::STRUCT);
	for (auto &type_pair : child_types) {
		names.push_back(type_pair.first);
		return_types.push_back(type_pair.second);
	}

	// Add generated constant column for row number
	if (parquet_options.file_row_number) {
		if (std::find(names.begin(), names.end(), "file_row_number") != names.end()) {
			throw BinderException(
			    "Using file_row_number option on file with column named file_row_number is not supported");
		}
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("file_row_number");
	}
}

ParquetOptions::ParquetOptions(ClientContext &context) {
	Value binary_as_string_val;
	if (context.TryGetCurrentSetting("binary_as_string", binary_as_string_val)) {
		binary_as_string = binary_as_string_val.GetValue<bool>();
	}
}

ParquetReader::ParquetReader(ClientContext &context_p, string file_name_p, ParquetOptions parquet_options_p)
    : fs(FileSystem::GetFileSystem(context_p)), allocator(BufferAllocator::Get(context_p)),
      parquet_options(std::move(parquet_options_p)) {
	file_name = std::move(file_name_p);
	file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
	if (!file_handle->CanSeek()) {
		throw NotImplementedException(
		    "Reading parquet files from a FIFO stream is not supported and cannot be efficiently supported since "
		    "metadata is located at the end of the file. Write the stream to disk first and read from there instead.");
	}
	// If object cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	if (!ObjectCache::ObjectCacheEnabled(context_p)) {
		metadata = LoadMetadata(allocator, *file_handle);
	} else {
		auto last_modify_time = fs.GetLastModifiedTime(*file_handle);
		metadata = ObjectCache::GetObjectCache(context_p).Get<ParquetFileMetadataCache>(file_name);
		if (!metadata || (last_modify_time + 10 >= metadata->read_time)) {
			metadata = LoadMetadata(allocator, *file_handle);
			ObjectCache::GetObjectCache(context_p).Put(file_name, metadata);
		}
	}
	InitializeSchema();
}

ParquetReader::ParquetReader(ClientContext &context_p, ParquetOptions parquet_options_p,
                             shared_ptr<ParquetFileMetadataCache> metadata_p)
    : fs(FileSystem::GetFileSystem(context_p)), allocator(BufferAllocator::Get(context_p)),
      metadata(std::move(metadata_p)), parquet_options(std::move(parquet_options_p)) {
	InitializeSchema();
}

ParquetReader::~ParquetReader() {
}

const FileMetaData *ParquetReader::GetFileMetadata() {
	D_ASSERT(metadata);
	D_ASSERT(metadata->metadata);
	return metadata->metadata.get();
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(const string &name) {
	idx_t file_col_idx;
	for (file_col_idx = 0; file_col_idx < names.size(); file_col_idx++) {
		if (names[file_col_idx] == name) {
			break;
		}
	}
	if (file_col_idx == names.size()) {
		return nullptr;
	}

	unique_ptr<BaseStatistics> column_stats;
	auto file_meta_data = GetFileMetadata();
	auto column_reader = root_reader->Cast<StructColumnReader>().GetChildReader(file_col_idx);

	for (idx_t row_group_idx = 0; row_group_idx < file_meta_data->row_groups.size(); row_group_idx++) {
		auto &row_group = file_meta_data->row_groups[row_group_idx];
		auto chunk_stats = column_reader->Stats(row_group_idx, row_group.columns);
		if (!chunk_stats) {
			return nullptr;
		}
		if (!column_stats) {
			column_stats = std::move(chunk_stats);
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

uint64_t ParquetReader::GetGroupCompressedSize(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	auto total_compressed_size = group.total_compressed_size;

	idx_t calc_compressed_size = 0;

	// If the global total_compressed_size is not set, we can still calculate it
	if (group.total_compressed_size == 0) {
		for (auto &column_chunk : group.columns) {
			calc_compressed_size += column_chunk.meta_data.total_compressed_size;
		}
	}

	if (total_compressed_size != 0 && calc_compressed_size != 0 &&
	    (idx_t)total_compressed_size != calc_compressed_size) {
		throw InvalidInputException("mismatch between calculated compressed size and reported compressed size");
	}

	return total_compressed_size ? total_compressed_size : calc_compressed_size;
}

uint64_t ParquetReader::GetGroupSpan(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	idx_t min_offset = NumericLimits<idx_t>::Maximum();
	idx_t max_offset = NumericLimits<idx_t>::Minimum();

	for (auto &column_chunk : group.columns) {

		// Set the min offset
		idx_t current_min_offset = NumericLimits<idx_t>::Maximum();
		if (column_chunk.meta_data.__isset.dictionary_page_offset) {
			current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.dictionary_page_offset);
		}
		if (column_chunk.meta_data.__isset.index_page_offset) {
			current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.index_page_offset);
		}
		current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.data_page_offset);
		min_offset = MinValue<idx_t>(current_min_offset, min_offset);
		max_offset = MaxValue<idx_t>(max_offset, column_chunk.meta_data.total_compressed_size + current_min_offset);
	}

	return max_offset - min_offset;
}

idx_t ParquetReader::GetGroupOffset(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	idx_t min_offset = NumericLimits<idx_t>::Maximum();

	for (auto &column_chunk : group.columns) {
		if (column_chunk.meta_data.__isset.dictionary_page_offset) {
			min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.dictionary_page_offset);
		}
		if (column_chunk.meta_data.__isset.index_page_offset) {
			min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.index_page_offset);
		}
		min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.data_page_offset);
	}

	return min_offset;
}

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t col_idx) {
	auto &group = GetGroup(state);
	auto column_id = reader_data.column_ids[col_idx];
	auto column_reader = state.root_reader->Cast<StructColumnReader>().GetChildReader(column_id);

	// TODO move this to columnreader too
	if (reader_data.filters) {
		auto stats = column_reader->Stats(state.group_idx_list[state.current_group], group.columns);
		// filters contain output chunk index, not file col idx!
		auto global_id = reader_data.column_mapping[col_idx];
		auto filter_entry = reader_data.filters->filters.find(global_id);
		if (stats && filter_entry != reader_data.filters->filters.end()) {
			bool skip_chunk = false;
			auto &filter = *filter_entry->second;
			auto prune_result = filter.CheckStatistics(*stats);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				skip_chunk = true;
			}
			if (skip_chunk) {
				// this effectively will skip this chunk
				state.group_offset = group.num_rows;
				return;
			}
		}
	}

	state.root_reader->InitializeRead(state.group_idx_list[state.current_group], group.columns,
	                                  *state.thrift_file_proto);
}

idx_t ParquetReader::NumRows() {
	return GetFileMetadata()->num_rows;
}

idx_t ParquetReader::NumRowGroups() {
	return GetFileMetadata()->row_groups.size();
}

void ParquetReader::InitializeScan(ParquetReaderScanState &state, vector<idx_t> groups_to_read) {
	state.current_group = -1;
	state.finished = false;
	state.group_offset = 0;
	state.group_idx_list = std::move(groups_to_read);
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
	if (!state.file_handle || state.file_handle->path != file_handle->path) {
		auto flags = FileFlags::FILE_FLAGS_READ;

		if (!file_handle->OnDiskFile() && file_handle->CanSeek()) {
			state.prefetch_mode = true;
			flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
		} else {
			state.prefetch_mode = false;
		}

		state.file_handle = fs.OpenFile(file_handle->path, flags);
	}

	state.thrift_file_proto = CreateThriftProtocol(allocator, *state.file_handle, state.prefetch_mode);
	state.root_reader = CreateReader();
	state.define_buf.resize(allocator, STANDARD_VECTOR_SIZE);
	state.repeat_buf.resize(allocator, STANDARD_VECTOR_SIZE);
}

void FilterIsNull(Vector &v, parquet_filter_t &filter_mask, idx_t count) {
	if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto &mask = ConstantVector::Validity(v);
		if (mask.RowIsValid(0)) {
			filter_mask.reset();
		}
		return;
	}
	D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);

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
	if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto &mask = ConstantVector::Validity(v);
		if (!mask.RowIsValid(0)) {
			filter_mask.reset();
		}
		return;
	}
	D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);

	auto &mask = FlatVector::Validity(v);
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && mask.RowIsValid(i);
		}
	}
}

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, T constant, parquet_filter_t &filter_mask, idx_t count) {
	if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto v_ptr = ConstantVector::GetData<T>(v);
		auto &mask = ConstantVector::Validity(v);

		if (mask.RowIsValid(0)) {
			if (!OP::Operation(v_ptr[0], constant)) {
				filter_mask.reset();
			}
		}
		return;
	}

	D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);
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

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, const Value &constant, parquet_filter_t &filter_mask, idx_t count) {
	TemplatedFilterOperation<T, OP>(v, constant.template GetValueUnsafe<T>(), filter_mask, count);
}

template <class OP>
static void FilterOperationSwitch(Vector &v, Value &constant, parquet_filter_t &filter_mask, idx_t count) {
	if (filter_mask.none() || count == 0) {
		return;
	}
	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedFilterOperation<bool, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::UINT8:
		TemplatedFilterOperation<uint8_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::UINT16:
		TemplatedFilterOperation<uint16_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::UINT32:
		TemplatedFilterOperation<uint32_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::UINT64:
		TemplatedFilterOperation<uint64_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::INT8:
		TemplatedFilterOperation<int8_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::INT16:
		TemplatedFilterOperation<int16_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::INT32:
		TemplatedFilterOperation<int32_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::INT64:
		TemplatedFilterOperation<int64_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::INT128:
		TemplatedFilterOperation<hugeint_t, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedFilterOperation<float, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedFilterOperation<double, OP>(v, constant, filter_mask, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedFilterOperation<string_t, OP>(v, constant, filter_mask, count);
		break;
	default:
		throw NotImplementedException("Unsupported type for filter %s", v.ToString());
	}
}

static void ApplyFilter(Vector &v, TableFilter &filter, parquet_filter_t &filter_mask, idx_t count) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction.child_filters) {
			ApplyFilter(v, *child_filter, filter_mask, count);
		}
		break;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		parquet_filter_t or_mask;
		for (auto &child_filter : conjunction.child_filters) {
			parquet_filter_t child_mask = filter_mask;
			ApplyFilter(v, *child_filter, child_mask, count);
			or_mask |= child_mask;
		}
		filter_mask &= or_mask;
		break;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
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

		auto &trans = reinterpret_cast<ThriftFileTransport &>(*state.thrift_file_proto->getTransport());
		trans.ClearPrefetch();
		state.current_group_prefetched = false;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return false;
		}

		uint64_t to_scan_compressed_bytes = 0;
		for (idx_t col_idx = 0; col_idx < reader_data.column_ids.size(); col_idx++) {
			PrepareRowGroupBuffer(state, col_idx);

			auto file_col_idx = reader_data.column_ids[col_idx];

			auto &root_reader = state.root_reader->Cast<StructColumnReader>();
			to_scan_compressed_bytes += root_reader.GetChildReader(file_col_idx)->TotalCompressedSize();
		}

		auto &group = GetGroup(state);
		if (state.prefetch_mode && state.group_offset != (idx_t)group.num_rows) {

			uint64_t total_row_group_span = GetGroupSpan(state);

			double scan_percentage = (double)(to_scan_compressed_bytes) / total_row_group_span;

			if (to_scan_compressed_bytes > total_row_group_span) {
				throw InvalidInputException(
				    "Malformed parquet file: sum of total compressed bytes of columns seems incorrect");
			}

			if (!reader_data.filters &&
			    scan_percentage > ParquetReaderPrefetchConfig::WHOLE_GROUP_PREFETCH_MINIMUM_SCAN) {
				// Prefetch the whole row group
				if (!state.current_group_prefetched) {
					auto total_compressed_size = GetGroupCompressedSize(state);
					if (total_compressed_size > 0) {
						trans.Prefetch(GetGroupOffset(state), total_row_group_span);
					}
					state.current_group_prefetched = true;
				}
			} else {
				// lazy fetching is when all tuples in a column can be skipped. With lazy fetching the buffer is only
				// fetched on the first read to that buffer.
				bool lazy_fetch = reader_data.filters;

				// Prefetch column-wise
				for (idx_t col_idx = 0; col_idx < reader_data.column_ids.size(); col_idx++) {
					auto file_col_idx = reader_data.column_ids[col_idx];
					auto &root_reader = state.root_reader->Cast<StructColumnReader>();

					bool has_filter = false;
					if (reader_data.filters) {
						auto entry = reader_data.filters->filters.find(reader_data.column_mapping[col_idx]);
						has_filter = entry != reader_data.filters->filters.end();
					}
					root_reader.GetChildReader(file_col_idx)->RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
				}

				trans.FinalizeRegistration();

				if (!lazy_fetch) {
					trans.PrefetchRegistered();
				}
			}
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

	// mask out unused part of bitset
	for (idx_t i = this_output_chunk_rows; i < STANDARD_VECTOR_SIZE; i++) {
		filter_mask.set(i, false);
	}

	state.define_buf.zero();
	state.repeat_buf.zero();

	auto define_ptr = (uint8_t *)state.define_buf.ptr;
	auto repeat_ptr = (uint8_t *)state.repeat_buf.ptr;

	auto &root_reader = state.root_reader->Cast<StructColumnReader>();

	if (reader_data.filters) {
		vector<bool> need_to_read(reader_data.column_ids.size(), true);

		// first load the columns that are used in filters
		for (auto &filter_col : reader_data.filters->filters) {
			if (filter_mask.none()) {
				// if no rows are left we can stop checking filters
				break;
			}
			auto filter_entry = reader_data.filter_map[filter_col.first];
			if (filter_entry.is_constant) {
				// this is a constant vector, look for the constant
				auto &constant = reader_data.constant_map[filter_entry.index].value;
				Vector constant_vector(constant);
				ApplyFilter(constant_vector, *filter_col.second, filter_mask, this_output_chunk_rows);
			} else {
				auto id = filter_entry.index;
				auto file_col_idx = reader_data.column_ids[id];
				auto result_idx = reader_data.column_mapping[id];

				auto &result_vector = result.data[result_idx];
				auto child_reader = root_reader.GetChildReader(file_col_idx);
				child_reader->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result_vector);
				need_to_read[id] = false;

				ApplyFilter(result_vector, *filter_col.second, filter_mask, this_output_chunk_rows);
			}
		}

		// we still may have to read some cols
		for (idx_t col_idx = 0; col_idx < reader_data.column_ids.size(); col_idx++) {
			if (!need_to_read[col_idx]) {
				continue;
			}
			auto file_col_idx = reader_data.column_ids[col_idx];
			if (filter_mask.none()) {
				root_reader.GetChildReader(file_col_idx)->Skip(result.size());
				continue;
			}
			auto &result_vector = result.data[reader_data.column_mapping[col_idx]];
			auto child_reader = root_reader.GetChildReader(file_col_idx);
			child_reader->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result_vector);
		}

		idx_t sel_size = 0;
		for (idx_t i = 0; i < this_output_chunk_rows; i++) {
			if (filter_mask[i]) {
				state.sel.set_index(sel_size++, i);
			}
		}

		result.Slice(state.sel, sel_size);
	} else {
		for (idx_t col_idx = 0; col_idx < reader_data.column_ids.size(); col_idx++) {
			auto file_col_idx = reader_data.column_ids[col_idx];
			auto &result_vector = result.data[reader_data.column_mapping[col_idx]];
			auto child_reader = root_reader.GetChildReader(file_col_idx);
			auto rows_read = child_reader->Read(result.size(), filter_mask, define_ptr, repeat_ptr, result_vector);
			if (rows_read != result.size()) {
				throw InvalidInputException("Mismatch in parquet read for column %llu, expected %llu rows, got %llu",
				                            file_col_idx, result.size(), rows_read);
			}
		}
	}

	state.group_offset += this_output_chunk_rows;
	return true;
}

} // namespace duckdb
