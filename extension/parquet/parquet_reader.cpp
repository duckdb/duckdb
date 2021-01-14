#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "parquet_statistics.hpp"

#include "thrift_tools.hpp"

#include "parquet_file_metadata_cache.hpp"

#include "duckdb/planner/table_filter.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/pair.hpp"

#include "duckdb/storage/object_cache.hpp"

#include <sstream>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>

namespace duckdb {

using parquet::format::ColumnChunk;
using parquet::format::ConvertedType;
using parquet::format::FieldRepetitionType;
using parquet::format::FileMetaData;
using parquet::format::RowGroup;
using parquet::format::SchemaElement;
using parquet::format::Statistics;
using parquet::format::Type;

static shared_ptr<ParquetFileMetadataCache> load_metadata(apache::thrift::protocol::TProtocol &proto, idx_t read_pos) {
	auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	auto metadata = make_unique<FileMetaData>();
	thrift_unpack_file(proto, read_pos, metadata.get());
	return make_shared<ParquetFileMetadataCache>(move(metadata), current_time);
}

static LogicalType derive_type(const SchemaElement &s_ele) {
	// inner node
	D_ASSERT(s_ele.__isset.type && s_ele.num_children == 0);
	switch (s_ele.type) {
	case Type::BOOLEAN:
		return LogicalType::BOOLEAN;
	case Type::INT32:
		return LogicalType::INTEGER;
	case Type::INT64:
		if (s_ele.__isset.converted_type) {
			switch (s_ele.converted_type) {
			case ConvertedType::TIMESTAMP_MICROS:
			case ConvertedType::TIMESTAMP_MILLIS:
				return LogicalType::TIMESTAMP;
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
		//			case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: {
		// TODO some decimals yuck
	case Type::BYTE_ARRAY:
		if (s_ele.__isset.converted_type) {
			switch (s_ele.converted_type) {
			case ConvertedType::UTF8:
				return LogicalType::VARCHAR;
			default:
				return LogicalType::BLOB;
			}
		}
		return LogicalType::BLOB;
	default:
		return LogicalType::INVALID;
	}
}

unique_ptr<ColumnReader> create_reader(std::vector<SchemaElement> schema, idx_t depth, idx_t max_define,
                                       idx_t max_repeat, idx_t &next_schema_idx, idx_t &next_file_idx) {
	D_ASSERT(next_schema_idx < schema.size());
	auto &s_ele = schema[next_schema_idx];
	auto this_idx = next_schema_idx;

	if (s_ele.__isset.repetition_type) {
		if (s_ele.repetition_type == FieldRepetitionType::OPTIONAL) {
			max_define = depth;
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			max_repeat++;
		}
	}

	if (!s_ele.__isset.type) { // inner node
		D_ASSERT(s_ele.num_children > 0);
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<ColumnReader>> child_readers;

		idx_t c_idx = 0;
		while (c_idx < s_ele.num_children) {
			next_schema_idx++;

			auto &child_ele = schema[next_schema_idx];

			auto child_reader =
			    create_reader(schema, depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx);
			child_types.push_back(make_pair(child_ele.name, child_reader->Type()));
			child_readers.push_back(move(child_reader));

			c_idx++;
		}
		D_ASSERT(child_types.size() > 0);
		unique_ptr<ColumnReader> result;
		LogicalType result_type;
		// if we only have a single child no reason to create a struct ay
		if (child_types.size() > 1) {
			result_type = LogicalType(LogicalTypeId::STRUCT, child_types);
			result = make_unique<StructColumnReader>(result_type, s_ele, this_idx, max_define, max_repeat,
			                                         move(child_readers));
		} else {
			// if we have a struct with only a single type, pull up
			result_type = child_types[0].second;
			result = move(child_readers[0]);
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			result_type = LogicalType(LogicalTypeId::LIST, {make_pair("", result_type)});
			return make_unique<ListColumnReader>(result_type, s_ele, this_idx, max_define, max_repeat, move(result));
		}
		return result;
	} else { // leaf node
		// TODO check return value of derive type or should we only do this on read()
		return ColumnReader::CreateReader(derive_type(s_ele), s_ele, next_file_idx++, max_define, max_repeat);
	}
}

ParquetReader::ParquetReader(ClientContext &context, string file_name_, vector<LogicalType> expected_types,
                             string initial_filename)
    : file_name(move(file_name_)), context(context) {
	auto &fs = FileSystem::GetFileSystem(context);

	// todo move this gunk to separate function so this is cleaned up a bit
	auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);

	ResizeableBuffer buf;
	buf.resize(4);
	memset(buf.ptr, '\0', 4);
	// check for magic bytes at start of file
	fs.Read(*handle, buf.ptr, 4);
	if (strncmp(buf.ptr, "PAR1", 4) != 0) {
		throw FormatException("Missing magic bytes in front of Parquet file");
	}

	// check for magic bytes at end of file
	auto file_size_signed = fs.GetFileSize(*handle);
	if (file_size_signed < 12) {
		throw FormatException("File too small to be a Parquet file");
	}
	auto file_size = (uint64_t)file_size_signed;
	fs.Read(*handle, buf.ptr, 4, file_size - 4);
	if (strncmp(buf.ptr, "PAR1", 4) != 0) {
		throw FormatException("No magic bytes found at end of file");
	}

	// read four-byte footer length from just before the end magic bytes
	fs.Read(*handle, buf.ptr, 4, file_size - 8);
	auto footer_len = *(uint32_t *)buf.ptr;
	if (footer_len <= 0) {
		throw FormatException("Footer length can't be 0");
	}
	if (file_size < 12 + footer_len) {
		throw FormatException("Footer length %d is too big for the file of size %d", footer_len, file_size);
	}

	// centrally create thrift transport/protocol to reduce allocation
	shared_ptr<DuckdbFileTransport> trans(new DuckdbFileTransport(fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ)));
	thrift_file_proto = make_unique<apache::thrift::protocol::TCompactProtocolT<DuckdbFileTransport>>(trans);

	// If object cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	auto metadata_pos = file_size - (footer_len + 8);
	if (!ObjectCache::ObjectCacheEnabled(context)) {
		metadata = load_metadata(*thrift_file_proto, metadata_pos);
	} else {
		metadata =
		    std::dynamic_pointer_cast<ParquetFileMetadataCache>(ObjectCache::GetObjectCache(context).Get(file_name));
		if (!metadata || (fs.GetLastModifiedTime(*handle) + 10 >= metadata->read_time)) {
			metadata = load_metadata(*thrift_file_proto, metadata_pos);
			ObjectCache::GetObjectCache(context).Put(file_name, metadata);
		}
	}

	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		throw FormatException("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw FormatException("Need at least one column in the file");
	}

	//    file_meta_data->printTo(std::cout);
	//    std::cout << '\n';

	//	// TODO
	//	for (auto &s_ele : file_meta_data->schema) {
	//		s_ele.printTo(std::cout);
	//		std::cout << '\n';
	//	}

	//	if (file_meta_data->schema[0].num_children != (int32_t)(file_meta_data->schema.size() - 1)) {
	//		throw FormatException("Only flat tables are supported (no nesting)");
	//	}

	//	this->return_types = expected_types;
	bool has_expected_types = expected_types.size() > 0;

	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	// auto type = derive_type_complex(file_meta_data->schema, next_child);
	// printf("XX %s %llu\n", type.ToString().c_str(), next_child);

	//	next_child = 0;
	this->root_reader = create_reader(file_meta_data->schema, 0, 0, 0, next_schema_idx, next_file_idx);
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	D_ASSERT(next_file_idx == file_meta_data->row_groups[0].columns.size());

	/*

	   // skip the first column its the root and otherwise useless
	   for (uint64_t col_idx = 1; col_idx < file_meta_data->schema.size(); col_idx++) {
	       auto &s_ele = file_meta_data->schema[col_idx];
	       if (!s_ele.__isset.type || s_ele.num_children > 0) {
	           throw FormatException("Only flat tables are supported (no nesting)");
	       }
	       // if this is REQUIRED, there are no defined levels in file
	       // if field is REPEATED, no bueno
	       if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
	           throw FormatException("REPEATED fields are not supported");
	       }
	       auto type = derive_type(s_ele);

	       if (has_expected_types) {
	           if (return_types[col_idx - 1] != type) {
	               if (initial_filename.empty()) {
	                   throw FormatException("column \"%s\" in parquet file is of type %s, could not auto cast to "
	                                         "expected type %s for this column",
	                                         s_ele.name, type.ToString(), return_types[col_idx - 1].ToString());
	               } else {
	                   throw FormatException("schema mismatch in Parquet glob: column \"%s\" in parquet file is of type
	   "
	                                         "%s, but in the original file \"%s\" this column is of type \"%s\"",
	                                         s_ele.name, type.ToString(), initial_filename,
	                                         return_types[col_idx - 1].ToString());
	               }
	           }
	       } else {
	           names.push_back(s_ele.name);
	           return_types.push_back(type);
	       }
	   } */
	// TODO deal with expected types again
	D_ASSERT(!has_expected_types);
	auto root_type = root_reader->Type();
	D_ASSERT(root_type.id() == LogicalTypeId::STRUCT);
	for (auto &type_pair : root_type.child_types()) {
		names.push_back(type_pair.first);
		return_types.push_back(type_pair.second);
	}
}

ParquetReader::~ParquetReader() {
}

const FileMetaData *ParquetReader::GetFileMetadata() {
	D_ASSERT(metadata);
	D_ASSERT(metadata->metadata);
	return metadata->metadata.get();
}

// TODO move to column reader
struct ValueIsValid {
	template <class T> static bool Operation(T value) {
		return true;
	}
};

template <> bool ValueIsValid::Operation(float value) {
	return Value::FloatIsValid(value);
}

template <> bool ValueIsValid::Operation(double value) {
	return Value::DoubleIsValid(value);
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(LogicalType &type, column_t column_index,
                                                         const FileMetaData *file_meta_data) {
	unique_ptr<BaseStatistics> column_stats;

	for (auto &row_group : file_meta_data->row_groups) {

		D_ASSERT(column_index < row_group.columns.size());
		auto &column_chunk = row_group.columns[column_index];
		auto &s_ele = file_meta_data->schema[column_index + 1];

		auto chunk_stats = parquet_transform_column_statistics(s_ele, type, column_chunk);

		if (!column_stats) {
			column_stats = move(chunk_stats);
		} else {
			column_stats->Merge(*chunk_stats);
		}
	}
	return column_stats;
}

const RowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	D_ASSERT(state.group_idx_list[state.current_group] >= 0 &&
	         state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
	return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
}

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t file_col_idx, LogicalType &type) {
	auto &group = GetGroup(state);

	//    group.printTo(std::cout);
	//	std::cout << "\n";
	// FIXME this needs to be handled differently. Perhaps we need to annotate types with the physical column id?
	auto &chunk = group.columns[file_col_idx];

	if (chunk.__isset.file_path) {
		throw FormatException("Only inlined data files are supported (no references)");
	}

	// TODO can we assume the columns are in the right order here?
	// perhaps keep schema paths around

	//	if (chunk.meta_data.path_in_schema.size() != 1) {
	//		throw FormatException("Only flat tables are supported (no nesting)");
	//	}

	/* FIXME
	if (state.filters) {
	    auto stats = parquet_transform_column_statistics(schema, type, chunk);
	    auto filter_entry = state.filters->filters.find(file_col_idx);
	    if (stats && filter_entry != state.filters->filters.end()) {
	        bool skip_chunk = false;
	        switch (type.id()) {
	        case LogicalTypeId::INTEGER:
	        case LogicalTypeId::BIGINT:
	        case LogicalTypeId::FLOAT:
	        case LogicalTypeId::TIMESTAMP:
	        case LogicalTypeId::DOUBLE: {
	            auto num_stats = (NumericStatistics &)*stats;
	            for (auto &filter : filter_entry->second) {
	                skip_chunk = !num_stats.CheckZonemap(filter.comparison_type, filter.constant);
	                if (skip_chunk) {
	                    break;
	                }
	            }
	            break;
	        }
	        case LogicalTypeId::BLOB:
	        case LogicalTypeId::VARCHAR: {
	            auto str_stats = (StringStatistics &)*stats;
	            for (auto &filter : filter_entry->second) {
	                skip_chunk = !str_stats.CheckZonemap(filter.comparison_type, filter.constant.str_value);
	                if (skip_chunk) {
	                    break;
	                }
	            }
	            break;
	        }
	        default:
	            // D_ASSERT(0);
	            break;
	            // TODO handle structs and lists here
	        }
	        if (skip_chunk) {
	            state.group_offset = group.num_rows;
	            return;
	            // this effectively will skip this chunk
	        }
	    }
	}
*/

	// FIXME move to constructor or Initialize
	state.column_readers[file_col_idx] = ((StructColumnReader *)root_reader.get())->GetChildReader(file_col_idx);
	state.column_readers[file_col_idx]->IntializeRead(group.columns, *thrift_file_proto);

	// TODO figure out what to do wrt file io
	// auto &fs = FileSystem::GetFileSystem(context);
	// auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);

	return;
}

idx_t ParquetReader::NumRows() {
	return GetFileMetadata()->num_rows;
}

idx_t ParquetReader::NumRowGroups() {
	return GetFileMetadata()->row_groups.size();
}

void ParquetReader::Initialize(ParquetReaderScanState &state, vector<column_t> column_ids, vector<idx_t> groups_to_read,
                               TableFilterSet *filters) {
	state.current_group = -1;
	state.finished = false;
	state.column_ids = move(column_ids);
	state.group_offset = 0;
	state.group_idx_list = move(groups_to_read);
	state.filters = filters;
	state.column_readers.resize(return_types.size());
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
}

template <class T, class OP>
void templated_filter_operation2(Vector &v, T constant, parquet_filter_t &filter_mask, idx_t count) {
	D_ASSERT(v.vector_type == VectorType::FLAT_VECTOR); // we just created the damn thing it better be

	auto v_ptr = FlatVector::GetData<T>(v);
	auto &nullmask = FlatVector::Nullmask(v);

	if (nullmask.any()) {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && !(nullmask)[i] && OP::Operation(v_ptr[i], constant);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
		}
	}
}

template <class OP>
static void templated_filter_operation(Vector &v, Value &constant, parquet_filter_t &filter_mask, idx_t count) {
	if (filter_mask.none() || count == 0) {
		return;
	}
	switch (v.type.id()) {
	case LogicalTypeId::BOOLEAN:
		templated_filter_operation2<bool, OP>(v, constant.value_.boolean, filter_mask, count);
		break;

	case LogicalTypeId::INTEGER:
		templated_filter_operation2<int32_t, OP>(v, constant.value_.integer, filter_mask, count);
		break;

	case LogicalTypeId::BIGINT:
		templated_filter_operation2<int64_t, OP>(v, constant.value_.bigint, filter_mask, count);
		break;

	case LogicalTypeId::FLOAT:
		templated_filter_operation2<float, OP>(v, constant.value_.float_, filter_mask, count);
		break;

	case LogicalTypeId::DOUBLE:
		templated_filter_operation2<double, OP>(v, constant.value_.double_, filter_mask, count);
		break;

	case LogicalTypeId::TIMESTAMP:
		templated_filter_operation2<timestamp_t, OP>(v, constant.value_.bigint, filter_mask, count);
		break;

	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		templated_filter_operation2<string_t, OP>(v, string_t(constant.str_value), filter_mask, count);
		break;

	default:
		throw NotImplementedException("Unsupported type for filter %s", v.ToString());
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
			auto file_col_idx = state.column_ids[out_col_idx];

			// this is a special case where we are not interested in the actual contents of the file
			if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}

			// TODO we know this type, and dont have to pass it down. but we should verify.
			PrepareRowGroupBuffer(state, file_col_idx, result.GetTypes()[out_col_idx]);
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

	ResizeableBuffer define_buf;
	ResizeableBuffer repeat_buf;
	define_buf.resize(this_output_chunk_rows);
	repeat_buf.resize(this_output_chunk_rows);
	define_buf.zero();
	repeat_buf.zero();

	auto define_ptr = (uint8_t *)define_buf.ptr;
	auto repeat_ptr = (uint8_t *)repeat_buf.ptr;

	if (state.filters) {
		vector<bool> need_to_read(result.ColumnCount(), true);

		// first load the columns that are used in filters
		for (auto &filter_col : state.filters->filters) {
			if (filter_mask.none()) { // if no rows are left we can stop checking filters
				break;
			}

			auto file_col_idx = state.column_ids[filter_col.first];
			state.column_readers[file_col_idx]->Read(result.size(), filter_mask, define_ptr, repeat_ptr,
			                                         result.data[filter_col.first]);

			need_to_read[filter_col.first] = false;

			for (auto &filter : filter_col.second) {
				switch (filter.comparison_type) {
				case ExpressionType::COMPARE_EQUAL:
					templated_filter_operation<Equals>(result.data[filter_col.first], filter.constant, filter_mask,
					                                   this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_LESSTHAN:
					templated_filter_operation<LessThan>(result.data[filter_col.first], filter.constant, filter_mask,
					                                     this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					templated_filter_operation<LessThanEquals>(result.data[filter_col.first], filter.constant,
					                                           filter_mask, this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_GREATERTHAN:
					templated_filter_operation<GreaterThan>(result.data[filter_col.first], filter.constant, filter_mask,
					                                        this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					templated_filter_operation<GreaterThanEquals>(result.data[filter_col.first], filter.constant,
					                                              filter_mask, this_output_chunk_rows);
					break;
				default:
					D_ASSERT(0);
				}
			}
		}

		// we still may have to read some cols
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			if (need_to_read[out_col_idx]) {
				auto file_col_idx = state.column_ids[out_col_idx];
				// TODO handle ROWID here, too
				state.column_readers[file_col_idx]->Read(result.size(), filter_mask, define_ptr, repeat_ptr,
				                                         result.data[out_col_idx]);
			}
		}

		idx_t sel_size = 0;
		for (idx_t i = 0; i < this_output_chunk_rows; i++) {
			if (filter_mask[i]) {
				state.sel.set_index(sel_size++, i);
			}
		}

		result.Slice(state.sel, sel_size);
		result.Verify();

	} else { // just fricking load the data
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			auto file_col_idx = state.column_ids[out_col_idx];

			if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				Value constant_42 = Value::BIGINT(42);
				result.data[out_col_idx].Reference(constant_42);
				continue;
			}

			state.column_readers[file_col_idx]->Read(result.size(), filter_mask, define_ptr, repeat_ptr,
			                                         result.data[out_col_idx]);
		}
	}

	state.group_offset += this_output_chunk_rows;
	return true; // thank you scan again
}

} // namespace duckdb
