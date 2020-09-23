#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
using namespace std;

DataChunk::DataChunk() : count(0) {
}

void DataChunk::InitializeEmpty(vector<LogicalType> &types) {
	assert(types.size() > 0);
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(types[i], nullptr));
	}
}

void DataChunk::Initialize(vector<LogicalType> &types) {
	assert(types.size() > 0);
	InitializeEmpty(types);
	for (idx_t i = 0; i < types.size(); i++) {
		data[i].Initialize();
	}
}

void DataChunk::Reset() {
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Initialize();
	}
	SetCardinality(0);
}

void DataChunk::Destroy() {
	data.clear();
	SetCardinality(0);
}

Value DataChunk::GetValue(idx_t col_idx, idx_t index) const {
	assert(index < size());
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, Value val) {
	data[col_idx].SetValue(index, move(val));
}

void DataChunk::Reference(DataChunk &chunk) {
	assert(chunk.column_count() <= column_count());
	SetCardinality(chunk);
	for (idx_t i = 0; i < chunk.column_count(); i++) {
		data[i].Reference(chunk.data[i]);
	}
}

void DataChunk::Copy(DataChunk &other, idx_t offset) {
	assert(column_count() == other.column_count());
	assert(other.size() == 0);

	for (idx_t i = 0; i < column_count(); i++) {
		assert(other.data[i].vector_type == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset, 0);
	}
	other.SetCardinality(size() - offset);
}

void DataChunk::Append(DataChunk &other) {
	if (other.size() == 0) {
		return;
	}
	if (column_count() != other.column_count()) {
		throw OutOfRangeException("Column counts of appending chunk doesn't match!");
	}
	for (idx_t i = 0; i < column_count(); i++) {
		assert(data[i].vector_type == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(other.data[i], data[i], other.size(), 0, size());
	}
	SetCardinality(size() + other.size());
}

void DataChunk::Normalify() {
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Normalify(size());
	}
}

vector<LogicalType> DataChunk::GetTypes() {
	vector<LogicalType> types;
	for (idx_t i = 0; i < column_count(); i++) {
		types.push_back(data[i].type);
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(column_count()) + " Columns]\n";
	for (idx_t i = 0; i < column_count(); i++) {
		retval += "- " + data[i].ToString(size()) + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) {
	// write the count
	serializer.Write<sel_t>(size());
	serializer.Write<idx_t>(column_count());
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		// write the types
		data[col_idx].type.Serialize(serializer);
	}
	// write the data
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		data[col_idx].Serialize(size(), serializer);
	}
}

void DataChunk::Deserialize(Deserializer &source) {
	auto rows = source.Read<sel_t>();
	idx_t column_count = source.Read<idx_t>();

	vector<LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		types.push_back(LogicalType::Deserialize(source));
	}
	Initialize(types);
	// now load the column data
	SetCardinality(rows);
	for (idx_t i = 0; i < column_count; i++) {
		data[i].Deserialize(rows, source);
	}
	Verify();
}

void DataChunk::Slice(const SelectionVector &sel_vector, idx_t count) {
	this->count = count;
	SelCache merge_cache;
	for (idx_t c = 0; c < column_count(); c++) {
		data[c].Slice(sel_vector, count, merge_cache);
	}
}

void DataChunk::Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset) {
	assert(other.column_count() <= col_offset + column_count());
	this->count = count;
	SelCache merge_cache;
	for (idx_t c = 0; c < other.column_count(); c++) {
		if (other.data[c].vector_type == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			data[col_offset + c].Reference(other.data[c]);
			data[col_offset + c].Slice(sel, count, merge_cache);
		} else {
			data[col_offset + c].Slice(other.data[c], sel, count);
		}
	}
}

unique_ptr<VectorData[]> DataChunk::Orrify() {
	auto orrified_data = unique_ptr<VectorData[]>(new VectorData[column_count()]);
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		data[col_idx].Orrify(size(), orrified_data[col_idx]);
	}
	return orrified_data;
}

void DataChunk::Hash(Vector &result) {
	assert(result.type.id() == LogicalTypeId::HASH);
	VectorOperations::Hash(data[0], result, size());
	for (idx_t i = 1; i < column_count(); i++) {
		VectorOperations::CombineHash(result, data[i], size());
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	assert(size() <= STANDARD_VECTOR_SIZE);
	// verify that all vectors in this chunk have the chunk selection vector
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Verify(size());
	}
#endif
}

void DataChunk::Print() {
	Printer::Print(ToString());
}

struct DuckDBArrowArrayHolder {
	ArrowArray array;
	const void *buffers[3];              // need max three pointers for strings
	unique_ptr<ArrowArray *[]> children; // just space for the *pointers* to children, not the children themselves

	Vector vector;
	unique_ptr<data_t[]> string_offsets;
	unique_ptr<data_t[]> string_data;
};

static void release_duckdb_arrow_array(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = (DuckDBArrowArrayHolder *)array->private_data;
	delete holder;
}

void DataChunk::ToArrowArray(ArrowArray *out_array) {
	assert(out_array);

	auto root_holder = new DuckDBArrowArrayHolder();
	root_holder->children = unique_ptr<ArrowArray *[]>(new ArrowArray *[column_count()]);
	out_array->private_data = root_holder;
	out_array->release = release_duckdb_arrow_array;

	out_array->children = root_holder->children.get();
	out_array->length = size();
	out_array->n_children = column_count();
	out_array->n_buffers = 1;
	out_array->buffers = root_holder->buffers;
	out_array->buffers[0] = nullptr; // there is no actual buffer there since we don't have NULLs
	out_array->offset = 0;
	out_array->null_count = 0; // needs to be 0
	out_array->dictionary = nullptr;

	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		auto holder = new DuckDBArrowArrayHolder();
		holder->vector.Reference(data[col_idx]);
		auto &child = holder->array;
		auto &vector = holder->vector;
		child.private_data = holder;
		child.release = release_duckdb_arrow_array;

		child.n_children = 0;
		child.null_count = -1; // unknown
		child.offset = 0;
		child.dictionary = nullptr;
		child.buffers = holder->buffers;

		child.length = size();

		switch (vector.vector_type) {
			// TODO support other vector types
		case VectorType::FLAT_VECTOR:

			switch (GetTypes()[col_idx].id()) {
				// TODO support other data types
			case LogicalTypeId::BOOLEAN:
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
			case LogicalTypeId::HUGEINT:
				child.n_buffers = 2;
				child.buffers[1] = (void *)FlatVector::GetData(vector);
				break;

			case LogicalTypeId::DATE: {
				child.n_buffers = 2;
				child.buffers[1] = (void *)FlatVector::GetData(vector);
				auto target_ptr = (uint32_t *)child.buffers[1];
				for (idx_t row_idx = 0; row_idx < size(); row_idx++) {
					target_ptr[row_idx] = Date::EpochDays(target_ptr[row_idx]);
				}
				break;
			}

			case LogicalTypeId::VARCHAR: {
				child.n_buffers = 3;
				holder->string_offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size() + 1)]);
				child.buffers[1] = holder->string_offsets.get();
				assert(child.buffers[1]);
				// step 1: figure out total string length:
				idx_t total_string_length = 0;
				auto string_t_ptr = FlatVector::GetData<string_t>(vector);
				auto is_null = FlatVector::Nullmask(vector);
				for (idx_t row_idx = 0; row_idx < size(); row_idx++) {
					if (is_null[row_idx]) {
						continue;
					}
					total_string_length += string_t_ptr[row_idx].GetSize();
				}
				// step 2: allocate this much
				holder->string_data = unique_ptr<data_t[]>(new data_t[total_string_length]);
				child.buffers[2] = holder->string_data.get();
				assert(child.buffers[2]);
				// step 3: assign buffers
				idx_t current_heap_offset = 0;
				auto target_ptr = (uint32_t *)child.buffers[1];

				for (idx_t row_idx = 0; row_idx < size(); row_idx++) {
					target_ptr[row_idx] = current_heap_offset;
					if (is_null[row_idx]) {
						continue;
					}
					auto &str = string_t_ptr[row_idx];
					memcpy((void *)((uint8_t *)child.buffers[2] + current_heap_offset), str.GetData(), str.GetSize());
					current_heap_offset += str.GetSize();
				}
				target_ptr[size()] = current_heap_offset; // need to terminate last string!
				break;
			}
			default:
				throw runtime_error("Unsupported type " + GetTypes()[col_idx].ToString());
			}

			child.null_count = FlatVector::Nullmask(vector).count();
			child.buffers[0] = (void *)&FlatVector::Nullmask(vector).flip();

			break;
		default:
			throw NotImplementedException(VectorTypeToString(vector.vector_type));
		}
		out_array->children[col_idx] = &child;
	}
}

} // namespace duckdb
