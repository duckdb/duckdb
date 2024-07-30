#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/array.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

DataChunk::DataChunk() : count(0), capacity(STANDARD_VECTOR_SIZE) {
}

DataChunk::~DataChunk() {
}

void DataChunk::InitializeEmpty(const vector<LogicalType> &types) {
	InitializeEmpty(types.begin(), types.end());
}

void DataChunk::Initialize(Allocator &allocator, const vector<LogicalType> &types, idx_t capacity_p) {
	Initialize(allocator, types.begin(), types.end(), capacity_p);
}

void DataChunk::Initialize(ClientContext &context, const vector<LogicalType> &types, idx_t capacity_p) {
	Initialize(Allocator::Get(context), types, capacity_p);
}

idx_t DataChunk::GetAllocationSize() const {
	idx_t total_size = 0;
	auto cardinality = size();
	for (auto &vec : data) {
		total_size += vec.GetAllocationSize(cardinality);
	}
	return total_size;
}

void DataChunk::Initialize(Allocator &allocator, vector<LogicalType>::const_iterator begin,
                           vector<LogicalType>::const_iterator end, idx_t capacity_p) {
	D_ASSERT(data.empty());                   // can only be initialized once
	D_ASSERT(std::distance(begin, end) != 0); // empty chunk not allowed
	capacity = capacity_p;
	for (; begin != end; begin++) {
		VectorCache cache(allocator, *begin, capacity);
		data.emplace_back(cache);
		vector_caches.push_back(std::move(cache));
	}
}

void DataChunk::Initialize(ClientContext &context, vector<LogicalType>::const_iterator begin,
                           vector<LogicalType>::const_iterator end, idx_t capacity_p) {
	Initialize(Allocator::Get(context), begin, end, capacity_p);
}

void DataChunk::InitializeEmpty(vector<LogicalType>::const_iterator begin, vector<LogicalType>::const_iterator end) {
	capacity = STANDARD_VECTOR_SIZE;
	D_ASSERT(data.empty());                   // can only be initialized once
	D_ASSERT(std::distance(begin, end) != 0); // empty chunk not allowed
	for (; begin != end; begin++) {
		data.emplace_back(*begin, nullptr);
	}
}

void DataChunk::Reset() {
	if (data.empty() || vector_caches.empty()) {
		return;
	}
	if (vector_caches.size() != data.size()) {
		throw InternalException("VectorCache and column count mismatch in DataChunk::Reset");
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].ResetFromCache(vector_caches[i]);
	}
	capacity = STANDARD_VECTOR_SIZE;
	SetCardinality(0);
}

void DataChunk::Destroy() {
	data.clear();
	vector_caches.clear();
	capacity = 0;
	SetCardinality(0);
}

Value DataChunk::GetValue(idx_t col_idx, idx_t index) const {
	D_ASSERT(index < size());
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, const Value &val) {
	data[col_idx].SetValue(index, val);
}

bool DataChunk::AllConstant() const {
	for (auto &v : data) {
		if (v.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			return false;
		}
	}
	return true;
}

void DataChunk::Reference(DataChunk &chunk) {
	D_ASSERT(chunk.ColumnCount() <= ColumnCount());
	SetCapacity(chunk);
	SetCardinality(chunk);
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		data[i].Reference(chunk.data[i]);
	}
}

void DataChunk::Move(DataChunk &chunk) {
	SetCardinality(chunk);
	SetCapacity(chunk);
	data = std::move(chunk.data);
	vector_caches = std::move(chunk.vector_caches);

	chunk.Destroy();
}

void DataChunk::Copy(DataChunk &other, idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset, 0);
	}
	other.SetCardinality(size() - offset);
}

void DataChunk::Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count, const idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);
	D_ASSERT((offset + source_count) <= size());

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], sel, source_count, offset, 0);
	}
	other.SetCardinality(source_count - offset);
}

void DataChunk::Split(DataChunk &other, idx_t split_idx) {
	D_ASSERT(other.size() == 0);
	D_ASSERT(other.data.empty());
	D_ASSERT(split_idx < data.size());
	const idx_t num_cols = data.size();
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		other.data.push_back(std::move(data[col_idx]));
		other.vector_caches.push_back(std::move(vector_caches[col_idx]));
	}
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		data.pop_back();
		vector_caches.pop_back();
	}
	other.SetCapacity(*this);
	other.SetCardinality(*this);
}

void DataChunk::Fuse(DataChunk &other) {
	D_ASSERT(other.size() == size());
	const idx_t num_cols = other.data.size();
	for (idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		data.emplace_back(std::move(other.data[col_idx]));
		vector_caches.emplace_back(std::move(other.vector_caches[col_idx]));
	}
	other.Destroy();
}

void DataChunk::ReferenceColumns(DataChunk &other, const vector<column_t> &column_ids) {
	D_ASSERT(ColumnCount() == column_ids.size());
	Reset();
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		auto &other_col = other.data[column_ids[col_idx]];
		auto &this_col = data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	SetCardinality(other.size());
}

void DataChunk::Append(const DataChunk &other, bool resize, SelectionVector *sel, idx_t sel_count) {
	idx_t new_size = sel ? size() + sel_count : size() + other.size();
	if (other.size() == 0) {
		return;
	}
	if (ColumnCount() != other.ColumnCount()) {
		throw InternalException("Column counts of appending chunk doesn't match!");
	}
	if (new_size > capacity) {
		if (resize) {
			auto new_capacity = NextPowerOfTwo(new_size);
			for (idx_t i = 0; i < ColumnCount(); i++) {
				data[i].Resize(size(), new_capacity);
			}
			capacity = new_capacity;
		} else {
			throw InternalException("Can't append chunk to other chunk without resizing");
		}
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		if (sel) {
			VectorOperations::Copy(other.data[i], data[i], *sel, sel_count, 0, size());
		} else {
			VectorOperations::Copy(other.data[i], data[i], other.size(), 0, size());
		}
	}
	SetCardinality(new_size);
}

void DataChunk::Flatten() {
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Flatten(size());
	}
}

vector<LogicalType> DataChunk::GetTypes() const {
	vector<LogicalType> types;
	for (idx_t i = 0; i < ColumnCount(); i++) {
		types.push_back(data[i].GetType());
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(ColumnCount()) + " Columns]\n";
	for (idx_t i = 0; i < ColumnCount(); i++) {
		retval += "- " + data[i].ToString(size()) + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) const {

	// write the count
	auto row_count = size();
	serializer.WriteProperty<sel_t>(100, "rows", NumericCast<sel_t>(row_count));

	// we should never try to serialize empty data chunks
	auto column_count = ColumnCount();
	D_ASSERT(column_count);

	// write the types
	serializer.WriteList(101, "types", column_count,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(data[i].GetType()); });

	// write the data
	serializer.WriteList(102, "columns", column_count, [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &object) {
			// Reference the vector to avoid potentially mutating it during serialization
			Vector serialized_vector(data[i].GetType());
			serialized_vector.Reference(data[i]);
			serialized_vector.Serialize(object, row_count);
		});
	});
}

void DataChunk::Deserialize(Deserializer &deserializer) {

	// read and set the row count
	auto row_count = deserializer.ReadProperty<sel_t>(100, "rows");

	// read the types
	vector<LogicalType> types;
	deserializer.ReadList(101, "types", [&](Deserializer::List &list, idx_t i) {
		auto type = list.ReadElement<LogicalType>();
		types.push_back(type);
	});

	// initialize the data chunk
	D_ASSERT(!types.empty());
	Initialize(Allocator::DefaultAllocator(), types, MaxValue<idx_t>(row_count, STANDARD_VECTOR_SIZE));
	SetCardinality(row_count);

	// read the data
	deserializer.ReadList(102, "columns", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &object) { data[i].Deserialize(object, row_count); });
	});
}

void DataChunk::Slice(const SelectionVector &sel_vector, idx_t count_p) {
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < ColumnCount(); c++) {
		data[c].Slice(sel_vector, count_p, merge_cache);
	}
}

void DataChunk::Slice(const DataChunk &other, const SelectionVector &sel, idx_t count_p, idx_t col_offset) {
	D_ASSERT(other.ColumnCount() <= col_offset + ColumnCount());
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < other.ColumnCount(); c++) {
		if (other.data[c].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			data[col_offset + c].Reference(other.data[c]);
			data[col_offset + c].Slice(sel, count_p, merge_cache);
		} else {
			data[col_offset + c].Slice(other.data[c], sel, count_p);
		}
	}
}

void DataChunk::Slice(idx_t offset, idx_t slice_count) {
	D_ASSERT(offset + slice_count <= size());
	SelectionVector sel(slice_count);
	for (idx_t i = 0; i < slice_count; i++) {
		sel.set_index(i, offset + i);
	}
	Slice(sel, slice_count);
}

unsafe_unique_array<UnifiedVectorFormat> DataChunk::ToUnifiedFormat() {
	auto unified_data = make_unsafe_uniq_array<UnifiedVectorFormat>(ColumnCount());
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].ToUnifiedFormat(size(), unified_data[col_idx]);
	}
	return unified_data;
}

void DataChunk::Hash(Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	VectorOperations::Hash(data[0], result, size());
	for (idx_t i = 1; i < ColumnCount(); i++) {
		VectorOperations::CombineHash(result, data[i], size());
	}
}

void DataChunk::Hash(vector<idx_t> &column_ids, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	D_ASSERT(!column_ids.empty());

	VectorOperations::Hash(data[column_ids[0]], result, size());
	for (idx_t i = 1; i < column_ids.size(); i++) {
		VectorOperations::CombineHash(result, data[column_ids[i]], size());
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	D_ASSERT(size() <= capacity);

	// verify that all vectors in this chunk have the chunk selection vector
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Verify(size());
	}

	if (!ColumnCount()) {
		// don't try to round-trip dummy data chunks with no data
		// e.g., these exist in queries like 'SELECT distinct(col0, col1) FROM tbl', where we have groups, but no
		// payload so the payload will be such an empty data chunk
		return;
	}

	// verify that we can round-trip chunk serialization
	MemoryStream mem_stream;
	BinarySerializer serializer(mem_stream);

	serializer.Begin();
	Serialize(serializer);
	serializer.End();

	mem_stream.Rewind();

	BinaryDeserializer deserializer(mem_stream);
	DataChunk new_chunk;

	deserializer.Begin();
	new_chunk.Deserialize(deserializer);
	deserializer.End();

	D_ASSERT(size() == new_chunk.size());
#endif
}

void DataChunk::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
