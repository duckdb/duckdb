#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk() {
}

void DataChunk::InitializeEmpty(vector<TypeId> &types) {
	assert(types.size() > 0);
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(*this, types[i], nullptr));
	}
}

void DataChunk::Initialize(vector<TypeId> &types) {
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
	return data[col_idx].GetValue(sel_vector ? sel_vector[index] : index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, Value val) {
	data[col_idx].SetValue(sel_vector ? sel_vector[index] : index, move(val));
}

void DataChunk::Reference(DataChunk &chunk) {
	assert(chunk.column_count() == column_count());
	SetCardinality(chunk);
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Reference(chunk.data[i]);
	}
}

void DataChunk::Copy(DataChunk &other, idx_t offset) {
	assert(column_count() == other.column_count());
	assert(other.size() == 0 && !other.sel_vector);

	for (idx_t i = 0; i < column_count(); i++) {
		VectorOperations::Copy(data[i], other.data[i], offset);
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
	assert(!sel_vector);
	for (idx_t i = 0; i < column_count(); i++) {
		VectorOperations::Append(other.data[i], data[i]);
	}
	SetCardinality(size() + other.size());
}

void DataChunk::ClearSelectionVector() {
	Normalify();
	if (!sel_vector) {
		return;
	}

	for (idx_t i = 0; i < column_count(); i++) {
		data[i].ClearSelectionVector();
	}
	sel_vector = nullptr;
}

void DataChunk::Normalify() {
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Normalify();
	}
}

vector<TypeId> DataChunk::GetTypes() {
	vector<TypeId> types;
	for (idx_t i = 0; i < column_count(); i++) {
		types.push_back(data[i].type);
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(column_count()) + " Columns]\n";
	for (idx_t i = 0; i < column_count(); i++) {
		retval += "- " + data[i].ToString() + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) {
	// write the count
	serializer.Write<sel_t>(size());
	serializer.Write<idx_t>(column_count());
	for (idx_t i = 0; i < column_count(); i++) {
		// write the types
		serializer.Write<int>((int)data[i].type);
	}
	// write the data
	for (idx_t i = 0; i < column_count(); i++) {
		auto type = data[i].type;
		if (TypeIsConstantSize(type)) {
			idx_t write_size = GetTypeIdSize(type) * size();
			auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
			// constant size type: simple memcpy
			VectorOperations::CopyToStorage(data[i], ptr.get());
			serializer.WriteData(ptr.get(), write_size);
		} else {
			assert(type == TypeId::VARCHAR);
			// strings are inlined into the blob
			// we use null-padding to store them
			auto strings = (string_t *)data[i].data;
			VectorOperations::Exec(sel_vector, size(), [&](idx_t j, idx_t k) {
				auto source = !data[i].nullmask[j] ? strings[j].GetData() : NullValue<const char *>();
				serializer.WriteString(source);
			});
		}
	}
}

void DataChunk::Deserialize(Deserializer &source) {
	auto rows = source.Read<sel_t>();
	idx_t column_count = source.Read<idx_t>();

	vector<TypeId> types;
	for (idx_t i = 0; i < column_count; i++) {
		types.push_back((TypeId)source.Read<int>());
	}
	Initialize(types);
	// now load the column data
	SetCardinality(rows);
	for (idx_t i = 0; i < column_count; i++) {
		auto type = data[i].type;
		if (TypeIsConstantSize(type)) {
			// constant size type: simple memcpy
			auto column_size = GetTypeIdSize(type) * rows;
			auto ptr = unique_ptr<data_t[]>(new data_t[column_size]);
			source.ReadData(ptr.get(), column_size);
			Vector v(*this, data[i].type, ptr.get());
			VectorOperations::ReadFromStorage(v, data[i]);
		} else {
			auto strings = (string_t *)data[i].data;
			for (idx_t j = 0; j < rows; j++) {
				// read the strings
				auto str = source.Read<string>();
				// now add the string to the StringHeap of the vector
				// and write the pointer into the vector
				if (IsNullValue<const char *>((const char *)str.c_str())) {
					data[i].nullmask[j] = true;
				} else {
					strings[j] = data[i].AddString(str);
				}
			}
		}
	}
	Verify();
}

void DataChunk::MoveStringsToHeap(StringHeap &heap) {
	for (idx_t c = 0; c < column_count(); c++) {
		if (data[c].type == TypeId::VARCHAR) {
			// move strings of this chunk to the specified heap
			auto source_strings = (string_t *)data[c].GetData();
			auto old_buffer = move(data[c].buffer);
			if (data[c].vector_type == VectorType::CONSTANT_VECTOR) {
				data[c].buffer = VectorBuffer::CreateConstantVector(TypeId::VARCHAR);
				data[c].data = data[c].buffer->GetData();
				auto target_strings = (string_t *)data[c].GetData();
				if (!data[c].nullmask[0] && !source_strings[0].IsInlined()) {
					target_strings[0] = heap.AddString(source_strings[0]);
				} else {
					target_strings[0] = source_strings[0];
				}
			} else {
				data[c].buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
				data[c].data = data[c].buffer->GetData();
				auto target_strings = (string_t *)data[c].GetData();
				VectorOperations::Exec(data[c], [&](idx_t i, idx_t k) {
					if (!data[c].nullmask[i] && !source_strings[i].IsInlined()) {
						target_strings[i] = heap.AddString(source_strings[i]);
					} else {
						target_strings[i] = source_strings[i];
					}
				});
			}
		}
	}
}

void DataChunk::Hash(Vector &result) {
	assert(result.type == TypeId::HASH);
	VectorOperations::Hash(data[0], result);
	for (idx_t i = 1; i < column_count(); i++) {
		VectorOperations::CombineHash(result, data[i]);
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	assert(size() <= STANDARD_VECTOR_SIZE);
	// verify that all vectors in this chunk have the chunk selection vector
	sel_t *v = sel_vector;
	for (idx_t i = 0; i < column_count(); i++) {
		assert(data[i].sel_vector() == v);
		data[i].Verify();
	}
	// verify that all vectors in the chunk have the same count
	for (idx_t i = 0; i < column_count(); i++) {
		assert(size() == data[i].size());
	}
#endif
}

void DataChunk::Print() {
	Printer::Print(ToString());
}
