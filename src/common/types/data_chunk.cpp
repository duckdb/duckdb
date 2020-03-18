#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"


using namespace duckdb;
using namespace std;

DataChunk::DataChunk() : count(0) {
}

void DataChunk::InitializeEmpty(vector<TypeId> &types) {
	assert(types.size() > 0);
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(types[i], nullptr));
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
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, Value val) {
	data[col_idx].SetValue(index, move(val));
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
	assert(other.size() == 0);

	for (idx_t i = 0; i < column_count(); i++) {
		assert(other.data[i].vector_type == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset);
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
		VectorOperations::Append(other.data[i], data[i], other.size(), size());
	}
	SetCardinality(size() + other.size());
}

void DataChunk::Normalify() {
	for (idx_t i = 0; i < column_count(); i++) {
		data[i].Normalify(size());
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
		serializer.Write<int>((int)data[col_idx].type);
	}
	// write the data
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		data[col_idx].Normalify(size());
		auto type = data[col_idx].type;
		if (TypeIsConstantSize(type)) {
			idx_t write_size = GetTypeIdSize(type) * size();
			auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
			// constant size type: simple memcpy
			VectorOperations::CopyToStorage(data[col_idx], ptr.get());
			serializer.WriteData(ptr.get(), write_size);
		} else {
			assert(type == TypeId::VARCHAR);
			// strings are inlined into the blob
			// we use null-padding to store them
			auto strings = FlatVector::GetData<string_t>(data[col_idx]);
			for(idx_t row_idx = 0; row_idx < size(); row_idx++) {
				auto source = !FlatVector::IsNull(data[col_idx], row_idx) ? strings[row_idx].GetData() : NullValue<const char *>();
				serializer.WriteString(source);
			}
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
			Vector v(data[i].type, ptr.get());
			VectorOperations::ReadFromStorage(v, data[i], rows);
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
					strings[j] = StringVector::AddString(data[i], str);
				}
			}
		}
	}
	Verify();
}

void DataChunk::SetCardinality(idx_t count, SelectionVector &sel_vector) {
	this->count = count;
	for(idx_t c = 0; c < column_count(); c++) {
		if (data[c].vector_type == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			throw NotImplementedException("FIXME merge dictionary");
		} else {
			data[c].Slice(sel_vector);
		}
	}
}

void DataChunk::MoveStringsToHeap(StringHeap &heap) {
	for (idx_t c = 0; c < column_count(); c++) {
		if (data[c].type == TypeId::VARCHAR) {
			StringVector::MoveStringsToHeap(data[c], size(), heap);
		}
	}
}

void DataChunk::Hash(Vector &result) {
	assert(result.type == TypeId::HASH);
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
