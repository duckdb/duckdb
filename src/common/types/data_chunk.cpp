
#include "common/types/data_chunk.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector_operations.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk()
    : count(0), column_count(0), data(nullptr), sel_vector(nullptr) {
}

void DataChunk::Initialize(std::vector<TypeId> &types, bool zero_data) {
	count = 0;
	column_count = types.size();
	size_t size = 0;
	for (auto &type : types) {
		size += GetTypeIdSize(type) * STANDARD_VECTOR_SIZE;
	}
	if (size > 0) {
		owned_data = unique_ptr<char[]>(new char[size]);
		if (zero_data) {
			memset(owned_data.get(), 0, size);
		}
	}

	char *ptr = owned_data.get();
	data = unique_ptr<Vector[]>(new Vector[types.size()]);
	for (size_t i = 0; i < types.size(); i++) {
		data[i].type = types[i];
		data[i].data = ptr;
		data[i].count = 0;
		data[i].sel_vector = nullptr;

		ptr += GetTypeIdSize(types[i]) * STANDARD_VECTOR_SIZE;
	}
}

void DataChunk::Reset() {
	count = 0;
	char *ptr = owned_data.get();
	for (size_t i = 0; i < column_count; i++) {
		data[i].data = ptr;
		data[i].count = 0;
		data[i].sel_vector = nullptr;
		data[i].owned_data = nullptr;
		data[i].string_heap.Destroy();
		ptr += GetTypeIdSize(data[i].type) * STANDARD_VECTOR_SIZE;
	}
	sel_vector = nullptr;
}

void DataChunk::Destroy() {
	data = nullptr;
	owned_data.reset();
	sel_vector = nullptr;
	column_count = 0;
	count = 0;
}

void DataChunk::Copy(DataChunk &other, size_t offset) {
	assert(column_count == other.column_count);
	other.sel_vector = nullptr;

	for (size_t i = 0; i < column_count; i++) {
		data[i].Copy(other.data[i], offset);
	}
	other.count = other.data[0].count;
}

void DataChunk::Move(DataChunk &other) {
	other.column_count = column_count;
	other.count = count;
	other.data = move(data);
	other.owned_data = move(owned_data);
	if (sel_vector) {
		other.sel_vector = sel_vector;
		if (sel_vector == owned_sel_vector) {
			// copy the owned selection vector
			memcpy(other.owned_sel_vector, owned_sel_vector,
			       STANDARD_VECTOR_SIZE * sizeof(sel_t));
		}
	}
	Destroy();
}

void DataChunk::Flatten() {
	if (!sel_vector) {
		return;
	}

	for (size_t i = 0; i < column_count; i++) {
		data[i].Flatten();
	}
	sel_vector = nullptr;
}

void DataChunk::Append(DataChunk &other) {
	if (other.count == 0) {
		return;
	}
	if (column_count != other.column_count) {
		throw OutOfRangeException(
		    "Column counts of appending chunk doesn't match!");
	}
	for (size_t i = 0; i < column_count; i++) {
		if (other.data[i].type != data[i].type) {
			throw TypeMismatchException(data[i].type, other.data[i].type,
			                            "Column types do not match!");
		}
	}
	if (count + other.count > STANDARD_VECTOR_SIZE) {
		throw OutOfRangeException(
		    "Count of chunk cannot exceed STANDARD_VECTOR_SIZE!");
	}
	for (size_t i = 0; i < column_count; i++) {
		data[i].Append(other.data[i]);
	}
	count += other.count;
}

void DataChunk::MergeSelVector(sel_t *current_vector, sel_t *new_vector,
                               sel_t *result, size_t new_count) {
	for (size_t i = 0; i < new_count; i++) {
		result[i] = current_vector[new_vector[i]];
	}
}

void DataChunk::SetSelectionVector(Vector &matches) {
	if (matches.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(
		    matches.type,
		    "Can only set selection vector using a boolean vector!");
	}
	bool *match_data = (bool *)matches.data;
	size_t match_count = 0;
	if (sel_vector) {
		assert(matches.sel_vector);
		// existing selection vector: have to merge the selection vector
		for (size_t i = 0; i < matches.count; i++) {
			if (match_data[sel_vector[i]] && !matches.nullmask[sel_vector[i]]) {
				owned_sel_vector[match_count++] = sel_vector[i];
			}
		}
		sel_vector = owned_sel_vector;
	} else {
		// no selection vector yet: can just set the selection vector
		for (size_t i = 0; i < matches.count; i++) {
			if (match_data[i] && !matches.nullmask[i]) {
				owned_sel_vector[match_count++] = i;
			}
		}
		if (match_count < matches.count) {
			// we only have to set the selection vector if tuples were filtered
			sel_vector = owned_sel_vector;
		}
	}
	count = match_count;
	for (size_t i = 0; i < column_count; i++) {
		data[i].count = this->count;
		data[i].sel_vector = sel_vector;
	}
}

vector<TypeId> DataChunk::GetTypes() {
	vector<TypeId> types;
	for (size_t i = 0; i < column_count; i++) {
		types.push_back(data[i].type);
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(column_count) + " Columns]\n";
	for (size_t i = 0; i < column_count; i++) {
		retval += "- " + data[i].ToString() + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) {
	// write the count
	serializer.Write<sel_t>(count);
	serializer.Write<uint64_t>(column_count);
	for (size_t i = 0; i < column_count; i++) {
		// write the types
		serializer.Write<int>((int)data[i].type);
	}
	// write the data
	for (size_t i = 0; i < column_count; i++) {
		auto type = data[i].type;
		if (TypeIsConstantSize(type)) {
			auto ptr = serializer.ManualWrite(GetTypeIdSize(type) * count);
			// constant size type: simple memcpy
			VectorOperations::CopyNull(data[i], ptr);
		} else {
			assert(type == TypeId::VARCHAR);
			// strings are inlined into the blob
			// we use null-padding to store them
			const char **strings = (const char **)data[i].data;
			for (size_t j = 0; j < count; j++) {
				auto source =
				    strings[j] ? strings[j] : NullValue<const char *>();
				serializer.WriteString(source);
			}
		}
	}
}

void DataChunk::Deserialize(Deserializer &source) {
	auto rows = source.Read<sel_t>();
	column_count = source.Read<uint64_t>();

	std::vector<TypeId> types;
	for (size_t i = 0; i < column_count; i++) {
		types.push_back((TypeId)source.Read<int>());
	}
	Initialize(types);
	// now load the column data
	for (size_t i = 0; i < column_count; i++) {
		auto type = data[i].type;
		if (TypeIsConstantSize(type)) {
			// constant size type: simple memcpy
			auto column_size = GetTypeIdSize(type) * rows;
			auto ptr = source.ReadData(column_size);
			Vector v(data[i].type, (char *)ptr);
			v.count = rows;
			VectorOperations::AppendNull(v, data[i]);
		} else {
			const char **strings = (const char **)data[i].data;
			for (size_t j = 0; j < rows; j++) {
				// read the strings
				auto str = source.Read<string>();
				// now add the string to the StringHeap of the vector
				// and write the pointer into the vector
				if (IsNullValue<const char *>((const char *)str.c_str())) {
					strings[j] = nullptr;
					data[i].nullmask[j] = true;
				} else {
					strings[j] = data[i].string_heap.AddString(str);
				}
			}
		}
		data[i].count = rows;
	}
	count = rows;
	Verify();
}

#ifdef DEBUG
void DataChunk::Verify() {
	// verify that all vectors in this chunk have the chunk selection vector
	sel_t *v = sel_vector;
	for (size_t i = 0; i < column_count; i++) {
		assert(data[i].sel_vector == v);
		data[i].Verify();
	}
	// verify that all vectors in the chunk have the same count
	for (size_t i = 0; i < column_count; i++) {
		assert(count == data[i].count);
	}
}
#endif
