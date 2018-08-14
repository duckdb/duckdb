
#include "common/types/data_chunk.hpp"
#include "common/types/vector_operations.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

DataChunk::DataChunk()
    : count(0), column_count(0), maximum_size(0), data(nullptr) {}

DataChunk::~DataChunk() {}

void DataChunk::Initialize(std::vector<TypeId> &types, oid_t maximum_chunk_size,
                           bool zero_data) {
	maximum_size = maximum_chunk_size;
	count = 0;
	column_count = types.size();
	oid_t size = 0;
	for (auto &type : types) {
		size += GetTypeIdSize(type) * maximum_size;
	}
	if (size > 0) {
		owned_data = unique_ptr<char[]>(new char[size]);
		if (zero_data) {
			memset(owned_data.get(), 0, size);
		}
	}

	char *ptr = owned_data.get();
	data = unique_ptr<Vector[]>(new Vector[types.size()]);
	for (oid_t i = 0; i < types.size(); i++) {
		data[i].type = types[i];
		data[i].data = ptr;
		data[i].maximum_size = maximum_size;
		data[i].count = 0;
		data[i].sel_vector = nullptr;
		if (data[i].type == TypeId::VARCHAR) {
			auto string_list = new unique_ptr<char[]>[maximum_size];
			data[i].owned_strings =
			    unique_ptr<unique_ptr<char[]>[]>(string_list);
		}

		ptr += GetTypeIdSize(types[i]) * maximum_size;
	}
}

void DataChunk::Reset() {
	count = 0;
	char *ptr = owned_data.get();
	for (oid_t i = 0; i < column_count; i++) {
		data[i].data = ptr;
		data[i].owns_data = false;
		data[i].count = 0;
		data[i].sel_vector = nullptr;
		ptr += GetTypeIdSize(data[i].type) * maximum_size;
	}
	sel_vector.reset();
}

void DataChunk::Destroy() {
	data = nullptr;
	owned_data.reset();
	column_count = 0;
	count = 0;
	maximum_size = 0;
}

void DataChunk::ForceOwnership() {
	char *ptr = owned_data.get();
	for (oid_t i = 0; i < column_count;
	     ptr += GetTypeIdSize(data[i].type) * maximum_size, i++) {
		if (data[i].sel_vector) {
			data[i].ForceOwnership();
		} else {
			if (data[i].owns_data)
				continue;
			if (data[i].data == ptr)
				continue;

			data[i].ForceOwnership();
		}
	}
}

void DataChunk::Append(DataChunk &other) {
	if (other.count == 0) {
		return;
	}
	if (column_count != other.column_count) {
		throw Exception("Column counts of appending chunk doesn't match!");
	}
	for (oid_t i = 0; i < column_count; i++) {
		if (other.data[i].type != data[i].type) {
			throw Exception("Column types do not match!");
		}
	}
	if (count + other.count > maximum_size) {
		// resize
		maximum_size = maximum_size * 2;
		for (oid_t i = 0; i < column_count; i++) {
			data[i].Resize(maximum_size);
		}
	}
	for (oid_t i = 0; i < column_count; i++) {
		data[i].Append(other.data[i]);
	}
	count += other.count;
}

bool DataChunk::HasConsistentSelVector() {
	if (column_count == 0) return true;
	sel_t *v = sel_vector.get();
	for(size_t i = 0; i < column_count; i++) {
		if (data[i].sel_vector != v) {
			return false;
		}
	}
	return true;
}

bool DataChunk::HasSelVector() {
	if (column_count == 0) return true;
	for(size_t i = 0; i < column_count; i++) {
		if (data[i].sel_vector) {
			return true;
		}
	}
	return false;
}

void DataChunk::MergeSelVector(sel_t *current_vector, sel_t *new_vector, sel_t * result, size_t new_count) {
	for (size_t i = 0; i < new_count; i++) {
		result[i] = current_vector[new_vector[i]];
	}
}

void DataChunk::SetSelVector(unique_ptr<sel_t[]> new_vector, size_t new_count) {
	if (!new_vector) {
		sel_vector = nullptr;
	} else {
		// check if any of the columns in the chunk have a selection vector
		if (HasSelVector()) {
			if (HasConsistentSelVector()) {
				// all columns in the DataChunk point towards the same selection vector
				// merge with the current selection vector
				for(size_t i = 0; i < new_count; i++) {
					assert(new_vector[i] < this->count);
				}
				DataChunk::MergeSelVector(sel_vector.get(), new_vector.get(), new_vector.get(), new_count);
				sel_vector = move(new_vector);
			} else {
				// columns point towards different selection vectors!
				// just give up and apply the selection vectors
				for(size_t i = 0; i < column_count; i++) {
					Vector result(data[i].type, new_count);
					result.count = new_count;
					VectorOperations::ApplySelectionVector(data[i], result, new_vector.get());
					result.Move(data[i]);
				}
				sel_vector = nullptr;
			}
		} else {
			// no selection vector, just assign the new one
			sel_vector = move(new_vector);
		}
	}
	// add a reference to all child nodes
	for(size_t i = 0; i < column_count; i++) {
		data[i].count = new_count;
		data[i].sel_vector = sel_vector.get();
	}
	this->count = new_count;

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
