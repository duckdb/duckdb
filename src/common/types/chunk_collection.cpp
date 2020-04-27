#include "duckdb/common/types/chunk_collection.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/assert.hpp"

#include <algorithm>
#include <cstring>

using namespace duckdb;
using namespace std;

void ChunkCollection::Verify() {
#ifdef DEBUG
	for (auto &chunk : chunks) {
		chunk->Verify();
	}
#endif
}

void ChunkCollection::Append(ChunkCollection &other) {
	for (auto &chunk : other.chunks) {
		Append(*chunk.get());
	}
}

void ChunkCollection::Append(DataChunk &new_chunk) {
	if (new_chunk.size() == 0) {
		return;
	}
	new_chunk.Verify();

	// we have to ensure that every chunk in the ChunkCollection is completely
	// filled, otherwise our O(1) lookup in GetValue and SetValue does not work
	// first fill the latest chunk, if it exists
	count += new_chunk.size();

	idx_t remaining_data = new_chunk.size();
	idx_t offset = 0;
	if (chunks.size() == 0) {
		// first chunk
		types = new_chunk.GetTypes();
	} else {
		// the types of the new chunk should match the types of the previous one
		assert(types.size() == new_chunk.column_count());
		auto new_types = new_chunk.GetTypes();
		for (idx_t i = 0; i < types.size(); i++) {
			if (new_types[i] != types[i]) {
				throw TypeMismatchException(new_types[i], types[i], "Type mismatch when combining rows");
			}
			if (types[i] == TypeId::LIST) {
				for (auto &chunk :
				     chunks) { // need to check all the chunks because they can have only-null list entries
					auto &chunk_vec = chunk->data[i];
					auto &new_vec = new_chunk.data[i];
					if (ListVector::HasEntry(chunk_vec) && ListVector::HasEntry(new_vec)) {
						auto &chunk_types = ListVector::GetEntry(chunk_vec).types;
						auto &new_types = ListVector::GetEntry(new_vec).types;
						if (chunk_types.size() > 0 && new_types.size() > 0 && chunk_types != new_types) {
							throw TypeMismatchException(chunk_types[0], new_types[i],
							                            "Type mismatch when combining lists");
						}
					}
				}
			}
			// TODO check structs, too
		}

		// first append data to the current chunk
		DataChunk &last_chunk = *chunks.back();
		idx_t added_data = std::min(remaining_data, (idx_t)(STANDARD_VECTOR_SIZE - last_chunk.size()));
		if (added_data > 0) {
			// copy <added_data> elements to the last chunk
			idx_t old_count = new_chunk.size();
			new_chunk.SetCardinality(added_data);

			last_chunk.Append(new_chunk);
			remaining_data -= added_data;
			// reset the chunk to the old data
			new_chunk.SetCardinality(old_count);
			offset = added_data;
		}
	}

	if (remaining_data > 0) {
		// create a new chunk and fill it with the remainder
		auto chunk = make_unique<DataChunk>();
		chunk->Initialize(types);
		new_chunk.Copy(*chunk, offset);
		chunks.push_back(move(chunk));
	}
}

// returns an int similar to a C comparator:
// -1 if left < right
// 0 if left == right
// 1 if left > right

template <class TYPE>
static int8_t templated_compare_value(Vector &left_vec, Vector &right_vec, idx_t left_idx, idx_t right_idx) {
	assert(left_vec.type == right_vec.type);
	auto left_val = FlatVector::GetData<TYPE>(left_vec)[left_idx];
	auto right_val = FlatVector::GetData<TYPE>(right_vec)[right_idx];
	if (Equals::Operation<TYPE>(left_val, right_val)) {
		return 0;
	}
	if (LessThan::Operation<TYPE>(left_val, right_val)) {
		return -1;
	}
	return 1;
}

// return type here is int32 because strcmp() on some platforms returns rather large values
static int32_t compare_value(Vector &left_vec, Vector &right_vec, idx_t vector_idx_left, idx_t vector_idx_right) {
	auto left_null = FlatVector::Nullmask(left_vec)[vector_idx_left];
	auto right_null = FlatVector::Nullmask(right_vec)[vector_idx_right];

	if (left_null && right_null) {
		return 0;
	} else if (right_null) {
		return 1;
	} else if (left_null) {
		return -1;
	}

	switch (left_vec.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return templated_compare_value<int8_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::INT16:
		return templated_compare_value<int16_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::INT32:
		return templated_compare_value<int32_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::INT64:
		return templated_compare_value<int64_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::FLOAT:
		return templated_compare_value<float>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::DOUBLE:
		return templated_compare_value<double>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case TypeId::VARCHAR:
		return templated_compare_value<string_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	default:
		throw NotImplementedException("Type for comparison");
	}
	return false;
}

static int compare_tuple(ChunkCollection *sort_by, vector<OrderType> &desc, idx_t left, idx_t right) {
	assert(sort_by);

	idx_t chunk_idx_left = left / STANDARD_VECTOR_SIZE;
	idx_t chunk_idx_right = right / STANDARD_VECTOR_SIZE;
	idx_t vector_idx_left = left % STANDARD_VECTOR_SIZE;
	idx_t vector_idx_right = right % STANDARD_VECTOR_SIZE;

	auto &left_chunk = sort_by->chunks[chunk_idx_left];
	auto &right_chunk = sort_by->chunks[chunk_idx_right];

	for (idx_t col_idx = 0; col_idx < desc.size(); col_idx++) {
		auto order_type = desc[col_idx];

		Vector &left_vec = left_chunk->data[col_idx];
		Vector &right_vec = right_chunk->data[col_idx];

		assert(left_vec.vector_type == VectorType::FLAT_VECTOR);
		assert(right_vec.vector_type == VectorType::FLAT_VECTOR);
		assert(left_vec.type == right_vec.type);

		auto comp_res = compare_value(left_vec, right_vec, vector_idx_left, vector_idx_right);

		if (comp_res == 0) {
			continue;
		}
		return comp_res < 0 ? (order_type == OrderType::ASCENDING ? -1 : 1)
		                    : (order_type == OrderType::ASCENDING ? 1 : -1);
	}
	return 0;
}

static int64_t _quicksort_initial(ChunkCollection *sort_by, vector<OrderType> &desc, idx_t *result) {
	// select pivot
	int64_t pivot = 0;
	int64_t low = 0, high = sort_by->count - 1;
	// now insert elements
	for (idx_t i = 1; i < sort_by->count; i++) {
		if (compare_tuple(sort_by, desc, i, pivot) <= 0) {
			result[low++] = i;
		} else {
			result[high--] = i;
		}
	}
	assert(low == high);
	result[low] = pivot;
	return low;
}

static void _quicksort_inplace(ChunkCollection *sort_by, vector<OrderType> &desc, idx_t *result, int64_t left,
                               int64_t right) {
	if (left >= right) {
		return;
	}

	int64_t middle = left + (right - left) / 2;
	int64_t pivot = result[middle];
	// move the mid point value to the front.
	int64_t i = left + 1;
	int64_t j = right;

	std::swap(result[middle], result[left]);
	while (i <= j) {
		while (i <= j && compare_tuple(sort_by, desc, result[i], pivot) <= 0) {
			i++;
		}

		while (i <= j && compare_tuple(sort_by, desc, result[j], pivot) > 0) {
			j--;
		}

		if (i < j) {
			std::swap(result[i], result[j]);
		}
	}
	std::swap(result[i - 1], result[left]);
	int64_t part = i - 1;

	_quicksort_inplace(sort_by, desc, result, left, part - 1);
	_quicksort_inplace(sort_by, desc, result, part + 1, right);
}

void ChunkCollection::Sort(vector<OrderType> &desc, idx_t result[]) {
	assert(result);
	if (count == 0)
		return;
	// quicksort
	int64_t part = _quicksort_initial(this, desc, result);
	_quicksort_inplace(this, desc, result, 0, part);
	_quicksort_inplace(this, desc, result, part + 1, count - 1);
}

// FIXME make this more efficient by not using the Value API
// just use memcpy in the vectors
// assert that there is no selection list
void ChunkCollection::Reorder(idx_t order_org[]) {
	auto order = unique_ptr<idx_t[]>(new idx_t[count]);
	memcpy(order.get(), order_org, sizeof(idx_t) * count);

	// adapted from https://stackoverflow.com/a/7366196/2652376

	auto val_buf = vector<Value>();
	val_buf.resize(column_count());

	idx_t j, k;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
			val_buf[col_idx] = GetValue(col_idx, i);
		}
		j = i;
		while (true) {
			k = order[j];
			order[j] = j;
			if (k == i) {
				break;
			}
			for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
				SetValue(col_idx, j, GetValue(col_idx, k));
			}
			j = k;
		}
		for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
			SetValue(col_idx, j, val_buf[col_idx]);
		}
	}
}

template <class TYPE>
static void templated_set_values(ChunkCollection *src_coll, Vector &tgt_vec, idx_t order[], idx_t col_idx,
                                 idx_t start_offset, idx_t remaining_data) {
	assert(src_coll);

	for (idx_t row_idx = 0; row_idx < remaining_data; row_idx++) {
		idx_t chunk_idx_src = order[start_offset + row_idx] / STANDARD_VECTOR_SIZE;
		idx_t vector_idx_src = order[start_offset + row_idx] % STANDARD_VECTOR_SIZE;

		auto &src_chunk = src_coll->chunks[chunk_idx_src];
		Vector &src_vec = src_chunk->data[col_idx];
		auto source_data = FlatVector::GetData<TYPE>(src_vec);
		auto target_data = FlatVector::GetData<TYPE>(tgt_vec);

		if (FlatVector::IsNull(src_vec, vector_idx_src)) {
			FlatVector::SetNull(tgt_vec, row_idx, true);
		} else {
			target_data[row_idx] = source_data[vector_idx_src];
		}
	}
}

// TODO: reorder functionality is similar, perhaps merge
void ChunkCollection::MaterializeSortedChunk(DataChunk &target, idx_t order[], idx_t start_offset) {
	idx_t remaining_data = min((idx_t)STANDARD_VECTOR_SIZE, count - start_offset);
	assert(target.GetTypes() == types);

	target.SetCardinality(remaining_data);
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		switch (types[col_idx]) {
		case TypeId::BOOL:
		case TypeId::INT8:
			templated_set_values<int8_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT16:
			templated_set_values<int16_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT32:
			templated_set_values<int32_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT64:
			templated_set_values<int64_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::FLOAT:
			templated_set_values<float>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::DOUBLE:
			templated_set_values<double>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::VARCHAR:
			templated_set_values<string_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;

		case TypeId::LIST:
		case TypeId::STRUCT: {
			for (idx_t row_idx = 0; row_idx < remaining_data; row_idx++) {
				idx_t chunk_idx_src = order[start_offset + row_idx] / STANDARD_VECTOR_SIZE;
				idx_t vector_idx_src = order[start_offset + row_idx] % STANDARD_VECTOR_SIZE;

				auto &src_chunk = chunks[chunk_idx_src];
				Vector &src_vec = src_chunk->data[col_idx];
				auto &tgt_vec = target.data[col_idx];
				if (FlatVector::IsNull(src_vec, vector_idx_src)) {
					FlatVector::SetNull(tgt_vec, row_idx, true);
				} else {
					tgt_vec.SetValue(row_idx, src_vec.GetValue(vector_idx_src));
				}
			}
		} break;
		default:
			throw NotImplementedException("Type is unsupported in MaterializeSortedChunk()");
		}
	}
	target.Verify();
}

Value ChunkCollection::GetValue(idx_t column, idx_t index) {
	return chunks[LocateChunk(index)]->GetValue(column, index % STANDARD_VECTOR_SIZE);
}

vector<Value> ChunkCollection::GetRow(idx_t index) {
	vector<Value> values;
	values.resize(column_count());

	for (idx_t p_idx = 0; p_idx < column_count(); p_idx++) {
		values[p_idx] = GetValue(p_idx, index);
	}
	return values;
}

void ChunkCollection::SetValue(idx_t column, idx_t index, Value value) {
	chunks[LocateChunk(index)]->SetValue(column, index % STANDARD_VECTOR_SIZE, value);
}

void ChunkCollection::Print() {
	Printer::Print(ToString());
}

bool ChunkCollection::Equals(ChunkCollection &other) {
	if (count != other.count) {
		return false;
	}
	if (column_count() != other.column_count()) {
		return false;
	}
	if (types != other.types) {
		return false;
	}
	// if count is equal amount of chunks should be equal
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
			auto lvalue = GetValue(col_idx, row_idx);
			auto rvalue = other.GetValue(col_idx, row_idx);
			if (!Value::ValuesAreEqual(lvalue, rvalue)) {
				return false;
			}
		}
	}
	return true;
}
static void _heapify(ChunkCollection *input, vector<OrderType> &desc, idx_t *heap, idx_t heap_size,
                     idx_t current_index) {
	if (current_index >= heap_size) {
		return;
	}
	idx_t left_child_index = current_index * 2 + 1;
	idx_t right_child_index = current_index * 2 + 2;
	idx_t swap_index = current_index;

	if (left_child_index < heap_size) {
		swap_index =
		    compare_tuple(input, desc, heap[swap_index], heap[left_child_index]) <= 0 ? left_child_index : swap_index;
	}

	if (right_child_index < heap_size) {
		swap_index =
		    compare_tuple(input, desc, heap[swap_index], heap[right_child_index]) <= 0 ? right_child_index : swap_index;
	}

	if (swap_index != current_index) {
		std::swap(heap[current_index], heap[swap_index]);
		_heapify(input, desc, heap, heap_size, swap_index);
	}
}

static void _heap_create(ChunkCollection *input, vector<OrderType> &desc, idx_t *heap, idx_t heap_size) {
	for (idx_t i = 0; i < heap_size; i++) {
		heap[i] = i;
	}

	// build heap
	for (int64_t i = heap_size / 2 - 1; i >= 0; i--) {
		_heapify(input, desc, heap, heap_size, i);
	}

	// Run through all the rows.
	for (idx_t i = heap_size; i < input->count; i++) {
		if (compare_tuple(input, desc, i, heap[0]) <= 0) {
			heap[0] = i;
			_heapify(input, desc, heap, heap_size, 0);
		}
	}
}

void ChunkCollection::Heap(vector<OrderType> &desc, idx_t heap[], idx_t heap_size) {
	assert(heap);
	if (count == 0)
		return;

	_heap_create(this, desc, heap, heap_size);

	// Heap is ready. Now do a heapsort
	for (int64_t i = heap_size - 1; i >= 0; i--) {
		std::swap(heap[i], heap[0]);
		_heapify(this, desc, heap, i, 0);
	}
}

idx_t ChunkCollection::MaterializeHeapChunk(DataChunk &target, idx_t order[], idx_t start_offset, idx_t heap_size) {
	idx_t remaining_data = min((idx_t)STANDARD_VECTOR_SIZE, heap_size - start_offset);
	assert(target.GetTypes() == types);

	target.SetCardinality(remaining_data);
	for (idx_t col_idx = 0; col_idx < column_count(); col_idx++) {
		switch (types[col_idx]) {
		case TypeId::BOOL:
		case TypeId::INT8:
			templated_set_values<int8_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT16:
			templated_set_values<int16_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT32:
			templated_set_values<int32_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::INT64:
			templated_set_values<int64_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::FLOAT:
			templated_set_values<float>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::DOUBLE:
			templated_set_values<double>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
		case TypeId::VARCHAR:
			templated_set_values<string_t>(this, target.data[col_idx], order, col_idx, start_offset, remaining_data);
			break;
			// TODO this is ugly and sloooow!
		case TypeId::STRUCT:
		case TypeId::LIST: {
			for (idx_t row_idx = 0; row_idx < remaining_data; row_idx++) {
				idx_t chunk_idx_src = order[start_offset + row_idx] / STANDARD_VECTOR_SIZE;
				idx_t vector_idx_src = order[start_offset + row_idx] % STANDARD_VECTOR_SIZE;

				auto &src_chunk = chunks[chunk_idx_src];
				Vector &src_vec = src_chunk->data[col_idx];
				auto &tgt_vec = target.data[col_idx];
				if (FlatVector::IsNull(src_vec, vector_idx_src)) {
					FlatVector::SetNull(tgt_vec, row_idx, true);
				} else {
					tgt_vec.SetValue(row_idx, src_vec.GetValue(vector_idx_src));
				}
			}
		} break;

		default:
			throw NotImplementedException("Type is unsupported in MaterializeHeapChunk()");
		}
	}
	target.Verify();
	return remaining_data;
}
