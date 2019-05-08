#include "execution/order_index.hpp"

#include "common/exception.hpp"
#include "common/types/static_vector.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/constant_expression.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

template <class T> struct SortChunk {
	T value;
	uint64_t row_id;
};

template <class T> static uint64_t templated_get_tuple_size() {
	return sizeof(SortChunk<T>);
}

static uint64_t GetTupleSize(TypeId type) {
	switch (type) {
	case TypeId::TINYINT:
		return templated_get_tuple_size<int8_t>();
	case TypeId::SMALLINT:
		return templated_get_tuple_size<int16_t>();
	case TypeId::INTEGER:
		return templated_get_tuple_size<int32_t>();
	case TypeId::BIGINT:
		return templated_get_tuple_size<int64_t>();
	case TypeId::DOUBLE:
		return templated_get_tuple_size<double>();
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

OrderIndex::OrderIndex(DataTable &table, vector<column_t> column_ids, vector<TypeId> types,
                       vector<TypeId> expression_types, vector<unique_ptr<Expression>> expressions,
                       uint64_t initial_capacity)
    : Index(IndexType::ORDER_INDEX), table(table), column_ids(column_ids), types(types), expressions(move(expressions)),
      tuple_size(0), data(nullptr), count(0), capacity(0) {
	// size of tuple is size of column id plus size of types
	tuple_size = GetTupleSize(types[0]);
	// initialize the data
	if (initial_capacity < STANDARD_VECTOR_SIZE) {
		initial_capacity = STANDARD_VECTOR_SIZE;
	}
	capacity = initial_capacity;
	data = unique_ptr<uint8_t[]>(new uint8_t[capacity * tuple_size]);

	expression_result.Initialize(expression_types);
}

template <class T>
static uint64_t binary_search(SortChunk<T> *array, T key, uint64_t lower, uint64_t upper, bool &found) {
	found = false;
	while (lower <= upper) {
		uint64_t middle = (lower + upper) / 2;
		auto middle_element = array[middle].value;

		if (middle_element < key) {
			lower = middle + 1;
		} else if (middle_element > key) {
			upper = middle - 1;
		} else {
			found = true;
			return middle;
		}
	}
	return upper;
}

template <class T> static uint64_t binary_search_lt(uint8_t *data, T key, uint64_t count) {
	auto array = (SortChunk<T> *)data;
	bool found = false;
	uint64_t pos = binary_search(array, key, 0, count, found);
	if (found) {
		while (pos > 0 && array[pos].value == key) {
			pos--;
		}
		if (pos != 0)
			pos++;
		return pos;
	} else {
		return pos;
	}
}

template <class T> static uint64_t binary_search_gt(uint8_t *data, T key, uint64_t count) {
	auto array = (SortChunk<T> *)data;
	bool found = false;
	uint64_t pos = binary_search(array, key, 0, count, found);
	while (pos > 0 && array[pos].value == key) {
		pos++;
	}
	if (!found)
		pos++;
	return pos;
}

template <class T> int64_t binary_search_lte(uint8_t *data, T key, uint64_t count) {
	auto array = (SortChunk<T> *)data;
	bool found = false;
	auto pos_orig = binary_search(array, key, 0, count, found);
	assert(pos_orig <= numeric_limits<int32_t>::max());
	int32_t pos = (int32_t)pos_orig;
	while (array[pos].value <= key && (uint64_t)pos < count)
		pos++;
	return pos;
}

template <class T> int64_t binary_search_gte(uint8_t *data, T key, uint64_t count) {
	auto array = (SortChunk<T> *)data;
	bool found = false;
	auto pos_orig = binary_search(array, key, 0, count, found);
	assert(pos_orig <= numeric_limits<uint32_t>::max());
	int32_t pos = (int32_t)pos_orig;
	if (found) {
		while (pos >= 0 && array[pos].value == key)
			pos--;
		pos++;
	} else {
		pos++;
	}
	return pos;
}

uint64_t OrderIndex::SearchLTE(Value value) {
	assert(value.type == types[0]);
	switch (types[0]) {
	case TypeId::TINYINT:
		return binary_search_lte<int8_t>(data.get(), value.value_.tinyint, count);
	case TypeId::SMALLINT:
		return binary_search_lte<int16_t>(data.get(), value.value_.smallint, count);
	case TypeId::INTEGER:
		return binary_search_lte<int32_t>(data.get(), value.value_.integer, count);
	case TypeId::BIGINT:
		return binary_search_lte<int64_t>(data.get(), value.value_.bigint, count);
	case TypeId::FLOAT:
		return binary_search_lte<float>(data.get(), value.value_.float_, count);
	case TypeId::DOUBLE:
		return binary_search_lte<double>(data.get(), value.value_.double_, count);
	default:
		throw NotImplementedException("Unimplemented type for index search");
	}
}

uint64_t OrderIndex::SearchGTE(Value value) {
	assert(value.type == types[0]);
	switch (types[0]) {
	case TypeId::TINYINT:
		return binary_search_gte<int8_t>(data.get(), value.value_.tinyint, count);
	case TypeId::SMALLINT:
		return binary_search_gte<int16_t>(data.get(), value.value_.smallint, count);
	case TypeId::INTEGER:
		return binary_search_gte<int32_t>(data.get(), value.value_.integer, count);
	case TypeId::BIGINT:
		return binary_search_gte<int64_t>(data.get(), value.value_.bigint, count);
	case TypeId::FLOAT:
		return binary_search_gte<float>(data.get(), value.value_.float_, count);
	case TypeId::DOUBLE:
		return binary_search_gte<double>(data.get(), value.value_.double_, count);
	default:
		throw NotImplementedException("Unimplemented type for index search");
	}
}

uint64_t OrderIndex::SearchLT(Value value) {
	assert(value.type == types[0]);
	switch (types[0]) {
	case TypeId::TINYINT:
		return binary_search_lt<int8_t>(data.get(), value.value_.tinyint, count);
	case TypeId::SMALLINT:
		return binary_search_lt<int16_t>(data.get(), value.value_.smallint, count);
	case TypeId::INTEGER:
		return binary_search_lt<int32_t>(data.get(), value.value_.integer, count);
	case TypeId::BIGINT:
		return binary_search_lt<int64_t>(data.get(), value.value_.bigint, count);
	case TypeId::FLOAT:
		return binary_search_lt<float>(data.get(), value.value_.float_, count);
	case TypeId::DOUBLE:
		return binary_search_lt<double>(data.get(), value.value_.double_, count);
	default:
		throw NotImplementedException("Unimplemented type for index search");
	}
}

uint64_t OrderIndex::SearchGT(Value value) {
	assert(value.type == types[0]);
	switch (types[0]) {
	case TypeId::TINYINT:
		return binary_search_gt<int8_t>(data.get(), value.value_.tinyint, count);
	case TypeId::SMALLINT:
		return binary_search_gt<int16_t>(data.get(), value.value_.smallint, count);
	case TypeId::INTEGER:
		return binary_search_gt<int32_t>(data.get(), value.value_.integer, count);
	case TypeId::BIGINT:
		return binary_search_gt<int64_t>(data.get(), value.value_.bigint, count);
	case TypeId::FLOAT:
		return binary_search_gt<float>(data.get(), value.value_.float_, count);
	case TypeId::DOUBLE:
		return binary_search_gt<double>(data.get(), value.value_.double_, count);
	default:
		throw NotImplementedException("Unimplemented type for index search");
	}
}

template <class T> static uint64_t templated_scan(uint64_t &from, uint64_t &to, uint8_t *data, int64_t *result_ids) {
	auto array = (SortChunk<T> *)data;
	uint64_t result_count = 0;
	for (; from < to; from++) {
		result_ids[result_count++] = array[from].row_id;
		if (result_count == STANDARD_VECTOR_SIZE) {
			from++;
			break;
		}
	}
	return result_count;
}

void OrderIndex::Scan(uint64_t &position_from, uint64_t &position_to, Value value, Vector &result_identifiers) {
	assert(result_identifiers.type == TypeId::BIGINT);
	auto row_ids = (int64_t *)result_identifiers.data;
	// perform the templated scan to find the tuples to extract
	switch (types[0]) {
	case TypeId::TINYINT:
		result_identifiers.count = templated_scan<int8_t>(position_from, position_to, data.get(), row_ids);
		break;
	case TypeId::SMALLINT:
		result_identifiers.count = templated_scan<int16_t>(position_from, position_to, data.get(), row_ids);
		break;
	case TypeId::INTEGER:
		result_identifiers.count = templated_scan<int32_t>(position_from, position_to, data.get(), row_ids);
		break;
	case TypeId::BIGINT:
		result_identifiers.count = templated_scan<int64_t>(position_from, position_to, data.get(), row_ids);
		break;
	case TypeId::DOUBLE:
		result_identifiers.count = templated_scan<double>(position_from, position_to, data.get(), row_ids);
		break;
	default:
		throw NotImplementedException("Unimplemented type for index scan");
	}
}

unique_ptr<IndexScanState> OrderIndex::InitializeScanSinglePredicate(Transaction &transaction,
                                                                     vector<column_t> column_ids, Value value,
                                                                     ExpressionType expression_type) {
	auto result = make_unique<OrderIndexScanState>(column_ids);
	// search inside the index for the constant value
	if (expression_type == ExpressionType::COMPARE_EQUAL) {
		result->current_index = SearchGTE(value);
		result->final_index = SearchLTE(value);
	} else if (expression_type == ExpressionType::COMPARE_GREATERTHAN) {
		result->current_index = SearchGT(value);
		result->final_index = count;
	} else if (expression_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		result->current_index = SearchGTE(value);
		result->final_index = count;
	} else if (expression_type == ExpressionType::COMPARE_LESSTHAN) {
		result->current_index = 0;
		result->final_index = SearchLT(value);
	} else if (expression_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		result->current_index = 0;
		result->final_index = SearchLTE(value);
	}
	return move(result);
}

unique_ptr<IndexScanState> OrderIndex::InitializeScanTwoPredicates(Transaction &transaction,
                                                                   vector<column_t> column_ids, Value low_value,
                                                                   ExpressionType low_expression_type, Value high_value,
                                                                   ExpressionType high_expression_type) {
	auto result = make_unique<OrderIndexScanState>(column_ids);
	assert(low_expression_type == ExpressionType::COMPARE_GREATERTHAN ||
	       low_expression_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
	assert(high_expression_type == ExpressionType::COMPARE_LESSTHAN ||
	       high_expression_type == ExpressionType::COMPARE_LESSTHANOREQUALTO);
	// search inside the index for the constant value
	if (low_expression_type == ExpressionType::COMPARE_GREATERTHAN)
		result->current_index = SearchGT(low_value);
	else if (low_expression_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO)
		result->current_index = SearchGTE(low_value);
	if (high_expression_type == ExpressionType::COMPARE_LESSTHAN)
		result->final_index = SearchLT(high_value);
	else if (high_expression_type == ExpressionType::COMPARE_LESSTHANOREQUALTO)
		result->final_index = SearchLTE(high_value);
	return move(result);
}

void OrderIndex::Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) {
	auto state = (OrderIndexScanState *)ss;
	// scan the index
	StaticVector<int64_t> result_identifiers;
	do {
		Scan(state->current_index, state->final_index, state->value, result_identifiers);
		if (result_identifiers.count == 0) {
			return;
		}
		// now go to the base table to fetch the tuples
		table.Fetch(transaction, result, state->column_ids, result_identifiers);
	} while (result_identifiers.count == 0);
}

template <class T> static void templated_insert(uint8_t *dataptr, DataChunk &input, Vector &row_ids) {
	auto actual_data = (SortChunk<T> *)dataptr;
	auto input_data = (T *)input.data[0].data;
	auto row_identifiers = (int64_t *)row_ids.data;
	for (uint64_t i = 0; i < row_ids.count; i++) {
		actual_data[i].value = input_data[i];
		actual_data[i].row_id = row_identifiers[i];
	}
}

static void insert_data(uint8_t *dataptr, DataChunk &input, Vector &row_ids) {
	switch (input.data[0].type) {
	case TypeId::TINYINT:
		templated_insert<int8_t>(dataptr, input, row_ids);
		break;
	case TypeId::SMALLINT:
		templated_insert<int16_t>(dataptr, input, row_ids);
		break;
	case TypeId::INTEGER:
		templated_insert<int32_t>(dataptr, input, row_ids);
		break;
	case TypeId::BIGINT:
		templated_insert<int64_t>(dataptr, input, row_ids);
		break;
	case TypeId::DOUBLE:
		templated_insert<double>(dataptr, input, row_ids);
		break;
	default:
		throw InvalidTypeException(input.data[0].type, "Invalid type for index");
	}
}

void OrderIndex::Insert(DataChunk &input, Vector &row_ids) {
	if (input.column_count > 1) {
		throw NotImplementedException("We only support single dimensional indexes currently");
	}
	assert(row_ids.type == TypeId::BIGINT);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);

	if (count + row_ids.count >= capacity) {
		// have to allocate new structure to make room for new entries
		capacity *= 2;
		auto new_data = unique_ptr<uint8_t[]>(new uint8_t[capacity * tuple_size]);
		// copy the old data
		memcpy(new_data.get(), data.get(), count * tuple_size);
		data = move(new_data);
	}

	// now insert the entries
	insert_data(data.get() + count * tuple_size, input, row_ids);
	count += row_ids.count;
}

template <class T> static void templated_sort(uint8_t *dataptr, uint64_t count) {
	auto actual_data = (SortChunk<T> *)dataptr;
	sort(actual_data, actual_data + count,
	     [](const SortChunk<T> &a, const SortChunk<T> &b) -> bool { return a.value < b.value; });
}

void OrderIndex::Sort() {
	switch (types[0]) {
	case TypeId::TINYINT:
		templated_sort<int8_t>(data.get(), count);
		break;
	case TypeId::SMALLINT:
		templated_sort<int16_t>(data.get(), count);
		break;
	case TypeId::INTEGER:
		templated_sort<int32_t>(data.get(), count);
		break;
	case TypeId::BIGINT:
		templated_sort<int64_t>(data.get(), count);
		break;
	case TypeId::DOUBLE:
		templated_sort<double>(data.get(), count);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void OrderIndex::Append(ClientContext &context, DataChunk &appended_data, uint64_t row_identifier_start) {
	lock_guard<mutex> l(lock);

	// first resolve the expressions
	ExpressionExecutor executor(appended_data);
	executor.Execute(expressions, expression_result);

	// create the row identifiers
	StaticVector<uint64_t> row_identifiers;
	auto row_ids = (uint64_t *)row_identifiers.data;
	row_identifiers.count = appended_data.size();
	for (uint64_t i = 0; i < row_identifiers.count; i++) {
		row_ids[i] = row_identifier_start + i;
	}

	Insert(expression_result, row_identifiers);
	// finally sort the index again
	Sort();
}

void OrderIndex::Update(ClientContext &context, vector<column_t> &update_columns, DataChunk &update_data,
                        Vector &row_identifiers) {
	// first check if the columns we use here are updated
	bool index_is_updated = false;
	for (auto &column : update_columns) {
		if (find(column_ids.begin(), column_ids.end(), column) != column_ids.end()) {
			index_is_updated = true;
			break;
		}
	}
	if (!index_is_updated) {
		// none of the indexed columns are updated
		// we can ignore the update
		return;
	}
	// otherwise we need to change the data inside the index
	lock_guard<mutex> l(lock);

	// first we recreate an insert chunk using the updated data
	// NOTE: this assumes update_data contains ALL columns used by the index
	// FIXME this method is ugly, but if we use a different DataChunk the column
	// references will not be aligned properly
	DataChunk temp_chunk;
	temp_chunk.Initialize(table.types);
	temp_chunk.data[0].count = update_data.size();
	for (uint64_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		bool found_column = false;
		for (uint64_t j = 0; i < update_columns.size(); j++) {
			if (column_ids[i] == update_columns[j]) {
				temp_chunk.data[column_ids[i]].Reference(update_data.data[update_columns[j]]);
				found_column = true;
				break;
			}
		}
		assert(found_column);
	}

	// now resolve the expressions on the temp_chunk
	ExpressionExecutor executor(temp_chunk);
	executor.Execute(expressions, expression_result);

	// insert the expression result
	Insert(expression_result, row_identifiers);

	// finally sort the index again
	Sort();
}

template <class T> void templated_print(uint8_t *dataptr, uint64_t count) {
	auto actual_data = (SortChunk<T> *)dataptr;
	for (uint64_t i = 0; i < count; i++) {
		cout << "[" << actual_data[i].value << " - " << actual_data[i].row_id << "]"
		     << "\n";
	}
}

void OrderIndex::Print() {
	switch (types[0]) {
	case TypeId::TINYINT:
		templated_print<int8_t>(data.get(), count);
		break;
	case TypeId::SMALLINT:
		templated_print<int16_t>(data.get(), count);
		break;
	case TypeId::INTEGER:
		templated_print<int32_t>(data.get(), count);
		break;
	case TypeId::BIGINT:
		templated_print<int64_t>(data.get(), count);
		break;
	case TypeId::DOUBLE:
		templated_print<double>(data.get(), count);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}
