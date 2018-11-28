
#include "storage/order_index.hpp"

#include "common/exception.hpp"
#include "common/types/static_vector.hpp"

#include "execution/expression_executor.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

template <class T> struct SortChunk {
	T value;
	uint64_t column_id;
};

template <class T> static size_t templated_get_tuple_size() {
	return sizeof(SortChunk<T>);
}

static size_t GetTupleSize(TypeId type) {
	switch (type) {
	case TypeId::TINYINT:
		return templated_get_tuple_size<int8_t>();
	case TypeId::SMALLINT:
		return templated_get_tuple_size<int16_t>();
	case TypeId::DATE:
	case TypeId::INTEGER:
		return templated_get_tuple_size<int32_t>();
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		return templated_get_tuple_size<int64_t>();
	case TypeId::DECIMAL:
		return templated_get_tuple_size<double>();
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

OrderIndex::OrderIndex(DataTable &table, vector<column_t> column_ids,
                       std::vector<TypeId> types,
                       std::vector<TypeId> expression_types,
                       vector<unique_ptr<Expression>> expressions,
                       size_t initial_capacity)
    : table(table), column_ids(column_ids), types(types),
      expressions(move(expressions)), tuple_size(0), data(nullptr), count(0),
      capacity(0) {
	// size of tuple is size of column id plus size of types
	tuple_size = GetTupleSize(types[0]);
	// initialize the data
	if (initial_capacity < STANDARD_VECTOR_SIZE) {
		initial_capacity = STANDARD_VECTOR_SIZE;
	}
	capacity = initial_capacity;
	data = unique_ptr<uint8_t[]>(new uint8_t[capacity]);

	expression_result.Initialize(expression_types);
}

template <class T>
static void templated_insert(uint8_t *dataptr, DataChunk &input,
                             Vector &row_ids) {
	auto actual_data = (SortChunk<T> *)dataptr;
	auto input_data = (T *)input.data[0].data;
	auto column_ids = (uint64_t *)row_ids.data;
	for (size_t i = 0; i < row_ids.count; i++) {
		actual_data[i].value = input_data[i];
		actual_data[i].column_id = column_ids[i];
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
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_insert<int32_t>(dataptr, input, row_ids);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_insert<int64_t>(dataptr, input, row_ids);
		break;
	case TypeId::DECIMAL:
		templated_insert<double>(dataptr, input, row_ids);
		break;
	default:
		throw InvalidTypeException(input.data[0].type,
		                           "Invalid type for index");
	}
}

void OrderIndex::Insert(DataChunk &input, Vector &row_ids) {
	if (input.column_count > 1) {
		throw NotImplementedException(
		    "We only support single dimensional indexes currently");
	}
	assert(row_ids.type == TypeId::POINTER);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);

	if (count + row_ids.count >= capacity) {
		// have to allocate new structure to make room for new entries
		capacity *= 2;
		auto new_data = unique_ptr<uint8_t[]>(new uint8_t[capacity]);
		// copy the old data
		memcpy(new_data.get(), data.get(), count * tuple_size);
		data = move(new_data);
	}

	// now insert the entries
	insert_data(data.get() + count * tuple_size, input, row_ids);
	count += row_ids.count;
}

template <class T> static void templated_sort(uint8_t *dataptr, size_t count) {
	auto actual_data = (SortChunk<T> *)dataptr;
	sort(actual_data, actual_data + count,
	     [](const SortChunk<T> &a, const SortChunk<T> &b) -> bool {
		     return a.value < b.value;
	     });
}

void OrderIndex::Sort() {
	switch (types[0]) {
	case TypeId::TINYINT:
		templated_sort<int8_t>(data.get(), count);
		break;
	case TypeId::SMALLINT:
		templated_sort<int16_t>(data.get(), count);
		break;
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_sort<int32_t>(data.get(), count);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_sort<int64_t>(data.get(), count);
		break;
	case TypeId::DECIMAL:
		templated_sort<double>(data.get(), count);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void OrderIndex::Append(ClientContext &context, DataChunk &appended_data,
                        size_t row_identifier_start) {
	lock_guard<mutex> l(lock);

	// first resolve the expressions
	ExpressionExecutor executor(appended_data, context);
	executor.Execute(expressions, expression_result);

	// create the row identifiers
	StaticVector<uint64_t> row_identifiers;
	auto column_ids = (uint64_t *)row_identifiers.data;
	row_identifiers.count = appended_data.size();
	for (size_t i = 0; i < row_identifiers.count; i++) {
		column_ids[i] = row_identifier_start + i;
	}

	Insert(expression_result, row_identifiers);
	// finally sort the index again
	Sort();
}

void OrderIndex::Update(ClientContext &context,
                        vector<column_t> &update_columns,
                        DataChunk &update_data, Vector &row_identifiers) {
	// first check if the columns we use here are updated
	bool index_is_updated = false;
	for (auto &column : update_columns) {
		if (find(column_ids.begin(), column_ids.end(), column) !=
		    column_ids.end()) {
			index_is_updated = true;
			break;
		}
	}
	if (!index_is_updated) {
		// none of the indexed columns are updated
		// we can ignore the update
		return;
	}
	throw NotImplementedException("Update");

	// otherwise we need to change the data inside the index
	lock_guard<mutex> l(lock);

	// finally sort the index again
	Sort();
}

template <class T> void templated_print(uint8_t *dataptr, size_t count) {
	auto actual_data = (SortChunk<T> *)dataptr;
	for (size_t i = 0; i < count; i++) {
		cout << "[" << actual_data[i].value << " - " << actual_data[i].column_id
		     << "]"
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
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_print<int32_t>(data.get(), count);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_print<int64_t>(data.get(), count);
		break;
	case TypeId::DECIMAL:
		templated_print<double>(data.get(), count);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}
