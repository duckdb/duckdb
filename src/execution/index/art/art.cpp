#include "execution/index/art/art.hpp"

using namespace duckdb;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types,
                       vector<TypeId> expression_types, vector<unique_ptr<Expression>> expressions,
                       size_t initial_capacity, vector<unique_ptr<Expression>> unbinded_expressions)
        : Index(IndexType::ORDER_INDEX), table(table), column_ids(column_ids), types(types), expressions(move(expressions)),
          unbinded_expressions(move(unbinded_expressions)), data(nullptr) {
    tree= NULL;
    expression_result.Initialize(expression_types);
}


void ART::Insert(DataChunk &input, Vector &row_ids) {
    if (input.column_count > 1) {
        throw NotImplementedException("We only support single dimensional indexes currently");
    }
    assert(row_ids.type == TypeId::POINTER);
    assert(input.size() == row_ids.count);
    assert(types[0] == input.data[0].type);

    // now insert the entries
    insert_data(data.get() + count * tuple_size, input, row_ids);
    count += row_ids.count;
}