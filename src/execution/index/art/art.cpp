#include "execution/index/art/art.hpp"

using namespace duckdb;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
            vector<unique_ptr<Expression>> expressions,vector<unique_ptr<Expression>> unbound_expressions)
        : Index(IndexType::ART,move(expressions),move(unbound_expressions)), table(table), column_ids(column_ids), types(types) {
    tree = NULL;
    expression_result.Initialize(expression_types);
    int n = 1;
    // little endian if true
    if (*(char *) &n == 1) {
        is_little_endian = true;


    }
}


//TODO: Suppport  FLOAT = float32_t DOUBLE =  float64_t ,VARCHAR = 9, char, representing a null-terminated UTF-8 string
void ART::Insert(DataChunk &input, Vector &row_ids) {
    if (input.column_count > 1) {
        throw NotImplementedException("We only support single dimensional indexes currently");
    }
    assert(row_ids.type == TypeId::POINTER);
    assert(input.size() == row_ids.count);
    assert(types[0] == input.data[0].type);
    switch (input.data[0].type) {
        case TypeId:: BOOLEAN:
        case TypeId:: TINYINT:
            templated_insert<int8_t,uint8_t>(input, row_ids);
            break;
        case TypeId:: SMALLINT:
            templated_insert<int16_t,uint16_t>(input, row_ids);
            break;
        case TypeId::INTEGER:
            templated_insert<int32_t,uint32_t>(input, row_ids);
            break;
        case TypeId::BIGINT:
            templated_insert<int64_t,uint64_t>(input, row_ids);
            break;
        default:
            throw InvalidTypeException(input.data[0].type, "Invalid type for index");
    }
}
