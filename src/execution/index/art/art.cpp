#include "execution/index/art/art.hpp"

using namespace duckdb;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
            vector<unique_ptr<Expression>> expressions,vector<unique_ptr<Expression>> unbound_expressions)
        : Index(IndexType::ART,move(expressions),move(unbound_expressions)), table(table), column_ids(column_ids), types(types){
    tree= NULL;
    expression_result.Initialize(expression_types);
    int n = 1;

    // little endian if true
    if(*(char *)&n == 1) {
        is_little_endian = true;
    }

}


void ART::Insert(DataChunk &input, Vector &row_ids) {
    if (input.column_count > 1) {
        throw NotImplementedException("We only support single dimensional indexes currently");
    }
    assert(row_ids.type == TypeId::POINTER);
    assert(input.size() == row_ids.count);
    assert(types[0] == input.data[0].type);
    switch (input.data[0].type) {
        case TypeId::BIGINT:
            templated_insert<int64_t,uint64_t>(input, row_ids);
            break;
        default:
            throw InvalidTypeException(input.data[0].type, "Invalid type for index");
    }
}

//switch (column.GetType()) {
//case type::TypeId::BOOLEAN:
//case type::TypeId::TINYINT: {
//auto raw = type::ValuePeeker::PeekTinyInt(input_key.GetValue(i));
//WriteValue<int8_t>(data + offset, raw);
//offset += sizeof(int8_t);
//break;
//}
//case type::TypeId::SMALLINT: {
//auto raw = type::ValuePeeker::PeekSmallInt(input_key.GetValue(i));
//WriteValue<int16_t>(data + offset, raw);
//offset += sizeof(int16_t);
//break;
//}
//case type::TypeId::DATE:
//case type::TypeId::INTEGER: {
//auto raw = type::ValuePeeker::PeekInteger(input_key.GetValue(i));
//WriteValue<int32_t>(data + offset, raw);
//offset += sizeof(int32_t);
//break;
//}
//case type::TypeId::TIMESTAMP:
//case type::TypeId::BIGINT: {
//auto raw = type::ValuePeeker::PeekBigInt(input_key.GetValue(i));
//WriteValue<int64_t>(data + offset, raw);
//offset += sizeof(int64_t);
//break;
//}
//case type::TypeId::VARCHAR: {
//auto varchar_val = input_key.GetValue(i);
//auto raw = type::ValuePeeker::PeekVarchar(varchar_val);
//auto raw_len = varchar_val.GetLength();
//WriteAsciiString(data + offset, raw, raw_len);
//offset += raw_len;
//break;
//}