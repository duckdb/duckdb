#include "execution/index/art/art.hpp"

using namespace duckdb;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types,
                       vector<TypeId> expression_types, vector<unique_ptr<Expression>> expressions,
                       size_t initial_capacity, vector<unique_ptr<Expression>> unbinded_expressions)
        : Index(IndexType::ORDER_INDEX), table(table), column_ids(column_ids), types(types), expressions(move(expressions)),
          unbinded_expressions(move(unbinded_expressions)), data(nullptr) {
    tree= NULL;
    expression_result.Initialize(expression_types);
    int n = 1;
    // little endian if true
    if(*(char *)&n == 1) {
        is_little_endian = true;
    }

}


template <class T> static void ART::templated_insert(DataChunk &input, Vector &row_ids) {
    auto input_data = (T *)input.data[0].data;
    auto row_identifiers = (uint64_t *)row_ids.data;
    for (size_t i = 0; i < row_ids.count; i++) {
        uint8_t minKey[maxPrefixLength];
        if (is_little_endian)
            con
        auto bin_data= loadKey(Node::flipSign(input_data[i],minKey);
        insert(tree,&tree,key,0,keys[i],8);
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
            templated_insert<int64_t>(input, row_ids);
            break;
        default:
            throw InvalidTypeException(input.data[0].type, "Invalid type for index");
    }
}

switch (column.GetType()) {
case type::TypeId::BOOLEAN:
case type::TypeId::TINYINT: {
auto raw = type::ValuePeeker::PeekTinyInt(input_key.GetValue(i));
WriteValue<int8_t>(data + offset, raw);
offset += sizeof(int8_t);
break;
}
case type::TypeId::SMALLINT: {
auto raw = type::ValuePeeker::PeekSmallInt(input_key.GetValue(i));
WriteValue<int16_t>(data + offset, raw);
offset += sizeof(int16_t);
break;
}
case type::TypeId::DATE:
case type::TypeId::INTEGER: {
auto raw = type::ValuePeeker::PeekInteger(input_key.GetValue(i));
WriteValue<int32_t>(data + offset, raw);
offset += sizeof(int32_t);
break;
}
case type::TypeId::TIMESTAMP:
case type::TypeId::BIGINT: {
auto raw = type::ValuePeeker::PeekBigInt(input_key.GetValue(i));
WriteValue<int64_t>(data + offset, raw);
offset += sizeof(int64_t);
break;
}
case type::TypeId::VARCHAR: {
auto varchar_val = input_key.GetValue(i);
auto raw = type::ValuePeeker::PeekVarchar(varchar_val);
auto raw_len = varchar_val.GetLength();
WriteAsciiString(data + offset, raw, raw_len);
offset += raw_len;
break;
}