#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {
using namespace std;

hash_t ValueOperations::Hash(const Value &op) {
	if (op.is_null) {
		return 0;
	}
	switch (op.type) {
	case PhysicalType::BOOL:
		return duckdb::Hash(op.value_.boolean);
	case PhysicalType::INT8:
		return duckdb::Hash(op.value_.tinyint);
	case PhysicalType::INT16:
		return duckdb::Hash(op.value_.smallint);
	case PhysicalType::INT32:
		return duckdb::Hash(op.value_.integer);
	case PhysicalType::INT64:
		return duckdb::Hash(op.value_.bigint);
	case PhysicalType::INT128:
		return duckdb::Hash(op.value_.hugeint);
	case PhysicalType::FLOAT:
		return duckdb::Hash(op.value_.float_);
	case PhysicalType::DOUBLE:
		return duckdb::Hash(op.value_.double_);
	case PhysicalType::POINTER:
		return duckdb::Hash(op.value_.pointer);
	case PhysicalType::VARCHAR:
		return duckdb::Hash(op.str_value.c_str());
	default:
		throw NotImplementedException("Unimplemented type for value hash");
	}
}

} // namespace duckdb
