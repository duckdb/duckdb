#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

hash_t ValueOperations::Hash(const Value &op) {
	if (op.is_null) {
		return 0;
	}
	switch (op.type) {
	case TypeId::BOOL:
		return duckdb::Hash(op.value_.boolean);
	case TypeId::INT8:
		return duckdb::Hash(op.value_.tinyint);
	case TypeId::INT16:
		return duckdb::Hash(op.value_.smallint);
	case TypeId::INT32:
		return duckdb::Hash(op.value_.integer);
	case TypeId::INT64:
		return duckdb::Hash(op.value_.bigint);
	case TypeId::FLOAT:
		return duckdb::Hash(op.value_.float_);
	case TypeId::DOUBLE:
		return duckdb::Hash(op.value_.double_);
	case TypeId::POINTER:
		return duckdb::Hash(op.value_.pointer);
	case TypeId::VARCHAR:
		return duckdb::Hash(op.str_value.c_str());
	default:
		throw NotImplementedException("Unimplemented type for hash");
	}
}
