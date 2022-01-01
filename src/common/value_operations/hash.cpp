#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {

hash_t ValueOperations::Hash(const Value &op) {
	if (op.IsNull()) {
		return 0;
	}
	switch (op.type().InternalType()) {
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
	case PhysicalType::UINT8:
		return duckdb::Hash(op.value_.utinyint);
	case PhysicalType::UINT16:
		return duckdb::Hash(op.value_.usmallint);
	case PhysicalType::UINT32:
		return duckdb::Hash(op.value_.uinteger);
	case PhysicalType::UINT64:
		return duckdb::Hash(op.value_.ubigint);
	case PhysicalType::INT128:
		return duckdb::Hash(op.value_.hugeint);
	case PhysicalType::FLOAT:
		return duckdb::Hash(op.value_.float_);
	case PhysicalType::DOUBLE:
		return duckdb::Hash(op.value_.double_);
	case PhysicalType::INTERVAL:
		return duckdb::Hash(op.value_.interval);
	case PhysicalType::VARCHAR:
		return duckdb::Hash(string_t(StringValue::Get(op)));
	case PhysicalType::STRUCT: {
		auto &struct_children = StructValue::GetChildren(op);
		hash_t hash = 0;
		for (auto &entry : struct_children) {
			hash ^= ValueOperations::Hash(entry);
		}
		return hash;
	}
	case PhysicalType::LIST: {
		auto &list_children = ListValue::GetChildren(op);
		hash_t hash = 0;
		for (auto &entry : list_children) {
			hash ^= ValueOperations::Hash(entry);
		}
		return hash;
	}
	default:
		throw InternalException("Unimplemented type for value hash");
	}
}

} // namespace duckdb
