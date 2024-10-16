//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/type_visitor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct TypeVisitor {
	template <class F>
	static bool Contains(const LogicalType &type, F &&predicate);

	static bool Contains(const LogicalType &type, LogicalTypeId type_id);

	template <class F>
	static LogicalType VisitReplace(const LogicalType &type, F &&func);
};

template <class F>
inline LogicalType TypeVisitor::VisitReplace(const LogicalType &type, F &&func) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			child.second = VisitReplace(child.second, func);
		}
		return func(LogicalType::STRUCT(children));
	}
	case LogicalTypeId::UNION: {
		auto children = UnionType::CopyMemberTypes(type);
		for (auto &child : children) {
			child.second = VisitReplace(child.second, func);
		}
		return func(LogicalType::UNION(children));
	}
	case LogicalTypeId::LIST: {
		auto child = ListType::GetChildType(type);
		return func(LogicalType::LIST(VisitReplace(child, func)));
	}
	case LogicalTypeId::ARRAY: {
		auto child = ArrayType::GetChildType(type);
		return func(LogicalType::ARRAY(VisitReplace(child, func), ArrayType::GetSize(type)));
	}
	case LogicalTypeId::MAP: {
		auto key = MapType::KeyType(type);
		auto value = MapType::ValueType(type);
		return func(LogicalType::MAP(VisitReplace(key, func), VisitReplace(value, func)));
	}
	default:
		return func(type);
	}
}

template <class F>
inline bool TypeVisitor::Contains(const LogicalType &type, F &&predicate) {
	if (predicate(type)) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		for (const auto &child : StructType::GetChildTypes(type)) {
			if (Contains(child.second, predicate)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::UNION:
		for (const auto &child : UnionType::CopyMemberTypes(type)) {
			if (Contains(child.second, predicate)) {
				return true;
			}
		}
		return false;
	case LogicalTypeId::LIST:
		return Contains(ListType::GetChildType(type), predicate);
	case LogicalTypeId::ARRAY:
		return Contains(ArrayType::GetChildType(type), predicate);
	case LogicalTypeId::MAP:
		return Contains(MapType::KeyType(type), predicate) || Contains(MapType::ValueType(type), predicate);
	default:
		return false;
	}
}

inline bool TypeVisitor::Contains(const LogicalType &type, LogicalTypeId type_id) {
	return Contains(type, [&](const LogicalType &ty) { return ty.id() == type_id; });
}

} // namespace duckdb
