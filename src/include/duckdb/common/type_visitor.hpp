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
	static constexpr idx_t MAX_TYPE_RECURSION_DEPTH = 1000;
	template <class F>
	static bool Contains(const LogicalType &type, F &&predicate);

	static bool Contains(const LogicalType &type, LogicalTypeId type_id);

	template <class F>
	static LogicalType VisitReplace(const LogicalType &type, F &&func);

private:
		template <class F>
	    static LogicalType VisitReplaceInternal(const LogicalType &type, F &&func , idx_t depth);

		template <class F>
	    static bool ContainsInternal(const LogicalType &type, F &&predicate, idx_t depth);
};

template <class F>
inline LogicalType TypeVisitor::VisitReplace(const LogicalType &type, F &&func) {
	return VisitReplaceInternal(type, func, 0);
}

template <class F>
inline LogicalType TypeVisitor::VisitReplaceInternal(const LogicalType &type, F &&func , idx_t depth) {
	if (depth >= MAX_TYPE_RECURSION_DEPTH) {
		throw InternalException("Max type recursion depth limit of %llu exceeded in TypeVisitor::VisitReplace",
		                        MAX_TYPE_RECURSION_DEPTH);
	}

	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		auto children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			child.second = VisitReplaceInternal(child.second, func, depth + 1);
		}
		return func(LogicalType::STRUCT(children));
	}
	case LogicalTypeId::TUPLE: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		auto children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			child.second = VisitReplaceInternal(child.second, func, depth + 1);
		}
		return func(LogicalType::TUPLE(children));
	}
	case LogicalTypeId::UNION: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		auto children = UnionType::CopyMemberTypes(type);
		for (auto &child : children) {
			child.second = VisitReplaceInternal(child.second, func, depth + 1);
		}
		return func(LogicalType::UNION(children));
	}
	case LogicalTypeId::LIST: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		const auto &child = ListType::GetChildType(type);
		return func(LogicalType::LIST(VisitReplaceInternal(child, func, depth + 1)));
	}
	case LogicalTypeId::ARRAY: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		const auto &child = ArrayType::GetChildType(type);
		return func(LogicalType::ARRAY(VisitReplaceInternal(child, func, depth + 1), ArrayType::GetSize(type)));
	}
	case LogicalTypeId::MAP: {
		if (!type.AuxInfo()) {
			return func(type);
		}
		const auto &key = MapType::KeyType(type);
		const auto &value = MapType::ValueType(type);
		return func(LogicalType::MAP(VisitReplaceInternal(key, func, depth + 1), VisitReplaceInternal(value, func, depth + 1)));
	}
	default:
		return func(type);
	}
}


template <class F>
inline bool TypeVisitor::Contains(const LogicalType &type, F &&predicate) {
	return ContainsInternal(type, predicate, 0);
}

template <class F>
inline bool TypeVisitor::ContainsInternal(const LogicalType &type, F &&predicate, idx_t depth) {
	if (depth >= MAX_TYPE_RECURSION_DEPTH) {
		throw InternalException("Max type recursion depth limit of %llu exceeded in TypeVisitor::ContainsInternal",
		                        MAX_TYPE_RECURSION_DEPTH);
	}
	if (predicate(type)) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		if (!type.AuxInfo()) {
			return false;
		}
		for (const auto &child : StructType::GetChildTypes(type)) {
			if (ContainsInternal(child.second, predicate, depth + 1)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::UNION:
		if (!type.AuxInfo()) {
			return false;
		}
		for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
			if (ContainsInternal(UnionType::GetMemberType(type, i), predicate, depth + 1)) {
				return true;
			}
		}
		return false;
	case LogicalTypeId::LIST:
		if (!type.AuxInfo()) {
			return false;
		}
		return ContainsInternal(ListType::GetChildType(type), predicate, depth + 1);
	case LogicalTypeId::ARRAY:
		if (!type.AuxInfo()) {
			return false;
		}
		return ContainsInternal(ArrayType::GetChildType(type), predicate, depth + 1);
	case LogicalTypeId::MAP:
		if (!type.AuxInfo()) {
			return false;
		}
		return ContainsInternal(MapType::KeyType(type), predicate , depth + 1) ||
			ContainsInternal(MapType::ValueType(type), predicate, depth + 1);
	default:
		return false;
	}
}

inline bool TypeVisitor::Contains(const LogicalType &type, LogicalTypeId type_id) {
	return Contains(type, [&](const LogicalType &ty) { return ty.id() == type_id; });
}

} // namespace duckdb
