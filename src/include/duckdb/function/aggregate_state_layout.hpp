//===----------------------------------------------------------------------===//
//
// duckdb/function/aggregate_state_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/type_util.hpp"

namespace duckdb {

//! Detection trait: true when STATE defines a nested STATE_TYPE (i.e. StructStateType<...>)
template <class STATE, class = void>
struct HasStructStateType : std::false_type {};

template <class STATE>
struct HasStructStateType<STATE, std::void_t<typename STATE::STATE_TYPE>> : std::true_type {};

//! Maps a single C++ field type to a LogicalType.
//! If T itself defines STATE_TYPE, returns its nested struct type; otherwise calls PrimitiveToLogicalType<T>().
template <class T>
LogicalType FieldToLogicalType() {
	if constexpr (HasStructStateType<T>::value) {
		return T::STATE_TYPE::GetLogicalType(T::STATE_NAMES);
	} else {
		return PrimitiveToLogicalType<T>();
	}
}

//! Describes a struct-typed aggregate state layout. Intended for use as a nested type inside an aggregate STATE struct:
//!   static constexpr const char *STATE_NAMES[] = {"field_a", "field_b"};
//!   using STATE_TYPE = StructStateType<uint64_t, double>;
//! UnaryAggregate/BinaryAggregate/NullaryAggregate detect STATE_TYPE and wire up SetStructStateExport automatically.
//! Fields that themselves have STATE_TYPE are recursively expanded into nested struct types.
//! Names are passed at call time (STATE::STATE_NAMES) rather than as template arguments.
template <typename... Ts>
struct StructStateType {
	static LogicalType GetLogicalType(const char *const *names) {
		child_list_t<LogicalType> children;
		idx_t i = 0;
		(children.emplace_back(names[i++], FieldToLogicalType<Ts>()), ...);
		return LogicalType::STRUCT(std::move(children));
	}
};

} // namespace duckdb
