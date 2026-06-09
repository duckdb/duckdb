//===----------------------------------------------------------------------===//
//
// duckdb/function/aggregate_state_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/type_util.hpp"

namespace duckdb {

//! Describes a struct-typed aggregate state layout using a compile-time field name array and a type parameter pack.
//! Intended for use as a nested type inside an aggregate STATE struct:
//!   static constexpr const char *STATE_NAMES[] = {"field_a", "field_b"};
//!   using STATE_TYPE = StructStateType<STATE_NAMES, uint64_t, double>;
//! UnaryAggregate/BinaryAggregate/NullaryAggregate detect STATE_TYPE and wire up SetStructStateExport automatically.
template <const char *const *Names, typename... Ts>
struct StructStateType {
	static LogicalType GetLogicalType() {
		child_list_t<LogicalType> children;
		idx_t i = 0;
		(children.emplace_back(Names[i++], PrimitiveToLogicalType<Ts>()), ...);
		return LogicalType::STRUCT(std::move(children));
	}
};

//! Detection trait: true when STATE defines a nested STATE_TYPE (i.e. StructStateType<...>)
template <class STATE, class = void>
struct HasStructStateType : std::false_type {};

template <class STATE>
struct HasStructStateType<STATE, std::void_t<typename STATE::STATE_TYPE>> : std::true_type {};

} // namespace duckdb
