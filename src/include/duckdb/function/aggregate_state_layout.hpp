//===----------------------------------------------------------------------===//
//
// duckdb/function/aggregate_state_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/type_util.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

//! Detection trait: true when STATE defines a nested STATE_TYPE (i.e. StructStateType<...>)
template <class STATE, class = void>
struct HasStructStateType : std::false_type {};

template <class STATE>
struct HasStructStateType<STATE, std::void_t<typename STATE::STATE_TYPE>> : std::true_type {};

//! Detection trait: true when STATE is itself a C++ primitive type mappable to a LogicalType via PrimitiveToLogicalType
template <class T>
struct HasPrimitiveLogicalType : std::false_type {};

template <>
struct HasPrimitiveLogicalType<bool> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<int8_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<int16_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<int32_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<int64_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<uint8_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<uint16_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<uint32_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<uint64_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<hugeint_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<uhugeint_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<float> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<double> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<date_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<dtime_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<dtime_tz_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<dtime_ns_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_sec_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_ms_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_ns_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_tz_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<timestamp_tz_ns_t> : std::true_type {};
template <>
struct HasPrimitiveLogicalType<interval_t> : std::true_type {};

//! Detection trait: true when STATE is optional<T> where T itself has HasPrimitiveLogicalType.
//! These states export as PrimitiveToLogicalType<T>(), with nullopt ↔ SQL NULL.
template <class T>
struct HasOptionalPrimitiveType : std::false_type {};

template <class T>
struct HasOptionalPrimitiveType<optional<T>> : HasPrimitiveLogicalType<T> {};

//! Maps a single C++ field type to a LogicalType.
//! If T itself defines STATE_TYPE, returns its nested struct type; otherwise calls PrimitiveToLogicalType<T>().
template <class T>
LogicalType FieldToLogicalType() {
	if constexpr (HasStructStateType<T>::value) {
		return T::STATE_TYPE::GetLogicalType();
	} else {
		return PrimitiveToLogicalType<T>();
	}
}

//! Describes a struct-typed aggregate state layout using a compile-time field name array and a type parameter pack.
//! Intended for use as a nested type inside an aggregate STATE struct:
//!   static constexpr const char *STATE_NAMES[] = {"field_a", "field_b"};
//!   using STATE_TYPE = StructStateType<STATE_NAMES, uint64_t, double>;
//! UnaryAggregate/BinaryAggregate/NullaryAggregate detect STATE_TYPE and wire up SetStructStateExport automatically.
//! Fields that themselves have STATE_TYPE are recursively expanded into nested struct types.
template <const char *const *Names, typename... Ts>
struct StructStateType {
	static LogicalType GetLogicalType() {
		child_list_t<LogicalType> children;
		idx_t i = 0;
		(children.emplace_back(Names[i++], FieldToLogicalType<Ts>()), ...);
		return LogicalType::STRUCT(std::move(children));
	}
};

} // namespace duckdb
