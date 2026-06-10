//===----------------------------------------------------------------------===//
//
// duckdb/function/aggregate_state_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/type_util.hpp"

namespace duckdb {

//! Detection trait: true when STATE defines a nested STATE_TYPE (i.e. StructStateType<...> or OptionalStateType<T>)
template <class STATE, class = void>
struct HasStructStateType : std::false_type {};

template <class STATE>
struct HasStructStateType<STATE, std::void_t<typename STATE::STATE_TYPE>> : std::true_type {};

//! Phantom marker type for use in StructStateType only.
//! Represents two consecutive flat fields at base+0 and base+sizeof(T): T value, bool is_set.
//! memset(0) initializes to "not set" (is_set = false, value = 0).
template <class T>
struct OptionalStateType {
	using value_type = T;
};

//! Detection trait: true when T is OptionalStateType<U> for some U.
template <class T>
struct IsOptionalStateType : std::false_type {};
template <class T>
struct IsOptionalStateType<OptionalStateType<T>> : std::true_type {};

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

//! Maps a single C++ field type to a LogicalType.
//! OptionalStateType<T> → PrimitiveToLogicalType<T>() (the optional encoding is captured in
//! AggregateStateField::is_optional) T with STATE_TYPE → nested struct type otherwise → PrimitiveToLogicalType<T>()
template <class T>
LogicalType FieldToLogicalType() {
	if constexpr (IsOptionalStateType<T>::value) {
		return PrimitiveToLogicalType<typename T::value_type>();
	} else if constexpr (HasStructStateType<T>::value) {
		return T::STATE_TYPE::GetLogicalType(T::STATE_NAMES);
	} else {
		return PrimitiveToLogicalType<T>();
	}
}

//! Per-field layout information within an aggregate state.
//! field_offset: byte offset of this field relative to the parent struct's base.
//! is_optional: true when this field is an OptionalStateType<T> — physically T value + bool is_set at
//! field_offset+sizeof(T). children: non-empty only when the field is itself a STRUCT; each child's offset is relative
//! to this field's base.
struct AggregateStateField {
	idx_t field_offset = 0;
	bool is_optional = false;
	vector<AggregateStateField> children;

	static idx_t GetPhysicalSize(const LogicalType &type) {
		if (type.id() != LogicalTypeId::STRUCT) {
			return GetTypeIdSize(type.InternalType());
		}
		idx_t size = 0;
		for (const auto &child : StructType::GetChildTypes(type)) {
			idx_t child_size = GetPhysicalSize(child.second);
			size = AlignValue(size, MinValue<idx_t>(child_size, 8));
			size += child_size;
		}
		return size;
	}

	static void PopulateChildren(const LogicalType &type, AggregateStateField &field) {
		if (type.id() != LogicalTypeId::STRUCT) {
			return;
		}
		D_ASSERT(field.children.empty());
		idx_t offset = 0;
		for (auto &[name, child_type] : StructType::GetChildTypes(type)) {
			idx_t child_size = GetPhysicalSize(child_type);
			offset = AlignValue(offset, MinValue<idx_t>(child_size, 8));
			AggregateStateField child_field;
			child_field.field_offset = offset;
			PopulateChildren(child_type, child_field);
			field.children.push_back(std::move(child_field));
			offset += child_size;
		}
	}
};

//! Populate one field's worth of AggregateStateField children from a C++ type T.
//! Advances `offset` by the physical size of T (including the is_set bool for OptionalStateType).
template <class T>
void PopulateFieldFromType(AggregateStateField &parent, idx_t &offset) {
	AggregateStateField child;
	if constexpr (IsOptionalStateType<T>::value) {
		using V = typename T::value_type;
		constexpr idx_t alignment = MinValue<idx_t>(sizeof(V), 8);
		offset = AlignValue(offset, alignment);
		child.field_offset = offset;
		child.is_optional = true;
		parent.children.push_back(std::move(child));
		offset += sizeof(V) + sizeof(bool);
	} else if constexpr (HasStructStateType<T>::value) {
		auto logical = FieldToLogicalType<T>();
		idx_t sz = AggregateStateField::GetPhysicalSize(logical);
		offset = AlignValue(offset, MinValue<idx_t>(sz, 8));
		child.field_offset = offset;
		AggregateStateField::PopulateChildren(logical, child);
		parent.children.push_back(std::move(child));
		offset += sz;
	} else {
		constexpr idx_t alignment = MinValue<idx_t>(sizeof(T), 8);
		offset = AlignValue(offset, alignment);
		child.field_offset = offset;
		parent.children.push_back(std::move(child));
		offset += sizeof(T);
	}
}

//! Describes a struct-typed aggregate state layout. Intended for use as a nested type inside an aggregate STATE struct:
//!   static constexpr const char *STATE_NAMES[] = {"field_a", "field_b"};
//!   using STATE_TYPE = StructStateType<uint64_t, double>;
//! UnaryAggregate/BinaryAggregate/NullaryAggregate detect STATE_TYPE and wire up SetStructStateExport automatically.
//! Fields that themselves have STATE_TYPE are recursively expanded into nested struct types.
//! OptionalStateType<T> fields are exported as nullable T (T value at base+0, bool is_set at base+sizeof(T)).
//! Names are passed at call time (STATE::STATE_NAMES) rather than as template arguments.
template <typename... Ts>
struct StructStateType {
	static LogicalType GetLogicalType(const char *const *names) {
		child_list_t<LogicalType> children;
		idx_t i = 0;
		(children.emplace_back(names[i++], FieldToLogicalType<Ts>()), ...);
		return LogicalType::STRUCT(std::move(children));
	}

	//! Populate field children using compile-time type knowledge, correctly handling OptionalStateType<T>.
	static void PopulateField(AggregateStateField &field) {
		idx_t offset = 0;
		(PopulateFieldFromType<Ts>(field, offset), ...);
	}
};

//! Detection trait: true when T is StructStateType<Us...> for some Us.
template <class T>
struct IsStructStateType : std::false_type {};
template <class... Ts>
struct IsStructStateType<StructStateType<Ts...>> : std::true_type {};

//! Top-level description of an aggregate state for export/import purposes.
//! Returned by the aggregate_get_state_type_t callback registered via SetStructStateExport.
//!
//! - Primitive state (e.g. int64_t for count): type=BIGINT, field.children empty, field.is_optional=false
//! - Optional state (e.g. OptionalStateType<bool>): type=BOOLEAN, field.is_optional=true, field.children empty
//! - Struct state: type=STRUCT(...), field.children fully populated via StructStateType::PopulateField
//! total_state_size is the aligned size of the full state (stride between consecutive states in a buffer).
struct AggregateStateLayout {
	AggregateStateLayout() = default;
	AggregateStateLayout(LogicalType type_p, idx_t total_state_size_p, bool is_optional = false)
	    : type(std::move(type_p)), total_state_size(total_state_size_p) {
		field.is_optional = is_optional;
		AggregateStateField::PopulateChildren(type, field);
	}

	LogicalType type;
	AggregateStateField field;
	idx_t total_state_size = 0;
};

} // namespace duckdb
