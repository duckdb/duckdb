//===----------------------------------------------------------------------===//
//
// duckdb/function/aggregate_state_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/types/list_segment.hpp"

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

//! Phantom marker type: resolves to the return type of the bound aggregate function.
struct StateReturnType {};

//! Phantom marker type: resolves to the type of the bound aggregate function's INDEX'th argument.
template <idx_t INDEX>
struct StateInputType {
	static constexpr idx_t index = INDEX;
};

//! Detection trait: true when T is StateInputType<INDEX> for some INDEX.
template <class T>
struct IsStateInputType : std::false_type {};
template <idx_t I>
struct IsStateInputType<StateInputType<I>> : std::true_type {};

//! The runtime types of a bound aggregate function - used to resolve the logical types of state fields that are
//! only known after binding (see StateReturnType / StateInputType).
struct StateLayoutTypeInfo {
	const LogicalType &return_type;
	const vector<LogicalType> &argument_types;
};

//! Resolves a type source marker (StateReturnType or StateInputType<INDEX>) to a LogicalType.
template <class SOURCE>
LogicalType ResolveStateSourceType(const StateLayoutTypeInfo &info) {
	if constexpr (IsStateInputType<SOURCE>::value) {
		D_ASSERT(SOURCE::index < info.argument_types.size());
		return info.argument_types[SOURCE::index];
	} else {
		static_assert(std::is_same<SOURCE, StateReturnType>::value,
		              "the type source must be StateReturnType or StateInputType<INDEX>");
		return info.return_type;
	}
}

//! Phantom marker type for use inside OptionalStateType.
//! Signals that the field stores a binary sort key (string_t) that must be decoded/encoded
//! via CreateSortKeyHelpers when exporting/importing aggregate state.
//! SOURCE describes where the decoded logical type comes from; ORDER is the ordering used when creating the sort key.
template <class SOURCE, OrderType ORDER>
struct StateSortKey {
	using SOURCE_TYPE = SOURCE;
	static constexpr OrderType order_type = ORDER;
};

//! Detection trait: true when T is StateSortKey<SOURCE, ORDER> for some SOURCE/ORDER.
template <class T>
struct IsStateSortKeyType : std::false_type {};
template <class S, OrderType O>
struct IsStateSortKeyType<StateSortKey<S, O>> : std::true_type {};

//! Phantom marker type for fields that are physically stored as T, while their exported logical type comes from
//! the bound aggregate function as described by SOURCE - e.g. exporting a string_t as VARCHAR, BLOB or BIT, or
//! an int64_t "by" value as TIMESTAMP.
template <class T, class SOURCE>
struct StateTypedValue {
	using PHYSICAL_TYPE = T;
	using SOURCE_TYPE = SOURCE;
};

//! Detection trait: true when T is StateTypedValue<V, SOURCE> for some V/SOURCE.
template <class T>
struct IsStateTypedValueType : std::false_type {};
template <class T, class S>
struct IsStateTypedValueType<StateTypedValue<T, S>> : std::true_type {};

//! Shorthand for values physically stored as a string_t.
template <class SOURCE>
using StateString = StateTypedValue<string_t, SOURCE>;

//! Phantom marker type for use as an aggregate STATE's STATE_TYPE.
//! Signals that the state is a LinkedList (see list_segment.hpp) holding the rows of a LIST value.
//! Export reads the linked list into a LIST vector; import appends the LIST value's rows back into a linked list.
//! SOURCE describes where the list's logical type comes from.
template <class SOURCE>
struct StateListType {
	using SOURCE_TYPE = SOURCE;
};

//! Detection trait: true when T is StateListType<SOURCE> for some SOURCE.
template <class T>
struct IsStateListType : std::false_type {};
template <class S>
struct IsStateListType<StateListType<S>> : std::true_type {};

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
//! info holds the runtime types of the bound function - used to resolve fields whose logical type cannot be
//! expressed statically (sort keys and StateTypedValue fields).
//! OptionalStateType<T> → the type of T (the optional encoding is captured in AggregateStateField::kind)
//! StateSortKey<SOURCE, ORDER> / StateTypedValue<T, SOURCE> → the resolved SOURCE type
//! T with STATE_TYPE → nested struct type   otherwise → PrimitiveToLogicalType<T>()
template <class T>
LogicalType FieldToLogicalType(const StateLayoutTypeInfo &info) {
	if constexpr (IsOptionalStateType<T>::value) {
		return FieldToLogicalType<typename T::value_type>(info);
	} else if constexpr (IsStateSortKeyType<T>::value || IsStateTypedValueType<T>::value) {
		return ResolveStateSourceType<typename T::SOURCE_TYPE>(info);
	} else if constexpr (HasStructStateType<T>::value) {
		return T::STATE_TYPE::GetLogicalType(T::STATE_NAMES, info);
	} else {
		return PrimitiveToLogicalType<T>();
	}
}

//! Describes the kind of a single field within an aggregate state layout.
enum class AggregateFieldKind : uint8_t {
	//! Scalar value. field_offset = byte offset of the value.
	PRIMITIVE,
	//! Compound struct. field_offset = byte offset of the struct base.
	//! children = one entry per struct member (offsets relative to this field's base).
	STRUCT,
	//! Nullable wrapper. field_offset = byte offset of the bool is_set flag.
	//! children has exactly one entry: the value field (PRIMITIVE, STRUCT, or SORT_KEY).
	//! The value field's field_offset is relative to the same parent base as this field.
	OPTIONAL,
	//! Binary sort key (stored as string_t). field_offset = byte offset of the string_t.
	//! sort_key_order carries the ordering. Always appears as children[0] of an OPTIONAL field.
	SORT_KEY,
	//! Linked list of values (stored as a LinkedList, see list_segment.hpp). field_offset = byte offset of the
	//! LinkedList. Exported as a LIST value; an empty linked list is exported as NULL. Only supported as the
	//! top-level field of a state - the segment functions live in AggregateStateLayout::list_functions.
	LIST,
};

//! Per-field layout information within an aggregate state.
struct AggregateStateField {
	idx_t field_offset = 0;
	//! Physical byte size of the data described by this field (including nested struct members).
	//! For OPTIONAL: includes is_set bool (field_offset + sizeof(bool)).
	idx_t field_size = 0;
	AggregateFieldKind kind = AggregateFieldKind::PRIMITIVE;
	OrderType sort_key_order = OrderType::ASCENDING; // only meaningful when kind == SORT_KEY
	vector<AggregateStateField> children;

	//! The alignment of this field when placed as a struct member, mirroring the C++ struct layout rules.
	//! For OPTIONAL the alignment is that of the wrapped value - the trailing is_set bool does not affect it.
	idx_t GetAlignment() const {
		if (kind == AggregateFieldKind::OPTIONAL) {
			D_ASSERT(children.size() == 1);
			return children[0].GetAlignment();
		}
		return MinValue<idx_t>(field_size, 8);
	}

	//! Shift this field's offsets to place it at `offset` within its parent.
	//! The value child of an OPTIONAL is relative to the same parent base as the optional itself, so it shifts along;
	//! STRUCT children are relative to the struct's own base and stay untouched.
	void ShiftBase(idx_t offset) {
		field_offset += offset;
		if (kind == AggregateFieldKind::OPTIONAL) {
			D_ASSERT(children.size() == 1);
			children[0].ShiftBase(offset);
		}
	}

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
			child_field.field_size = child_size;
			child_field.kind =
			    (child_type.id() == LogicalTypeId::STRUCT) ? AggregateFieldKind::STRUCT : AggregateFieldKind::PRIMITIVE;
			PopulateChildren(child_type, child_field);
			field.children.push_back(std::move(child_field));
			offset += child_size;
		}
	}
};

// Forward-declared so StructStateType::AppendChildren can call it before the full definition below.
template <class T>
AggregateStateField BuildStateField();

//! Describes a struct-typed aggregate state layout. Intended for use as a nested type inside an aggregate STATE struct:
//!   static constexpr const char *STATE_NAMES[] = {"field_a", "field_b"};
//!   using STATE_TYPE = StructStateType<uint64_t, double>;
//! UnaryAggregate/BinaryAggregate/NullaryAggregate detect STATE_TYPE and wire up SetStructStateExport automatically.
//! Fields that themselves have STATE_TYPE are recursively expanded into nested struct types.
//! OptionalStateType<T> fields are exported as nullable T (T value at base+0, bool is_set at base+sizeof(T)).
//! Names are passed at call time (STATE::STATE_NAMES) rather than as template arguments.
template <typename... Ts>
struct StructStateType {
	static LogicalType GetLogicalType(const char *const *names, const StateLayoutTypeInfo &info) {
		child_list_t<LogicalType> children;
		idx_t i = 0;
		(children.emplace_back(names[i++], FieldToLogicalType<Ts>(info)), ...);
		return LogicalType::STRUCT(std::move(children));
	}

	//! Appends one child field for type T into field.children, advancing offset by the field's physical size.
	template <class T>
	static void AppendChildField(AggregateStateField &field, idx_t &offset) {
		auto child = BuildStateField<T>();
		offset = AlignValue(offset, child.GetAlignment());
		child.ShiftBase(offset);
		offset += child.field_size;
		field.children.push_back(std::move(child));
	}

	//! Populate field.children for all member types, computing offsets from sizes.
	static void AppendChildren(AggregateStateField &field, idx_t &offset) {
		(AppendChildField<Ts>(field, offset), ...);
	}
};

//! Detection trait: true when T is StructStateType<Us...> for some Us.
template <class T>
struct IsStructStateType : std::false_type {};
template <class... Ts>
struct IsStructStateType<StructStateType<Ts...>> : std::true_type {};

//! Build an AggregateStateField for a compile-time state type T, with field_offset=0.
//! field_size is always set to the physical byte size of T's data (see kind docs for details).
//!
//! Composable rules:
//!   - OptionalStateType<V>  → OPTIONAL: is_set at field_offset=V.field_size, wraps BuildStateField<V>()
//!   - StateSortKey<ORDER>   → SORT_KEY: field_size=sizeof(string_t)
//!   - StructStateType<Ts…>  → STRUCT: children built via AppendChildren, field_size=total child data size
//!   - anything else         → PRIMITIVE: field_size=sizeof(T)
template <class T>
AggregateStateField BuildStateField() {
	AggregateStateField field;
	if constexpr (IsOptionalStateType<T>::value) {
		using V = typename T::value_type;
		field.kind = AggregateFieldKind::OPTIONAL;
		auto value_child = BuildStateField<V>();
		value_child.field_offset = 0;
		field.field_offset = value_child.field_size; // is_set follows the value data
		field.field_size = value_child.field_size + sizeof(bool);
		field.children.push_back(std::move(value_child));
	} else if constexpr (IsStateSortKeyType<T>::value) {
		field.kind = AggregateFieldKind::SORT_KEY;
		field.sort_key_order = T::order_type;
		field.field_size = sizeof(string_t);
	} else if constexpr (IsStateTypedValueType<T>::value) {
		// stored as a plain value - only the logical type is resolved at bind time
		field.field_size = sizeof(typename T::PHYSICAL_TYPE);
	} else if constexpr (IsStateListType<T>::value) {
		field.kind = AggregateFieldKind::LIST;
		field.field_size = sizeof(LinkedList);
	} else if constexpr (IsStructStateType<T>::value) {
		// T is StructStateType<Ts...> — the phantom descriptor type itself
		field.kind = AggregateFieldKind::STRUCT;
		idx_t offset = 0;
		T::AppendChildren(field, offset);
		field.field_size = offset; // total physical size of all members
	} else if constexpr (HasStructStateType<T>::value) {
		// T is a concrete C++ struct that declares STATE_TYPE = StructStateType<...>
		field.kind = AggregateFieldKind::STRUCT;
		idx_t offset = 0;
		T::STATE_TYPE::AppendChildren(field, offset);
		field.field_size = sizeof(T);
	} else {
		// PRIMITIVE
		field.field_size = sizeof(T);
	}
	return field;
}

//! Top-level description of an aggregate state for export/import purposes.
//! Returned by the aggregate_get_state_type_t callback registered via SetStructStateExport.
//!
//! - Primitive state (e.g. int64_t for count): field.kind=PRIMITIVE, field.field_offset=0, field.children empty.
//! - Optional primitive (e.g. OptionalStateType<double>): field.kind=OPTIONAL,
//!   field.field_offset=sizeof(double) (is_set offset), field.children=[{kind=PRIMITIVE, field_offset=0}].
//! - Optional struct: field.kind=OPTIONAL, field.field_offset=struct_size (is_set offset),
//!   field.children=[{kind=STRUCT, field_offset=0, children=[struct fields]}].
//! - Non-optional struct: field.kind=STRUCT, field.field_offset=0, field.children=[struct fields].
//! total_state_size is the aligned stride between consecutive states in a packed buffer.
struct AggregateStateLayout {
	AggregateStateLayout() = default;
	AggregateStateLayout(LogicalType type_p, idx_t total_state_size_p, bool is_optional = false)
	    : type(std::move(type_p)), total_state_size(total_state_size_p) {
		if (is_optional) {
			field.kind = AggregateFieldKind::OPTIONAL;
			field.field_offset = AggregateStateField::GetPhysicalSize(type); // is_set after the value
			field.field_size = field.field_offset + sizeof(bool);
			AggregateStateField value_child;
			value_child.field_offset = 0;
			value_child.field_size = field.field_offset;
			value_child.kind =
			    (type.id() == LogicalTypeId::STRUCT) ? AggregateFieldKind::STRUCT : AggregateFieldKind::PRIMITIVE;
			AggregateStateField::PopulateChildren(type, value_child);
			field.children.push_back(std::move(value_child));
		} else if (type.id() == LogicalTypeId::STRUCT) {
			field.kind = AggregateFieldKind::STRUCT;
			AggregateStateField::PopulateChildren(type, field);
			field.field_size = AggregateStateField::GetPhysicalSize(type);
		} else {
			field.field_size = AggregateStateField::GetPhysicalSize(type);
		}
		// else: field.kind = PRIMITIVE (default), field.children empty
	}

	LogicalType type;
	AggregateStateField field;
	idx_t total_state_size = 0;
	//! The segment functions used to read/write the linked list state when field.kind is LIST
	ListSegmentFunctions list_functions;
};

} // namespace duckdb
