#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

//! Renders an unnamed TUPLE as "(value, ...)" - a single-element tuple gets a trailing comma "(x,)" so it round-trips
static bool TupleToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	// first cast all child elements to varchar
	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	Vector varchar_tuple(cast_data.target, count);
	FlatVector::SetSize(varchar_tuple, count);
	StructBoundCastData::StructToStructCast(source, varchar_tuple, count, parameters);
	auto &base_children = StructVector::GetEntries(source);

	auto &children = StructVector::GetEntries(varchar_tuple);
	auto source_validity = varchar_tuple.Validity();
	vector<VectorIterator<string_t>> child_iterators;
	for (auto &child : children) {
		child_iterators.emplace_back(child.Values<string_t>());
	}
	static constexpr idx_t SEP_LENGTH = 2;
	static constexpr idx_t NULL_LENGTH = 4;
	auto value_needs_quotes = make_unsafe_uniq_array_uninitialized<bool>(children.size());

	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t r = 0; r < count; r++) {
		if (!source_validity.IsValid(r)) {
			result_data.WriteNull();
			continue;
		}

		//! Calculate the total length of the row
		idx_t string_length = 2; // ()
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				string_length += SEP_LENGTH;
			}
			auto add_escapes = !base_children[c].GetType().IsNested();
			auto string_length_func = add_escapes ? VectorCastHelpers::CalculateEscapedStringLength<false>
			                                      : VectorCastHelpers::CalculateStringLength;
			auto child_entry = child_iterators[c][r];
			if (child_entry.IsValid()) {
				//! Skip the `\`, not a special character outside quotes
				string_length += string_length_func(child_entry.GetValue(), value_needs_quotes[c]);
			} else {
				string_length += NULL_LENGTH;
			}
		}
		// a single-element tuple needs a trailing comma to round-trip: "(x,)"
		if (children.size() == 1) {
			string_length++;
		}

		auto &result_str = result_data.WriteEmptyString(string_length);
		auto dataptr = result_str.GetDataWriteable();

		//! Serialize the tuple to the string
		idx_t offset = 0;
		dataptr[offset++] = '(';
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			auto add_escapes = !base_children[c].GetType().IsNested();
			auto write_string_func =
			    add_escapes ? VectorCastHelpers::WriteEscapedString<false> : VectorCastHelpers::WriteString;
			auto child_entry = child_iterators[c][r];
			if (child_entry.IsValid()) {
				//! Skip the `\`, not a special character outside quotes
				offset += write_string_func(dataptr + offset, child_entry.GetValue(), value_needs_quotes[c]);
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		if (children.size() == 1) {
			dataptr[offset++] = ',';
		}
		dataptr[offset++] = ')';
		result_str.Finalize();
	}
	return true;
}

BoundCastInfo DefaultCasts::TupleCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	// a TUPLE is an unnamed struct - all casts are positional, and share the physical struct cast operators
	switch (target.id()) {
	case LogicalTypeId::TUPLE:
	case LogicalTypeId::STRUCT:
		return BoundCastInfo(StructBoundCastData::StructToStructCast,
		                     StructBoundCastData::BindStructToStructCast(input, source, target),
		                     StructBoundCastData::InitStructCastLocalState);
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all child entries to VARCHAR entries
		auto &tuple_children = StructType::GetChildTypes(source);
		child_list_t<LogicalType> varchar_children;
		for (auto &child_entry : tuple_children) {
			varchar_children.push_back(make_pair(child_entry.first, LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::TUPLE(std::move(varchar_children));
		return BoundCastInfo(TupleToVarcharCast,
		                     StructBoundCastData::BindStructToStructCast(input, source, varchar_type),
		                     StructBoundCastData::InitStructCastLocalState);
	}
	case LogicalTypeId::MAP:
		// a TUPLE is unnamed, so (unlike a named STRUCT) it cannot be cast to a MAP
		throw TypeMismatchException(input.query_location, source, target, "Cannot cast unnamed STRUCTs to MAP");
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
