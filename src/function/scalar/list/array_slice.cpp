#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

template <typename INPUT_TYPE, typename INDEX_TYPE>
INDEX_TYPE ValueOffset(const INPUT_TYPE &value) {
	return 0;
}

template <>
int64_t ValueOffset(const list_entry_t &value) {
	return value.offset;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INDEX_TYPE ValueLength(const INPUT_TYPE &value) {
	return 0;
}

template <>
int64_t ValueLength(const list_entry_t &value) {
	return value.length;
}

template <>
int32_t ValueLength(const string_t &value) {
	return LengthFun::Length<string_t, int32_t>(value);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
bool ClampIndex(INDEX_TYPE &index, const INPUT_TYPE &value) {
	const auto length = ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (index < 0) {
		if (-index > length) {
			return false;
		}
		index = length + index;
	} else if (index > length) {
		index = length;
	}
	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static bool ClampSlice(const INPUT_TYPE &value, INDEX_TYPE &begin, INDEX_TYPE &end, bool begin_valid, bool end_valid) {
	// Clamp offsets
	begin = begin_valid ? begin : 0;
	end = end_valid ? end : ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (!ClampIndex(begin, value) || !ClampIndex(end, value)) {
		return false;
	}
	end = MaxValue<INDEX_TYPE>(begin, end);

	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValue(Vector &result, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end) {
	return input;
}

template <>
list_entry_t SliceValue(Vector &result, list_entry_t input, int64_t begin, int64_t end) {
	input.offset += begin;
	input.length = end - begin;
	return input;
}

template <>
string_t SliceValue(Vector &result, string_t input, int32_t begin, int32_t end) {
	// one-based - zero has strange semantics
	return SubstringFun::SubstringScalarFunction(result, input, begin + 1, end - begin);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteSlice(Vector &result, Vector &s, Vector &b, Vector &e, const idx_t count) {
	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto rdata = ConstantVector::GetData<INPUT_TYPE>(result);
		auto sdata = ConstantVector::GetData<INPUT_TYPE>(s);
		auto bdata = ConstantVector::GetData<INDEX_TYPE>(b);
		auto edata = ConstantVector::GetData<INDEX_TYPE>(e);

		auto sliced = sdata[0];
		auto begin = (bdata[0] > 0) ? bdata[0] - 1 : bdata[0];
		auto end = edata[0];

		auto svalid = !ConstantVector::IsNull(s);
		auto bvalid = !ConstantVector::IsNull(b);
		auto evalid = !ConstantVector::IsNull(e);

		// Try to slice
		if (!svalid || !ClampSlice(sliced, begin, end, bvalid, evalid)) {
			ConstantVector::SetNull(result, true);
		} else {
			rdata[0] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end);
		}
	} else {
		UnifiedVectorFormat sdata, bdata, edata;

		s.ToUnifiedFormat(count, sdata);
		b.ToUnifiedFormat(count, bdata);
		e.ToUnifiedFormat(count, edata);

		auto rdata = FlatVector::GetData<INPUT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; ++i) {
			auto sidx = sdata.sel->get_index(i);
			auto bidx = bdata.sel->get_index(i);
			auto eidx = edata.sel->get_index(i);

			auto sliced = ((INPUT_TYPE *)sdata.data)[sidx];
			auto begin = ((INDEX_TYPE *)bdata.data)[bidx];
			auto end = ((INDEX_TYPE *)edata.data)[eidx];

			begin = (begin > 0) ? begin - 1 : begin;

			auto svalid = sdata.validity.RowIsValid(sidx);
			auto bvalid = bdata.validity.RowIsValid(bidx);
			auto evalid = edata.validity.RowIsValid(eidx);

			// Try to slice
			if (!svalid || !ClampSlice(sliced, begin, end, bvalid, evalid)) {
				rmask.SetInvalid(i);
			} else {
				rdata[i] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end);
			}
		}
	}

	result.Verify(count);
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	Vector &s = args.data[0];
	Vector &b = args.data[1];
	Vector &e = args.data[2];

	s.Flatten(count);
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST:
		// Share the value dictionary as we are just going to slice it
		ListVector::ReferenceEntry(result, s);
		ExecuteSlice<list_entry_t, int64_t>(result, s, b, e, count);
		break;
	case LogicalTypeId::VARCHAR:
		ExecuteSlice<string_t, int32_t>(result, s, b, e, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			break;
		}
	}
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 3);
	switch (arguments[0]->return_type.id()) {
	case LogicalTypeId::LIST:
		// The result is the same type
		bound_function.return_type = arguments[0]->return_type;
		break;
	case LogicalTypeId::VARCHAR:
		// string slice returns a string, but can only accept 32 bit integers
		bound_function.return_type = arguments[0]->return_type;
		bound_function.arguments[1] = LogicalType::INTEGER;
		bound_function.arguments[2] = LogicalType::INTEGER;
		break;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		break;
	default:
		throw BinderException("ARRAY_SLICE can only operate on LISTs and VARCHARs");
	}

	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void ArraySliceFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun({LogicalType::ANY, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::ANY,
	                   ArraySliceFunction, ArraySliceBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction({"array_slice", "list_slice"}, fun);
}

} // namespace duckdb
