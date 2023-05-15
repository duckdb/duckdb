#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

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
	begin = (begin > 0) ? begin - 1 : begin;
	end = end_valid ? end : ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (!ClampIndex(begin, value) || !ClampIndex(end, value)) {
		return false;
	}
	end = MaxValue<INDEX_TYPE>(begin, end);

	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValue(Vector &result, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end, idx_t step) {
	return input;
}

template <>
list_entry_t SliceValue(Vector &result, list_entry_t input, int64_t begin, int64_t end, idx_t step) {
	input.offset += begin;
	if (step != 1) {
        input.length = (end - begin) / step;
    } else {
	    input.length = end - begin;
	}
	return input;
}

template <>
string_t SliceValue(Vector &result, string_t input, int32_t begin, int32_t end, idx_t step) {
	// one-based - zero has strange semantics
	return SubstringFun::SubstringUnicode(result, input, begin + 1, end - begin);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValueWithSteps(Vector &result, SelectionVector &sel, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end, idx_t step, idx_t &sel_idx) {
    return input;
}

template <>
list_entry_t SliceValueWithSteps(Vector &result, SelectionVector &sel, list_entry_t input, int64_t begin, int64_t end, idx_t step, idx_t &sel_idx) {
    input.offset += begin;
    if (step != 1) {
        input.length = (end - begin) / step;
    } else {
        input.length = end - begin;
    }
	idx_t len = (end - begin) / step;
	auto child_idx = begin;
    for (; sel_idx < len; ++sel_idx) {
		sel.set_index(sel_idx, child_idx);
		child_idx += step;
	}
    return input;
}

template <>
string_t SliceValueWithSteps(Vector &result, SelectionVector &sel, string_t input, int32_t begin, int32_t end, idx_t step, idx_t &sel_idx) {
    // one-based - zero has strange semantics
    return SubstringFun::SubstringUnicode(result, input, begin + 1, end - begin);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteSlice(Vector &result, Vector &v, Vector &b, Vector &e, const idx_t count, optional_ptr<Vector> s) {
	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto rdata = ConstantVector::GetData<INPUT_TYPE>(result);
		auto vdata = ConstantVector::GetData<INPUT_TYPE>(v);
		auto bdata = ConstantVector::GetData<INDEX_TYPE>(b);
		auto edata = ConstantVector::GetData<INDEX_TYPE>(e);
		auto sdata = s ? ConstantVector::GetData<INDEX_TYPE>(*s) : nullptr;

		auto sliced = vdata[0];
		auto begin = bdata[0];
		auto end = edata[0];
		auto step = sdata ? sdata[0] : 1;

		auto vvalid = !ConstantVector::IsNull(v);
		auto bvalid = !ConstantVector::IsNull(b);
		auto evalid = !ConstantVector::IsNull(e);
		auto svalid = s && !ConstantVector::IsNull(*s);

		// Try to slice
		if (!vvalid || !ClampSlice(sliced, begin, end, bvalid, evalid)) {
			ConstantVector::SetNull(result, true);
		} else {
			rdata[0] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end, step);
		}
	} else {
		UnifiedVectorFormat vdata, bdata, edata, sdata;

		v.ToUnifiedFormat(count, vdata);
		b.ToUnifiedFormat(count, bdata);
		e.ToUnifiedFormat(count, edata);
		if (s) {
            s->ToUnifiedFormat(count, sdata);
        }

		auto rdata = FlatVector::GetData<INPUT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		SelectionVector sel;
		idx_t sel_idx = 0;
		if (s) {
		    sel.Initialize(count);
		}

		for (idx_t i = 0; i < count; ++i) {
			auto vidx = vdata.sel->get_index(i);
			auto bidx = bdata.sel->get_index(i);
			auto eidx = edata.sel->get_index(i);
			auto sidx = s ? sdata.sel->get_index(i) : 0;

			auto sliced = ((INPUT_TYPE *)vdata.data)[vidx];
			auto begin = ((INDEX_TYPE *)bdata.data)[bidx];
			auto end = ((INDEX_TYPE *)edata.data)[eidx];
			auto step = s ? ((INDEX_TYPE *)sdata.data)[sidx] : 1;

			auto vvalid = vdata.validity.RowIsValid(vidx);
			auto bvalid = bdata.validity.RowIsValid(bidx);
			auto evalid = edata.validity.RowIsValid(eidx);
			auto svalid = s && sdata.validity.RowIsValid(sidx);

			if (step == 1) {
                // Try to slice
                if (!vvalid || !ClampSlice(sliced, begin, end, bvalid, evalid)) {
                    rmask.SetInvalid(i);
                } else {
                    rdata[i] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end, step);
                }
            } else {
				rdata[i] = SliceValueWithSteps<INPUT_TYPE, INDEX_TYPE>(result,sel, sliced, begin, end, step, sel_idx);
			}
		}
		if (s) {
			result.Slice(sel, count);
		}
	}

	result.Verify(count);
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 4);
	D_ASSERT(args.data.size() == 3 || args.data.size() == 4);
	auto count = args.size();

	Vector &v = args.data[0];
	Vector &b = args.data[1];
	Vector &e = args.data[2];

	optional_ptr<Vector> s;
	if (args.ColumnCount() == 4) {
		s = &args.data[3];
	}

	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST:
		// Share the value dictionary as we are just going to slice it
		if (v.GetVectorType() != VectorType::FLAT_VECTOR && v.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			v.Flatten(count);
		}
		ListVector::ReferenceEntry(result, v);
		ExecuteSlice<list_entry_t, int64_t>(result, v, b, e, count, s);
		break;
	case LogicalTypeId::VARCHAR:
		ExecuteSlice<string_t, int32_t>(result, v, b, e, count, s);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 3 || bound_function.arguments.size() == 4);
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
		if (bound_function.arguments.size() == 4) {
            bound_function.arguments[3] = LogicalType::INTEGER;
        }
		break;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		break;
	default:
		throw BinderException("ARRAY_SLICE can only operate on LISTs and VARCHARs");
	}

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListSliceFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun({LogicalType::ANY, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::ANY,
	                   ArraySliceFunction, ArraySliceBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
