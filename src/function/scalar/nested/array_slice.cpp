#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

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

template <typename INPUT_TYPE, typename INDEX_TYPE>
bool ClampIndex(INDEX_TYPE &index, const INPUT_TYPE &value) {
	const auto offset = ValueOffset<INPUT_TYPE, INDEX_TYPE>(value);
	const auto length = ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (index < 0) {
		if (-index > length) {
			return false;
		}
		index = offset + length + index;
	} else if (index > length) {
		return false;
	}
	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
void TruncateValue(INPUT_TYPE &value, INDEX_TYPE begin, INDEX_TYPE end) {
}

template <>
void TruncateValue(list_entry_t &list, int64_t begin, int64_t end) {
	list.offset += begin;
	list.length = end - begin;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static bool SliceValue(INPUT_TYPE &value, INDEX_TYPE begin, INDEX_TYPE end, bool begin_valid, bool end_valid) {
	// Clamp offsets
	begin = begin_valid ? begin : 0;
	end = end_valid ? end : ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (!ClampIndex(begin, value) || !ClampIndex(end, value)) {
		return false;
	}
	end = MaxValue<INDEX_TYPE>(begin, end);

	// Trim down the range
	TruncateValue(value, begin, end);

	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteSlice(Vector &result, Vector &l, Vector &b, Vector &e, const idx_t count) {
	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto rdata = ConstantVector::GetData<INPUT_TYPE>(result);
		auto ldata = ConstantVector::GetData<INPUT_TYPE>(l);
		auto bdata = ConstantVector::GetData<INDEX_TYPE>(b);
		auto edata = ConstantVector::GetData<INDEX_TYPE>(e);

		auto list = ldata[0];
		auto begin = bdata[0];
		auto end = edata[0];

		auto lvalid = !ConstantVector::IsNull(l);
		auto bvalid = !ConstantVector::IsNull(b);
		auto evalid = !ConstantVector::IsNull(e);

		// Try to slice
		if (!lvalid || !SliceValue(list, begin, end, bvalid, evalid)) {
			ConstantVector::SetNull(result, true);
		} else {
			rdata[0] = list;
		}
	} else {
		VectorData ldata, bdata, edata;

		l.Orrify(count, ldata);
		b.Orrify(count, bdata);
		e.Orrify(count, edata);

		auto rdata = FlatVector::GetData<INPUT_TYPE>(result);
		auto rmask = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; ++i) {
			auto lidx = ldata.sel->get_index(i);
			auto bidx = bdata.sel->get_index(i);
			auto eidx = edata.sel->get_index(i);

			auto list = ((INPUT_TYPE *)ldata.data)[lidx];
			auto begin = ((INDEX_TYPE *)bdata.data)[bidx];
			auto end = ((INDEX_TYPE *)edata.data)[eidx];

			auto lvalid = ldata.validity.RowIsValid(lidx);
			auto bvalid = bdata.validity.RowIsValid(bidx);
			auto evalid = edata.validity.RowIsValid(eidx);

			// Try to slice
			if (!lvalid || !SliceValue(list, begin, end, bvalid, evalid)) {
				rmask.SetInvalid(i);
			} else {
				rdata[i] = list;
			}
		}
	}

	result.Verify(count);
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().child_types().size() == 1);
	D_ASSERT(args.ColumnCount() == 3);
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	Vector &l = args.data[0];
	Vector &b = args.data[1];
	Vector &e = args.data[2];

	switch (result.GetType().id()) {
	case LogicalTypeId::LIST:
		// Share the value dictionary as we are just going to slice it
		ListVector::ReferenceEntry(result, l);
		ExecuteSlice<list_entry_t, int64_t>(result, l, b, e, count);
		break;
	case LogicalTypeId::VARCHAR:
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		throw BinderException("ARRAY_SLICE can only operate on LISTs");
	}

	// The result is the same type
	bound_function.return_type = arguments[0]->return_type;
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void ArraySliceFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_slice", {LogicalType::ANY, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::LIST,
	                   ArraySliceFunction, false, ArraySliceBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
