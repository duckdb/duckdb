#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/for_vector_arithmetic.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

// decimal scale-up casts multiply the payload by 10^delta: run them narrow when the FOR bounds allow it
template <class DST_T>
bool TryFORScaleUp(Vector &source, Vector &result, idx_t count, uint8_t dst_width, uint64_t mul,
                   buffer_ptr<DictionaryEntry> &dict_cache) {
#ifdef DUCKDB_SMALLER_BINARY
	(void)source;
	(void)result;
	(void)count;
	(void)dst_width;
	(void)mul;
	(void)dict_cache;
	return false;
#else
	FORVector::ScanData<DST_T> scan;
	if (!FORVector::TryGetScanData(source, scan)) {
		return false;
	}
	DST_T result_max;
	if (!TryMultiplyOperator::Operation(scan.max_value, DST_T(mul), result_max)) {
		return false;
	}
	if (FORValueOps<DST_T>::ToUnsignedStorage(result_max) >= Uhugeint::POWERS_OF_TEN[dst_width]) {
		return false;
	}
	PhysicalType res_stored;
	if (!FORVector::TryGetStoredTypeForMax<DST_T>(result_max, res_stored)) {
		return false;
	}
	using EXECUTOR = FORStandardExecutor<MultiplyOperator>;
	const auto compute = EXECUTOR::MaxStored(res_stored, scan.stored_type);
	auto fill = [&](Vector &target, idx_t n) {
		FORVector::Create<DST_T>(target, compute, result_max);
		uint64_t cval = mul;
		BinaryBufferArgs args;
		args.ldata = FORVector::GetData(*scan.for_vec);
		args.lvalidity = scan.validity.get();
		args.rdata = const_data_ptr_cast(&cval);
		args.rconstant = true;
		args.result_data = FORVector::GetData(target);
		args.result_validity = &FORVector::Validity(target);
		args.count = n;
		EXECUTOR::Kernel(scan.stored_type, compute, compute)(args);
	};
	if (scan.sel) {
		const idx_t child_count = scan.for_vec->size();
		if (child_count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		if (!dict_cache || dict_cache->data.GetType() != result.GetType()) {
			dict_cache = make_buffer<DictionaryEntry>(Vector(result.GetType(), STANDARD_VECTOR_SIZE));
			// full-stride allocation and pipeline-local, so consumers may widen it in place
			dict_cache->data.BufferMutable().cache_owned = true;
		}
		fill(dict_cache->data, child_count);
		result.Dictionary(dict_cache, *scan.sel, count);
	} else {
		fill(result, count);
	}
	return true;
#endif
}

bool TryFORDecimalScaleUp(Vector &source, Vector &result, idx_t count, buffer_ptr<DictionaryEntry> &dict_cache) {
	auto &src_type = source.GetType();
	auto &dst_type = result.GetType();
	if (src_type.id() != LogicalTypeId::DECIMAL || dst_type.id() != LogicalTypeId::DECIMAL ||
	    GetTypeIdSize(dst_type.InternalType()) < GetTypeIdSize(src_type.InternalType())) {
		return false;
	}
	const auto src_scale = DecimalType::GetScale(src_type);
	const auto dst_scale = DecimalType::GetScale(dst_type);
	if (dst_scale <= src_scale || dst_scale - src_scale >= 20) {
		return false;
	}
	uint64_t mul = 1;
	for (auto d = src_scale; d < dst_scale; d++) {
		mul *= 10;
	}
	bool ok = false;
	FOR_SWITCH_LOGICAL(dst_type.InternalType(), DST_T, {
		ok = TryFORScaleUp<DST_T>(source, result, count, DecimalType::GetWidth(dst_type), mul, dict_cache);
	});
	return ok;
}

} // namespace

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	result->AddChild(expr.Child());
	result->Finalize();

	if (expr.GetBoundCast().HasInitLocalState()) {
		auto context_ptr = root.executor->HasContext() ? &root.executor->GetContext() : nullptr;
		CastLocalStateParameters parameters(context_ptr, expr.GetBoundCast().GetCastData());
		result->local_state = expr.GetBoundCast().InitLocalState(parameters);
	}
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto lstate = ExecuteFunctionState::GetFunctionState(*state);

	// resolve the child
	state->intermediate_chunk.Reset();

	auto &child = state->intermediate_chunk.data[0];
	auto child_state = state->child_states[0].get();

	Execute(expr.Child(), child_state, sel, count, child);

	string error_message;
	auto error_ref = expr.IsTryCast() ? &error_message : nullptr;
	CastParameters parameters(expr.GetBoundCast().GetCastData(), false, error_ref, lstate);
	parameters.query_location = expr.GetQueryLocation();
	parameters.cast_source = &expr.Child();
	parameters.cast_target = expr;
	idx_t cast_count = count;
	bool all_constant = child.GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (all_constant) {
		// if the input is constant we only need to cast one value
		if (ConstantVector::IsNull(child) && result.GetType().id() != LogicalTypeId::UNION) {
			// if the input is constant NULL the output is always constant NULL
			// ... except for unions, that are special
			ConstantVector::SetNull(result, count_t(count));
			return;
		}
		// temporarily set the input size to 1
		FlatVector::SetSize(child, 1ULL);
		cast_count = 1;
	}
	if (FORVector::TryCastType(child, result, cast_count) ||
	    TryFORDecimalScaleUp(child, result, cast_count, state->Cast<ExecuteFunctionState>().for_dictionary)) {
		// FOR-preserving cast: re-typed or rescaled over a narrow payload, cannot fail
		if (result.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
			FlatVector::SetSize(result, count_t(count));
		}
		return;
	}
	expr.GetBoundCast().Cast(child, result, cast_count, parameters);
	if (all_constant) {
		if (child.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			child.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		// restore the size of the input vector
		FlatVector::SetSize(child, count);
		// ensure the result type is constant
		result.FlattenAndSetConstant();
	}
	FlatVector::SetSize(result, count_t(count));
}

} // namespace duckdb
