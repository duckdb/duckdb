#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "core_functions/aggregate/distributive_functions.hpp"

namespace duckdb {

// Algorithms from
// "New cardinality estimation algorithms for HyperLogLog sketches"
// Otmar Ertl, arXiv:1702.01284
namespace {

struct ApproxDistinctCountState {
	HyperLogLogP<10> hll;
};

struct ApproxCountDistinctFunction {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.hll.Merge(source.hll);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = UnsafeNumericCast<T>(state.hll.Count());
	}

	static bool IgnoreNull() {
		return true;
	}
};

void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                                       idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];

	auto input_validity = input.Validity();

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	Vector hash_vec(LogicalType::HASH, count);
	VectorOperations::Hash(input, hash_vec, count);

	auto states = state_vector.Values<ApproxDistinctCountState *>();
	auto hashes = hash_vec.Values<hash_t>();
	for (idx_t i = 0; i < count; i++) {
		if (!input_validity.IsValid(i)) {
			continue;
		}
		auto agg_state = states[i].GetValue();
		const auto hash = hashes[i].GetValue();
		agg_state->hll.InsertElement(hash);
	}
}

// The exported state is the sketch's registers as a BLOB: a HyperLogLog is a fixed array of 2^P
// one-byte registers, and merging two sketches takes the per-register maximum, so an exported state
// combines (combine_aggr) and finalizes exactly as the in-memory one does.  The static field layouts
// describe structs of scalars and lists, not a fixed byte array, hence the explicit callbacks.
AggregateStateLayout ApproxCountDistinctGetStateType(AggregateLayoutInput &) {
	AggregateStateLayout layout;
	layout.type = LogicalType::BLOB;
	layout.total_state_size = AlignValue<idx_t>(sizeof(ApproxDistinctCountState));
	return layout;
}

void ApproxCountDistinctExportState(Vector &state_vector, AggregateFinalizeInputData &, Vector &result, idx_t count,
                                    idx_t offset) {
	auto states = state_vector.Values<ApproxDistinctCountState *>();
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		result_data[offset + i] =
		    StringVector::AddStringOrBlob(result, const_char_ptr_cast(state.hll.GetRegisters()), HyperLogLogP<10>::M);
	}
}

void ApproxCountDistinctImportState(AggregateImportInputData &input) {
	const auto &layout = input.layout;
	const auto count = input.input_vec.size();
	UnifiedVectorFormat vdata;
	input.input_vec.ToUnifiedFormat(vdata);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *reinterpret_cast<ApproxDistinctCountState *>(input.dest_buffer + i * layout.total_state_size);
		state.hll = HyperLogLogP<10>();
		const auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue; // a NULL state is an empty sketch
		}
		auto &blob = blobs[idx];
		if (blob.GetSize() != HyperLogLogP<10>::M) {
			throw InvalidInputException("approx_count_distinct state: expected %llu registers, got %llu bytes",
			                            HyperLogLogP<10>::M, blob.GetSize());
		}
		state.hll.SetRegisters(const_data_ptr_cast(blob.GetData()));
	}
}

AggregateFunction GetApproxCountDistinctFunction(const LogicalType &input_type) {
	auto fun = AggregateFunction(
	    {}, LogicalTypeId::BIGINT, AggregateFunction::StateSize<ApproxDistinctCountState>,
	    AggregateFunction::StateInitialize<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    ApproxCountDistinctUpdateFunction,
	    AggregateFunction::StateCombine<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    AggregateFunction::StateFinalize<ApproxDistinctCountState, int64_t, ApproxCountDistinctFunction>, nullptr);
	fun.GetSignature().AddParameter("any", input_type);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetStateExportCallbacks(ApproxCountDistinctGetStateType, ApproxCountDistinctExportState,
	                            ApproxCountDistinctImportState);
	return fun;
}

} // namespace

AggregateFunction ApproxCountDistinctFun::GetFunction() {
	return GetApproxCountDistinctFunction(LogicalType::ANY);
}

} // namespace duckdb
