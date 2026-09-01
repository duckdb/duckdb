#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"

namespace duckdb {

namespace {

struct KahanSumSetOperation {
	template <class STATE>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE &state, idx_t count) {
		state.is_set = true;
	}
};

struct KahanSumOperation : public BaseSumOperation<KahanSumSetOperation, KahanAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

} // namespace

AggregateFunction KahanSumFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KahanSumState, double, double, KahanSumOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE);
}

} // namespace duckdb
