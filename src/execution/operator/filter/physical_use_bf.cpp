#include "duckdb/execution/operator/filter/physical_use_bf.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <utility>

namespace duckdb {
PhysicalUseBF::PhysicalUseBF(vector<LogicalType> types, const shared_ptr<FilterPlan> &filter_plan,
                             shared_ptr<BloomFilter> bf, PhysicalCreateBF *related_create_bfs,
                             idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::USE_BF, std::move(types), estimated_cardinality),
      filter_plan(filter_plan), related_creator(related_create_bfs), bf_to_use(std::move(bf)) {
}

class UseBFState : public CachingOperatorState {
public:
	static constexpr int64_t NUM_CHUNK_FOR_CHECK = 32;
	static constexpr double SELECTIVITY_THRESHOLD = 0.9;

public:
	explicit UseBFState() : sel_vector(STANDARD_VECTOR_SIZE), lookup_results(STANDARD_VECTOR_SIZE), is_checked(false) {
	}

	SelectionVector sel_vector;
	vector<uint32_t> lookup_results;

	bool use_bf = true;
	atomic<bool> is_checked;
	int64_t num_chunk = 0;
	uint64_t num_received = 0;
	uint64_t num_sent = 0;

public:
	void CheckBFSelectivity(uint64_t num_in, uint64_t num_out) {
		num_received += num_in;
		num_sent += num_out;
		num_chunk++;

		if (num_chunk > NUM_CHUNK_FOR_CHECK) {
			is_checked = true;

			double selectivity = static_cast<double>(num_sent) / static_cast<double>(num_received);
			if (selectivity > SELECTIVITY_THRESHOLD) {
				use_bf = false;
			}
		}
	}

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<UseBFState>();
}

InsertionOrderPreservingMap<string> PhysicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["BF Creators"] = "0x" + std::to_string(reinterpret_cast<size_t>(related_creator)) + "\n";
	return result;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	related_creator->BuildPipelinesFromRelated(current, meta_pipeline);
	children[0].get().BuildPipelines(current, meta_pipeline);
}

OperatorResultType PhysicalUseBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<UseBFState>();

	// This operator has no BloomFilter to use
	if (!bf_to_use || !bf_to_use->finalized_ || !state.use_bf) {
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// 1. Lookup the BloomFilter
	auto count = input.size();
	auto &results = state.lookup_results;
	bf_to_use->Lookup(input, results);

	// 2. Fill results
	idx_t result_count = 0;
	auto &sel = state.sel_vector;
	for (size_t i = 0; i < count; i++) {
		sel.set_index(result_count, i);
		result_count += results[i];
	}
	if (result_count == count) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, sel, result_count);
	}

	// 3. Update statistics
	if (!state.is_checked) {
		state.CheckBFSelectivity(input.size(), result_count);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}
} // namespace duckdb
