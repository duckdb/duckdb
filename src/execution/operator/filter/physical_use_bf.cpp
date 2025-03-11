#include "duckdb/execution/operator/filter/physical_use_bf.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {
namespace {
static void BloomFilterExecute(const vector<Vector> &result, const shared_ptr<BlockedBloomFilter> &bloom_filter,
                               SelectionVector &sel, idx_t &approved_tuple_count, idx_t row_num) {
	if (!bloom_filter->finalized_) {
		approved_tuple_count = 0;
		return;
	}
	idx_t result_count = 0;
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(const_cast<Vector &>(result[bloom_filter->BoundColsApplied[0]]), hashes, row_num);
	for (int i = 1; i < bloom_filter->BoundColsApplied.size(); i++) {
		VectorOperations::CombineHash(hashes, const_cast<Vector &>(result[bloom_filter->BoundColsApplied[i]]), row_num);
	}
	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		hashes.Flatten(row_num);
	}
	bloom_filter->Find(arrow::internal::CpuInfo::AVX2, row_num, (hash_t *)hashes.GetData(), sel, result_count, false);

	approved_tuple_count = result_count;
}
} // namespace

PhysicalUseBF::PhysicalUseBF(vector<LogicalType> types, vector<shared_ptr<BlockedBloomFilter>> bf,
                             const vector<PhysicalCreateBF *> &related_create_bfs, idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::USE_BF, std::move(types), estimated_cardinality),
      bf_to_use(std::move(bf)), related_create_bfs(related_create_bfs) {
}

unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CachingOperatorState>();
}

InsertionOrderPreservingMap<string> PhysicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_use.size());
	result["Hash Column Number"] = std::to_string(bf_to_use[0]->BoundColsApplied.size());
	string bfs;
	for (auto *bf : related_create_bfs) {
		bfs += "0x" + std::to_string(reinterpret_cast<size_t>(bf)) + "\n";
	}
	result["BF Creators"] = bfs;
	return result;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	for (auto cell : related_create_bfs) {
		cell->BuildPipelinesFromRelated(current, meta_pipeline);
	}
	children[0]->BuildPipelines(current, meta_pipeline);
}

OperatorResultType PhysicalUseBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state_p) const {
	idx_t row_num = input.size();
	idx_t result_count = input.size();
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	auto bf = bf_to_use[0];

	BloomFilterExecute(input.data, bf, sel, result_count, row_num);

	if (result_count == row_num) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}
} // namespace duckdb
