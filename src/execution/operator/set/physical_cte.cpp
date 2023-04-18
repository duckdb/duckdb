#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalCTE::PhysicalCTE(string ctename, idx_t table_index, vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                         unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CTE, std::move(types), estimated_cardinality), table_index(table_index),
      ctename(ctename) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                 DataChunk &input) const {
	working_table->Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();

	auto child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, this);
	child_meta_pipeline->Build(*children[0]);

	auto &state = meta_pipeline.GetState();

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies[cte_scan] = child_meta_pipeline->GetBasePipeline().get();
	}

	children[1]->BuildPipelines(current, meta_pipeline);
}

vector<const PhysicalOperator *> PhysicalCTE::GetSources() const {
	return {this};
}

string PhysicalCTE::ParamsToString() const {
	string result = "";
	result += "\n[INFOSEPARATOR]\n";
	result += ctename;
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("idx: %llu", table_index);
	return result;
}

} // namespace duckdb
