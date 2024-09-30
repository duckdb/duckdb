#include "duckdb/execution/operator/set/physical_union.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality, bool allow_out_of_order)
    : PhysicalOperator(PhysicalOperatorType::UNION, std::move(types), estimated_cardinality),
      allow_out_of_order(allow_out_of_order) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalUnion::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	// order matters if any of the downstream operators are order dependent,
	// or if the sink preserves order, but does not support batch indices to do so
	auto sink = meta_pipeline.GetSink();
	bool order_matters = false;
	if (!allow_out_of_order) {
		order_matters = true;
	}
	if (current.IsOrderDependent()) {
		order_matters = true;
	}
	if (sink) {
		if (sink->SinkOrderDependent() || sink->RequiresBatchIndex()) {
			order_matters = true;
		}
		if (!sink->ParallelSink()) {
			order_matters = true;
		}
	}

	// create a union pipeline that has identical dependencies to 'current'
	auto &union_pipeline = meta_pipeline.CreateUnionPipeline(current, order_matters);

	// continue with the current pipeline
	children[0]->BuildPipelines(current, meta_pipeline);

	vector<shared_ptr<Pipeline>> dependencies;
	optional_ptr<MetaPipeline> last_child_ptr;
	const auto can_saturate_threads = children[0]->CanSaturateThreads(current.GetClientContext());
	if (order_matters || can_saturate_threads) {
		// we add dependencies if order matters: union_pipeline comes after all pipelines created by building current
		dependencies = meta_pipeline.AddDependenciesFrom(union_pipeline, union_pipeline, false);
		// we also add dependencies if the LHS child can saturate all available threads
		// in that case, we recursively make all RHS children depend on the LHS.
		// This prevents breadth-first plan evaluation
		if (can_saturate_threads) {
			last_child_ptr = meta_pipeline.GetLastChild();
		}
	}

	// build the union pipeline
	children[1]->BuildPipelines(union_pipeline, meta_pipeline);

	if (last_child_ptr) {
		// the pointer was set, set up the dependencies
		meta_pipeline.AddRecursiveDependencies(dependencies, *last_child_ptr);
	}

	// Assign proper batch index to the union pipeline
	// This needs to happen after the pipelines have been built because unions can be nested
	meta_pipeline.AssignNextBatchIndex(union_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalUnion::GetSources() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		auto child_sources = child->GetSources();
		result.insert(result.end(), child_sources.begin(), child_sources.end());
	}
	return result;
}

} // namespace duckdb
