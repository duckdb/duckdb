#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ExecutionContext::ExecutionContext(ClientContext &context) :
	context(context) {

}

ExecutionContext::~ExecutionContext() {

}

void ExecutionContext::Initialize(unique_ptr<PhysicalOperator> plan) {
	pipelines.clear();
	while(!scheduled_pipelines.empty()) {
		scheduled_pipelines.pop();
	}

	physical_plan = move(plan);
	physical_state = physical_plan->GetOperatorState();

	context.profiler.Initialize(physical_plan.get());

	BuildPipelines(physical_plan.get(), pipelines);
	// schedule pipelines that do not have dependents
	for(auto &pipeline : pipelines) {
		Schedule(pipeline.get());
	}
}

void ExecutionContext::Reset() {
	physical_plan = nullptr;
	physical_state = nullptr;
}


void ExecutionContext::BuildPipelines(PhysicalOperator *op, vector<unique_ptr<Pipeline>> &pipelines) {
	if (op->IsSink()) {
		// operator is a sink, build a pipeline
		auto pipeline = make_unique<Pipeline>(*this);
		pipeline->sink = (PhysicalSink*) op;
		pipeline->sink_state = pipeline->sink->GetSinkState();
		switch(op->type) {
		case PhysicalOperatorType::INSERT:
		case PhysicalOperatorType::DELETE:
		case PhysicalOperatorType::UPDATE:
		case PhysicalOperatorType::CREATE:
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::SIMPLE_AGGREGATE:
		case PhysicalOperatorType::WINDOW:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::TOP_N:
			// single operator, set as child
			pipeline->child = op->children[0].get();
			break;
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
			// regular join, create a pipeline with RHS source that sinks into this pipeline
			pipeline->child = op->children[1].get();
			// on the LHS (probe child), we recurse with the current set of pipelines
			BuildPipelines(op->children[0].get(), pipelines);
			break;
		default:
			throw NotImplementedException("Unimplemented sink type!");
		}
		// recurse into the pipeline child
		BuildPipelines(pipeline->child, pipeline->dependents);
		for(auto &child : pipeline->dependents) {
			child->parent = pipeline.get();
		}
		pipelines.push_back(move(pipeline));
	} else {
		// operator is not a sink! recurse in children
		for(auto &child : op->children) {
			BuildPipelines(child.get(), pipelines);
		}
	}
};

void ExecutionContext::Schedule(Pipeline *pipeline) {
	if (pipeline->dependents.size() > 0) {
		// cannot schedule this pipeline: try to schedule its dependents
		for(auto &dependent : pipeline->dependents) {
			Schedule(dependent.get());
		}
	} else {
		scheduled_pipelines.push(pipeline);
	}
}

vector<TypeId> ExecutionContext::GetTypes() {
	assert(physical_plan);
	return physical_plan->GetTypes();
}

unique_ptr<DataChunk> ExecutionContext::FetchChunk() {
	if (pipelines.size() > 0) {
		// execute scheduled pipelines until we are done
		while(!scheduled_pipelines.empty()) {
			auto pipeline = scheduled_pipelines.front();
			scheduled_pipelines.pop();

			pipeline->Execute(context);
		}
		assert(pipelines.size() == 0);
	}
	assert(physical_plan);

	auto chunk = make_unique<DataChunk>();
	// run the plan to get the next chunks
	physical_plan->InitializeChunk(*chunk);
	physical_plan->GetChunk(context, *chunk, physical_state.get());
	return chunk;
}

void ExecutionContext::EraseDependent(Pipeline *pipeline) {
	pipelines.erase(std::find_if(pipelines.begin(), pipelines.end(),  [&](std::unique_ptr<Pipeline>& p) {
		return p.get() == pipeline;
	}));

}

}
