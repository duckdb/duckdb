#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

namespace duckdb {

string PhysicalOperator::GetName() const {
	return PhysicalOperatorToString(type);
}

string PhysicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

// LCOV_EXCL_START
void PhysicalOperator::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
unique_ptr<OperatorState> PhysicalOperator::GetOperatorState(ClientContext &context) const {
	return make_unique<OperatorState>();
}

unique_ptr<GlobalOperatorState> PhysicalOperator::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<GlobalOperatorState>();
}

OperatorResultType PhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<LocalSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<GlobalSourceState>();
}

// LCOV_EXCL_START
void PhysicalOperator::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
SinkResultType PhysicalOperator::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                      DataChunk &input) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}
// LCOV_EXCL_STOP

void PhysicalOperator::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
}

SinkFinalizeType PhysicalOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<LocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<GlobalSinkState>();
}

} // namespace duckdb
