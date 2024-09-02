#include "duckdb/execution/operator/helper/physical_set_variable.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

PhysicalSetVariable::PhysicalSetVariable(string name_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::SET_VARIABLE, {LogicalType::BOOLEAN}, estimated_cardinality),
      name(std::move(name_p)) {
}

SourceResultType PhysicalSetVariable::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

class SetVariableGlobalState : public GlobalSinkState {
public:
	SetVariableGlobalState() {
	}

	bool is_set = false;
};

unique_ptr<GlobalSinkState> PhysicalSetVariable::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SetVariableGlobalState>();
}

SinkResultType PhysicalSetVariable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<SetVariableGlobalState>();
	if (chunk.size() != 1 || gstate.is_set) {
		throw InvalidInputException("PhysicalSetVariable can only handle a single value");
	}
	auto &config = ClientConfig::GetConfig(context.client);
	config.SetUserVariable(name, chunk.GetValue(0, 0));
	gstate.is_set = true;
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
