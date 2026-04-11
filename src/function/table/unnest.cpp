#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/execution/operator/projection/physical_unnest.hpp"

namespace duckdb {

struct UnnestBindData : public FunctionData {
	explicit UnnestBindData(vector<LogicalType> input_types_p) : input_types(std::move(input_types_p)) {
	}

	vector<LogicalType> input_types;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnnestBindData>(input_types);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnnestBindData>();
		return input_types == other.input_types;
	}
};

struct UnnestGlobalState : public GlobalTableFunctionState {
	UnnestGlobalState() {
	}

	vector<unique_ptr<Expression>> select_list;

	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
};

struct UnnestLocalState : public LocalTableFunctionState {
	UnnestLocalState() {
	}

	unique_ptr<OperatorState> operator_state;
};

static unique_ptr<FunctionData> UnnestBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.input_table_types.empty()) {
		throw BinderException("UNNEST requires at least one list or array as input");
	}
	for (idx_t i = 0; i < input.input_table_types.size(); i++) {
		auto &input_type = input.input_table_types[i];
		if (input_type.id() != LogicalTypeId::LIST && input_type.id() != LogicalTypeId::ARRAY) {
			throw BinderException("UNNEST requires list or array arguments");
		}
		if (input_type.id() == LogicalTypeId::LIST) {
			return_types.push_back(ListType::GetChildType(input_type));
		} else {
			return_types.push_back(ArrayType::GetChildType(input_type));
		}
		names.push_back("unnest" + (input.input_table_types.size() > 1 ? to_string(i + 1) : string()));
	}
	return make_uniq<UnnestBindData>(input.input_table_types);
}

static unique_ptr<LocalTableFunctionState> UnnestLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<UnnestGlobalState>();

	auto result = make_uniq<UnnestLocalState>();
	result->operator_state = PhysicalUnnest::GetState(context, gstate.select_list);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> UnnestInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<UnnestBindData>();
	auto result = make_uniq<UnnestGlobalState>();

	for (idx_t i = 0; i < bind_data.input_types.size(); i++) {
		unique_ptr<Expression> child = make_uniq<BoundReferenceExpression>(bind_data.input_types[i], i);
		if (child->return_type.id() == LogicalTypeId::ARRAY) {
			child = BoundCastExpression::AddArrayCastToList(context, std::move(child));
		}
		auto bound_unnest = make_uniq<BoundUnnestExpression>(ListType::GetChildType(child->return_type));
		bound_unnest->child = std::move(child);
		result->select_list.push_back(std::move(bound_unnest));
	}
	return std::move(result);
}

static OperatorResultType UnnestFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                         DataChunk &output) {
	auto &state = data_p.global_state->Cast<UnnestGlobalState>();
	auto &lstate = data_p.local_state->Cast<UnnestLocalState>();
	return PhysicalUnnest::ExecuteInternal(context, input, output, *lstate.operator_state, state.select_list, false);
}

void UnnestTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction unnest_function("unnest", {LogicalType::ANY}, nullptr, UnnestBind, UnnestInit, UnnestLocalInit);
	unnest_function.varargs = LogicalType::ANY;
	unnest_function.in_out_function = UnnestFunction;
	set.AddFunction(unnest_function);
}

} // namespace duckdb
