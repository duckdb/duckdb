#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct ListBindData : public FunctionData {
	explicit ListBindData(const LogicalType &stype_p);
	~ListBindData() override;

	LogicalType stype;
	ListSegmentFunctions functions;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ListBindData>(stype);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ListBindData>();
		return stype == other.stype;
	}
};

ListBindData::ListBindData(const LogicalType &stype_p) : stype(stype_p) {
	// always unnest once because the result vector is of type LIST
	auto type = ListType::GetChildType(stype_p);
	GetSegmentDataFunctions(functions, type);
}

ListBindData::~ListBindData() {
}

struct ListAggState {
	LinkedList linked_list;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.linked_list.total_capacity = 0;
		state.linked_list.first_segment = nullptr;
		state.linked_list.last_segment = nullptr;
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {

	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, count, input_data);

	UnifiedVectorFormat states_data;
	state_vector.ToUnifiedFormat(count, states_data);
	auto states = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[states_data.sel->get_index(i)];
		list_bind_data.functions.AppendRow(aggr_input_data.allocator, state.linked_list, input_data, i);
	}
}

static void ListCombineFunction(Vector &states_vector, Vector &combined, AggregateInputData &, idx_t count) {

	UnifiedVectorFormat states_data;
	states_vector.ToUnifiedFormat(count, states_data);
	auto states_ptr = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states_ptr[states_data.sel->get_index(i)];
		if (state.linked_list.total_capacity == 0) {
			// NULL, no need to append
			// this can happen when adding a FILTER to the grouping, e.g.,
			// LIST(i) FILTER (WHERE i <> 3)
			continue;
		}

		if (combined_ptr[i]->linked_list.total_capacity == 0) {
			combined_ptr[i]->linked_list = state.linked_list;
			continue;
		}

		// append the linked list
		combined_ptr[i]->linked_list.last_segment->next = state.linked_list.first_segment;
		combined_ptr[i]->linked_list.last_segment = state.linked_list.last_segment;
		combined_ptr[i]->linked_list.total_capacity += state.linked_list.total_capacity;
	}
}

static void ListFinalize(Vector &states_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {

	UnifiedVectorFormat states_data;
	states_vector.ToUnifiedFormat(count, states_data);
	auto states = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	// first iterate over all entries and set up the list entries, and get the newly required total length
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states[states_data.sel->get_index(i)];
		const auto rid = i + offset;
		result_data[rid].offset = total_len;
		if (state.linked_list.total_capacity == 0) {
			mask.SetInvalid(rid);
			result_data[rid].length = 0;
			continue;
		}

		// set the length and offset of this list in the result vector
		auto total_capacity = state.linked_list.total_capacity;
		result_data[rid].length = total_capacity;
		total_len += total_capacity;
	}

	// reserve capacity, then iterate over all entries again and copy over the data to the child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetEntry(result);
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states[states_data.sel->get_index(i)];
		const auto rid = i + offset;
		if (state.linked_list.total_capacity == 0) {
			continue;
		}

		idx_t current_offset = result_data[rid].offset;
		list_bind_data.functions.BuildListVector(state.linked_list, result_child, current_offset);
	}

	ListVector::SetListSize(result, total_len);
}

static void ListWindow(Vector inputs[], const ValidityMask &filter_mask, AggregateInputData &aggr_input_data,
                       idx_t input_count, data_ptr_t state, const FrameBounds &frame, const FrameBounds &prev,
                       Vector &result, idx_t rid, idx_t bias) {

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();
	LinkedList linked_list;

	// UPDATE step

	D_ASSERT(input_count == 1);
	auto &input = inputs[0];

	// FIXME: we unify more values than necessary (count is frame.end)
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, frame.end, input_data);

	for (idx_t i = frame.start; i < frame.end; i++) {
		list_bind_data.functions.AppendRow(aggr_input_data.allocator, linked_list, input_data, i);
	}

	// FINALIZE step

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	// set the length and offset of this list in the result vector
	result_data[rid].offset = total_len;
	result_data[rid].length = linked_list.total_capacity;
	D_ASSERT(linked_list.total_capacity != 0);
	total_len += linked_list.total_capacity;

	// reserve capacity, then copy over the data to the child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetEntry(result);
	idx_t offset = result_data[rid].offset;
	list_bind_data.functions.BuildListVector(linked_list, result_child, offset);

	ListVector::SetListSize(result, total_len);
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	D_ASSERT(function.arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		function.arguments[0] = LogicalTypeId::UNKNOWN;
		function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return make_uniq<ListBindData>(function.return_type);
}

AggregateFunction ListFun::GetFunction() {
	return AggregateFunction({LogicalType::ANY}, LogicalTypeId::LIST, AggregateFunction::StateSize<ListAggState>,
	                         AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                         ListCombineFunction, ListFinalize, nullptr, ListBindFunction, nullptr, nullptr,
	                         ListWindow);
}

} // namespace duckdb
