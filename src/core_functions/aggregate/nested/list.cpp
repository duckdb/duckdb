#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

static void RecursiveFlatten(Vector &vector, idx_t &count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		vector.Flatten(count);
	}

	auto internal_type = vector.GetType().InternalType();
	if (internal_type == PhysicalType::LIST) {
		auto &child_vector = ListVector::GetEntry(vector);
		auto child_vector_count = ListVector::GetListSize(vector);
		RecursiveFlatten(child_vector, child_vector_count);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(vector);
		for (auto &child : children) {
			RecursiveFlatten(*child, count);
		}
	}
}

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
	static void Initialize(STATE *state) {
		state->linked_list.total_capacity = 0;
		state->linked_list.first_segment = nullptr;
		state->linked_list.last_segment = nullptr;
	}

	template <class STATE>
	static void Destroy(AggregateInputData &aggr_input_data, STATE *state) {
		D_ASSERT(state);
		auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();
		list_bind_data.functions.Destroy(aggr_input_data.allocator, state->linked_list);
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto states = UnifiedVectorFormat::GetData<ListAggState *>(sdata);
	RecursiveFlatten(input, count);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		list_bind_data.functions.AppendRow(aggr_input_data.allocator, state->linked_list, input, i, count);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	UnifiedVectorFormat sdata;
	state.ToUnifiedFormat(count, sdata);
	auto states_ptr = UnifiedVectorFormat::GetData<ListAggState *>(sdata);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (state->linked_list.total_capacity == 0) {
			// NULL, no need to append.
			continue;
		}

		// copy the linked list of the state
		auto copied_linked_list = LinkedList(state->linked_list.total_capacity, nullptr, nullptr);
		list_bind_data.functions.CopyLinkedList(state->linked_list, copied_linked_list, aggr_input_data.allocator);

		// append the copied linked list to the combined state
		if (combined_ptr[i]->linked_list.last_segment) {
			combined_ptr[i]->linked_list.last_segment->next = copied_linked_list.first_segment;
		} else {
			combined_ptr[i]->linked_list.first_segment = copied_linked_list.first_segment;
		}
		combined_ptr[i]->linked_list.last_segment = copied_linked_list.last_segment;
		combined_ptr[i]->linked_list.total_capacity += copied_linked_list.total_capacity;
	}
}

static void ListFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<ListAggState *>(sdata);

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();
	// first iterate over all of the entries and set up the list entries, plus get the newly required total length
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;
		result_data[rid].offset = total_len;
		if (state->linked_list.total_capacity == 0) {
			mask.SetInvalid(rid);
			result_data[rid].length = 0;
			continue;
		}
		// set the length and offset of this list in the result vector
		auto total_capacity = state->linked_list.total_capacity;
		result_data[rid].length = total_capacity;
		total_len += total_capacity;
	}
	// reserve capacity, then iterate over all of the entries again and copy over the data tot he child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetEntry(result);
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;
		if (state->linked_list.total_capacity == 0) {
			continue;
		}

		idx_t current_offset = result_data[rid].offset;
		list_bind_data.functions.BuildListVector(state->linked_list, result_child, current_offset);
	}
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
	                         ListCombineFunction, ListFinalize, nullptr, ListBindFunction,
	                         AggregateFunction::StateDestroy<ListAggState, ListFunction>, nullptr, nullptr);
}

} // namespace duckdb
