#include "duckdb/execution/operator/order/physical_order.hpp"

#include "blockquicksort_wrapper.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	const size_t ENTRY_SIZE;

	const vector<OrderType> ORDER_TYPES;
	const vector<OrderByNullType> ORDER_BY_NULL_TYPES;
	const vector<LogicalType> TYPES;
	const vector<BaseStatistics *> STATS;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	//! The lock for updating the global order state
	mutex lock;

	BufferManager &buffer_manager;
	unique_ptr<RowChunk> sorting_block;

	//! Orders

	//! To execute the expressions that are sorted
	ExpressionExecutor executor;

	//! Fields concerning the actual sorting
	unique_ptr<SortingState> sorting_state;
};

class OrderLocalState : public LocalSinkState {
public:
	//! Holds a vector of incoming sorting columns
	DataChunk sort;

	//! Used for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto state = make_unique<OrderGlobalState>(buffer_manager);

	size_t entry_size = 0;
	vector<OrderType> order_types;
	vector<OrderByNullType> order_by_null_types;
	vector<LogicalType> types;
	vector<BaseStatistics *> stats;
	for (auto &order : orders) {
		// global state ExpressionExecutor
		auto &expr = *order.expression;
		state->executor.AddExpression(expr);

		// sorting state
		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		types.push_back(expr.return_type);
		if (expr.stats) {
			stats.push_back(expr.stats.get());
		} else {
			stats.push_back(nullptr);
		}

		// compute entry size
		if (expr.stats && expr.stats->has_null) {
			entry_size++;
		}
		auto physical_type = expr.return_type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(expr.return_type.InternalType());
		} else {
			switch (physical_type) {
			case PhysicalType::VARCHAR:
				entry_size += 8;
				break;
			default:
				throw NotImplementedException("Sorting type");
			}
		}
	}
	// make room for an 'index' column at the end
	entry_size += sizeof(idx_t);

	idx_t vectors_per_block =
	    (buffer_manager.GetMaxMemory() / 2 / entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	vectors_per_block = MaxValue(vectors_per_block, Storage::BLOCK_ALLOC_SIZE / entry_size / STANDARD_VECTOR_SIZE);
	state->sorting_block = make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, entry_size);
	state->sorting_state =
	    unique_ptr<SortingState>(new SortingState {entry_size, order_types, order_by_null_types, types, stats});

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto result = make_unique<OrderLocalState>();
	vector<LogicalType> types;
	for (auto &order : orders) {
		types.push_back(order.expression->return_type);
	}
	result->sort.Initialize(types);
	return result;
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = *gstate.sorting_state;

	// obtain sorting columns
	auto &sort = lstate.sort;
	gstate.executor.Execute(input, sort);

	// build and serialize
	idx_t start = gstate.sorting_block->Build(input.size(), lstate.key_locations);
	for (idx_t sort_col = 0; sort_col < sort.data.size(); sort_col++) {
		bool has_null;
		bool invert = false;
		if (sorting_state.STATS[sort_col]) {
			has_null = sorting_state.STATS[sort_col]->has_null;
			invert = sorting_state.ORDER_BY_NULL_TYPES[sort_col] == OrderByNullType::NULLS_LAST;
		} else {
			has_null = false;
		}
		gstate.sorting_block->SerializeVectorSortable(sort.data[sort_col], sort.size(), *lstate.sel_ptr, sort.size(),
		                                              lstate.key_locations, has_null, invert);
	}
	RowChunk::SerializeIndices(lstate.key_locations, start, sort.size());
}

static void RadixSort(BufferManager &buffer_manager, data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset,
                      const idx_t &sorting_size, const SortingState &sorting_state) {
	auto temp_block = buffer_manager.RegisterMemory(
	    MaxValue(count * sorting_state.ENTRY_SIZE, (idx_t)Storage::BLOCK_ALLOC_SIZE), false);
	auto handle = buffer_manager.Pin(temp_block);
	data_ptr_t temp = handle->node->buffer;
	bool swap = false;

	idx_t counts[256];
	u_int8_t byte;
	for (idx_t offset = col_offset + sorting_size - 1; offset + 1 > col_offset; offset--) {
		// init to 0
		memset(counts, 0, sizeof(counts));
		// collect counts
		for (idx_t i = 0; i < count; i++) {
			byte = *(dataptr + i * sorting_state.ENTRY_SIZE + offset);
			counts[byte]++;
		}
		// compute offsets from counts
		for (idx_t val = 1; val < 256; val++) {
			counts[val] = counts[val] + counts[val - 1];
		}
		// re-order the data in temporary array
		for (int i = count - 1; i >= 0; i--) {
			byte = *(dataptr + i * sorting_state.ENTRY_SIZE + offset);
			memcpy(temp + (counts[byte] - 1) * sorting_state.ENTRY_SIZE, dataptr + i * sorting_state.ENTRY_SIZE,
			       sorting_state.ENTRY_SIZE);
			counts[byte]--;
		}
		std::swap(dataptr, temp);
		swap = !swap;
	}

	if (swap) {
		memcpy(temp, dataptr, count * sorting_state.ENTRY_SIZE);
	}
}

static void SortInMemory(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	const auto &sorting_state = *state.sorting_state;
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	auto &block = state.sorting_block->blocks.back();
	auto handle = buffer_manager.Pin(block.block);
	auto dataptr = handle->node->buffer;

	const idx_t sorting_size = sorting_state.ENTRY_SIZE - sizeof(idx_t);
    RadixSort(buffer_manager, dataptr, block.count, 0, sorting_size, sorting_state);
}

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;

	if (state.sorting_block->blocks.size() == 1) {
		SortInMemory(pipeline, context, state);
	} else {
		throw NotImplementedException("External sorting");
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child) : PhysicalOperatorState(op, child) {
	}
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &sink = (OrderGlobalState &)*this->sink_state;
	const auto &sorting_state = *sink.sorting_state;

	chunk.Verify();
}

string PhysicalOrder::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
