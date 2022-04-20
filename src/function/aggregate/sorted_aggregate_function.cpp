#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

struct SortedAggregateBindData : public FunctionData {

	// TODO: Collection sorting does not handle OrderByNullType correctly
	// so this is the third hack around it...
	static OrderByNullType NormaliseNullOrder(OrderType sense, OrderByNullType null_order) {
		if (sense != OrderType::DESCENDING) {
			return null_order;
		}

		switch (null_order) {
		case OrderByNullType::NULLS_FIRST:
			return OrderByNullType::NULLS_LAST;
		case OrderByNullType::NULLS_LAST:
			return OrderByNullType::NULLS_FIRST;
		default:
			throw InternalException("Unknown NULL order sense");
		}
	}

	SortedAggregateBindData(const AggregateFunction &function_p, vector<unique_ptr<Expression>> &children,
	                        unique_ptr<FunctionData> bind_info_p, const BoundOrderModifier &order_bys)
	    : function(function_p), bind_info(move(bind_info_p)) {
		arg_types.reserve(children.size());
		for (const auto &child : children) {
			arg_types.emplace_back(child->return_type);
		}
		for (auto &order : order_bys.orders) {
			order_sense.emplace_back(order.type);
			null_order.emplace_back(NormaliseNullOrder(order.type, order.null_order));
			sort_types.emplace_back(order.expression->return_type);
		}
	}

	SortedAggregateBindData(const SortedAggregateBindData &other)
	    : function(other.function), arg_types(other.arg_types), order_sense(other.order_sense),
	      null_order(other.null_order), sort_types(other.sort_types) {
		if (other.bind_info) {
			bind_info = other.bind_info->Copy();
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<SortedAggregateBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const SortedAggregateBindData &)other_p;
		if (bind_info && other.bind_info) {
			if (!bind_info->Equals(*other.bind_info)) {
				return false;
			}
		} else if (bind_info || other.bind_info) {
			return false;
		}
		return function == other.function && order_sense == other.order_sense && null_order == other.null_order &&
		       sort_types == other.sort_types;
	}

	AggregateFunction function;
	vector<LogicalType> arg_types;
	unique_ptr<FunctionData> bind_info;

	vector<OrderType> order_sense;
	vector<OrderByNullType> null_order;
	vector<LogicalType> sort_types;
};

struct SortedAggregateState {
	SortedAggregateState() : nsel(0) {
	}

	ChunkCollection arguments;
	ChunkCollection ordering;

	// Selection for scattering
	SelectionVector sel;
	idx_t nsel;
};

struct SortedAggregateFunction {
	template <typename STATE>
	static void Initialize(STATE *state) {
		new (state) STATE();
	}

	template <typename STATE>
	static void Destroy(STATE *state) {
		state->~STATE();
	}

	static void ProjectInputs(Vector inputs[], SortedAggregateBindData *order_bind, idx_t input_count, idx_t count,
	                          DataChunk &arg_chunk, DataChunk &sort_chunk) {
		idx_t col = 0;

		arg_chunk.InitializeEmpty(order_bind->arg_types);
		for (auto &dst : arg_chunk.data) {
			dst.Reference(inputs[col++]);
		}
		arg_chunk.SetCardinality(count);

		sort_chunk.InitializeEmpty(order_bind->sort_types);
		for (auto &dst : sort_chunk.data) {
			dst.Reference(inputs[col++]);
		}
		sort_chunk.SetCardinality(count);
	}

	static void SimpleUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		const auto order_bind = (SortedAggregateBindData *)bind_data;
		DataChunk arg_chunk;
		DataChunk sort_chunk;
		ProjectInputs(inputs, order_bind, input_count, count, arg_chunk, sort_chunk);

		const auto order_state = (SortedAggregateState *)state;
		order_state->arguments.Append(arg_chunk);
		order_state->ordering.Append(sort_chunk);
	}

	static void ScatterUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		if (!count) {
			return;
		}

		// Append the arguments to the two sub-collections
		const auto order_bind = (SortedAggregateBindData *)bind_data;
		DataChunk arg_inputs;
		DataChunk sort_inputs;
		ProjectInputs(inputs, order_bind, input_count, count, arg_inputs, sort_inputs);

		// We have to scatter the chunks one at a time
		// so build a selection vector for each one.
		VectorData svdata;
		states.Orrify(count, svdata);

		// Build the selection vector for each state.
		auto sdata = (SortedAggregateState **)svdata.data;
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->sel.data()) {
				order_state->sel.Initialize();
			}
			order_state->sel.set_index(order_state->nsel++, i);
		}

		// Append nonempty slices to the arguments
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->nsel) {
				continue;
			}

			DataChunk arg_chunk;
			arg_chunk.InitializeEmpty(arg_inputs.GetTypes());
			arg_chunk.Slice(arg_inputs, order_state->sel, order_state->nsel);
			order_state->arguments.Append(arg_chunk);

			DataChunk sort_chunk;
			sort_chunk.InitializeEmpty(sort_inputs.GetTypes());
			sort_chunk.Slice(sort_inputs, order_state->sel, order_state->nsel);
			order_state->ordering.Append(sort_chunk);

			// Mark the slice as empty now we have consumed it.
			order_state->nsel = 0;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, FunctionData *bind_data) {
		if (source.arguments.Count() == 0) {
			return;
		}
		target->arguments.Append(const_cast<ChunkCollection &>(source.arguments));
		target->ordering.Append(const_cast<ChunkCollection &>(source.ordering));
	}

	static void Finalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count, idx_t offset) {
		const auto order_bind = (SortedAggregateBindData *)bind_data;

		//	 Reusable inner state
		vector<data_t> agg_state(order_bind->function.state_size());
		Vector agg_state_vec(Value::POINTER((idx_t)agg_state.data()));

		// Sorting buffer
		vector<idx_t> reordering;

		// State variables
		const auto input_count = order_bind->function.arguments.size();
		auto bind_info = order_bind->bind_info.get();

		// Inner aggregate APIs
		auto initialize = order_bind->function.initialize;
		auto destructor = order_bind->function.destructor;
		auto simple_update = order_bind->function.simple_update;
		auto update = order_bind->function.update;
		auto finalize = order_bind->function.finalize;

		auto sdata = FlatVector::GetData<SortedAggregateState *>(states);
		for (idx_t i = 0; i < count; ++i) {
			initialize(agg_state.data());
			auto state = sdata[i];

			// Apply the sort before delegating the chunks
			const auto agg_count = state->ordering.Count();
			if (agg_count > 0) {
				reordering.resize(agg_count);
				state->ordering.Sort(order_bind->order_sense, order_bind->null_order, reordering.data());
				state->arguments.Reorder(reordering.data());
			}

			for (auto &chunk : state->arguments.Chunks()) {
				// These are all simple updates, so use it if available
				if (simple_update) {
					simple_update(chunk->data.data(), bind_info, input_count, agg_state.data(), chunk->size());
				} else {
					// We are only updating a constant state
					agg_state_vec.SetVectorType(VectorType::CONSTANT_VECTOR);
					update(chunk->data.data(), bind_info, input_count, agg_state_vec, chunk->size());
				}
			}

			// Finalize a single value at the next offset
			agg_state_vec.SetVectorType(states.GetVectorType());
			finalize(agg_state_vec, bind_info, result, 1, i + offset);

			if (destructor) {
				destructor(agg_state_vec, 1);
			}
		}
	}
};

unique_ptr<FunctionData> AggregateFunction::BindSortedAggregate(AggregateFunction &bound_function,
                                                                vector<unique_ptr<Expression>> &children,
                                                                unique_ptr<FunctionData> bind_info,
                                                                unique_ptr<BoundOrderModifier> order_bys) {

	auto sorted_bind = make_unique<SortedAggregateBindData>(bound_function, children, move(bind_info), *order_bys);

	// The arguments are the children plus the sort columns.
	for (auto &order : order_bys->orders) {
		children.emplace_back(move(order.expression));
	}

	vector<LogicalType> arguments;
	arguments.reserve(children.size());
	for (const auto &child : children) {
		arguments.emplace_back(child->return_type);
	}

	// Replace the aggregate with the wrapper
	bound_function = AggregateFunction(
	    bound_function.name, arguments, bound_function.return_type, AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::ScatterUpdate,
	    AggregateFunction::StateCombine<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::Finalize, SortedAggregateFunction::SimpleUpdate, nullptr,
	    AggregateFunction::StateDestroy<SortedAggregateState, SortedAggregateFunction>);

	return move(sorted_bind);
}

} // namespace duckdb
