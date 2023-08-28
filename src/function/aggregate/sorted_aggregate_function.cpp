#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

struct SortedAggregateBindData : public FunctionData {
	SortedAggregateBindData(ClientContext &context, BoundAggregateExpression &expr)
	    : buffer_manager(BufferManager::GetBufferManager(context)), function(expr.function),
	      bind_info(std::move(expr.bind_info)), threshold(ClientConfig::GetConfig(context).ordered_aggregate_threshold),
	      external(ClientConfig::GetConfig(context).force_external) {
		auto &children = expr.children;
		arg_types.reserve(children.size());
		for (const auto &child : children) {
			arg_types.emplace_back(child->return_type);
		}
		auto &order_bys = *expr.order_bys;
		sort_types.reserve(order_bys.orders.size());
		for (auto &order : order_bys.orders) {
			orders.emplace_back(order.Copy());
			sort_types.emplace_back(order.expression->return_type);
		}
		sorted_on_args = (children.size() == order_bys.orders.size());
		for (size_t i = 0; sorted_on_args && i < children.size(); ++i) {
			sorted_on_args = children[i]->Equals(*order_bys.orders[i].expression);
		}
	}

	SortedAggregateBindData(const SortedAggregateBindData &other)
	    : buffer_manager(other.buffer_manager), function(other.function), arg_types(other.arg_types),
	      sort_types(other.sort_types), sorted_on_args(other.sorted_on_args), threshold(other.threshold),
	      external(other.external) {
		if (other.bind_info) {
			bind_info = other.bind_info->Copy();
		}
		for (auto &order : other.orders) {
			orders.emplace_back(order.Copy());
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SortedAggregateBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SortedAggregateBindData>();
		if (bind_info && other.bind_info) {
			if (!bind_info->Equals(*other.bind_info)) {
				return false;
			}
		} else if (bind_info || other.bind_info) {
			return false;
		}
		if (function != other.function) {
			return false;
		}
		if (orders.size() != other.orders.size()) {
			return false;
		}
		for (size_t i = 0; i < orders.size(); ++i) {
			if (!orders[i].Equals(other.orders[i])) {
				return false;
			}
		}
		return true;
	}

	BufferManager &buffer_manager;
	AggregateFunction function;
	vector<LogicalType> arg_types;
	unique_ptr<FunctionData> bind_info;

	vector<BoundOrderByNode> orders;
	vector<LogicalType> sort_types;
	bool sorted_on_args;

	//! The sort flush threshold
	const idx_t threshold;
	const bool external;
};

struct SortedAggregateState {
	//! Default buffer size, optimised for small group to avoid blowing out memory.
	static const idx_t BUFFER_CAPACITY = 16;

	SortedAggregateState() : count(0), nsel(0), offset(0) {
	}

	static inline void InitializeBuffer(DataChunk &chunk, const vector<LogicalType> &types) {
		if (!chunk.ColumnCount() && !types.empty()) {
			chunk.Initialize(Allocator::DefaultAllocator(), types, BUFFER_CAPACITY);
		}
	}

	//! Make sure the buffer is large enough for slicing
	static inline void ResetBuffer(DataChunk &chunk, const vector<LogicalType> &types) {
		chunk.Reset();
		chunk.Destroy();
		chunk.Initialize(Allocator::DefaultAllocator(), types);
	}

	void Flush(const SortedAggregateBindData &order_bind) {
		if (ordering) {
			return;
		}

		ordering = make_uniq<ColumnDataCollection>(order_bind.buffer_manager, order_bind.sort_types);
		InitializeBuffer(sort_buffer, order_bind.sort_types);
		ordering->Append(sort_buffer);
		ResetBuffer(sort_buffer, order_bind.sort_types);

		if (!order_bind.sorted_on_args) {
			arguments = make_uniq<ColumnDataCollection>(order_bind.buffer_manager, order_bind.arg_types);
			InitializeBuffer(arg_buffer, order_bind.arg_types);
			arguments->Append(arg_buffer);
			ResetBuffer(arg_buffer, order_bind.arg_types);
		}
	}

	void Update(const SortedAggregateBindData &order_bind, DataChunk &sort_chunk, DataChunk &arg_chunk) {
		count += sort_chunk.size();

		// Lazy instantiation of the buffer chunks
		InitializeBuffer(sort_buffer, order_bind.sort_types);
		if (!order_bind.sorted_on_args) {
			InitializeBuffer(arg_buffer, order_bind.arg_types);
		}

		if (sort_chunk.size() + sort_buffer.size() > STANDARD_VECTOR_SIZE) {
			Flush(order_bind);
		}
		if (arguments) {
			ordering->Append(sort_chunk);
			arguments->Append(arg_chunk);
		} else if (ordering) {
			ordering->Append(sort_chunk);
		} else if (order_bind.sorted_on_args) {
			sort_buffer.Append(sort_chunk, true);
		} else {
			sort_buffer.Append(sort_chunk, true);
			arg_buffer.Append(arg_chunk, true);
		}
	}

	void UpdateSlice(const SortedAggregateBindData &order_bind, DataChunk &sort_inputs, DataChunk &arg_inputs) {
		count += nsel;

		// Lazy instantiation of the buffer chunks
		InitializeBuffer(sort_buffer, order_bind.sort_types);
		if (!order_bind.sorted_on_args) {
			InitializeBuffer(arg_buffer, order_bind.arg_types);
		}

		if (nsel + sort_buffer.size() > STANDARD_VECTOR_SIZE) {
			Flush(order_bind);
		}
		if (arguments) {
			sort_buffer.Reset();
			sort_buffer.Slice(sort_inputs, sel, nsel);
			ordering->Append(sort_buffer);

			arg_buffer.Reset();
			arg_buffer.Slice(arg_inputs, sel, nsel);
			arguments->Append(arg_buffer);
		} else if (ordering) {
			sort_buffer.Reset();
			sort_buffer.Slice(sort_inputs, sel, nsel);
			ordering->Append(sort_buffer);
		} else if (order_bind.sorted_on_args) {
			sort_buffer.Append(sort_inputs, true, &sel, nsel);
		} else {
			sort_buffer.Append(sort_inputs, true, &sel, nsel);
			arg_buffer.Append(arg_inputs, true, &sel, nsel);
		}

		nsel = 0;
		offset = 0;
	}

	void Combine(SortedAggregateBindData &order_bind, SortedAggregateState &other) {
		if (other.arguments) {
			// Force CDC if the other has it
			Flush(order_bind);
			ordering->Combine(*other.ordering);
			arguments->Combine(*other.arguments);
			count += other.count;
		} else if (other.ordering) {
			// Force CDC if the other has it
			Flush(order_bind);
			ordering->Combine(*other.ordering);
			count += other.count;
		} else if (other.sort_buffer.size()) {
			Update(order_bind, other.sort_buffer, other.arg_buffer);
		}
	}

	void PrefixSortBuffer(DataChunk &prefixed) {
		for (column_t col_idx = 0; col_idx < sort_buffer.ColumnCount(); ++col_idx) {
			prefixed.data[col_idx + 1].Reference(sort_buffer.data[col_idx]);
		}
		prefixed.SetCardinality(sort_buffer);
	}

	void Finalize(const SortedAggregateBindData &order_bind, DataChunk &prefixed, LocalSortState &local_sort) {
		if (arguments) {
			ColumnDataScanState sort_state;
			ordering->InitializeScan(sort_state);
			ColumnDataScanState arg_state;
			arguments->InitializeScan(arg_state);
			for (sort_buffer.Reset(); ordering->Scan(sort_state, sort_buffer); sort_buffer.Reset()) {
				PrefixSortBuffer(prefixed);
				arg_buffer.Reset();
				arguments->Scan(arg_state, arg_buffer);
				local_sort.SinkChunk(prefixed, arg_buffer);
			}
			ordering->Reset();
			arguments->Reset();
		} else if (ordering) {
			ColumnDataScanState sort_state;
			ordering->InitializeScan(sort_state);
			for (sort_buffer.Reset(); ordering->Scan(sort_state, sort_buffer); sort_buffer.Reset()) {
				PrefixSortBuffer(prefixed);
				local_sort.SinkChunk(prefixed, sort_buffer);
			}
			ordering->Reset();
		} else if (order_bind.sorted_on_args) {
			PrefixSortBuffer(prefixed);
			local_sort.SinkChunk(prefixed, sort_buffer);
		} else {
			PrefixSortBuffer(prefixed);
			local_sort.SinkChunk(prefixed, arg_buffer);
		}
	}

	idx_t count;
	unique_ptr<ColumnDataCollection> arguments;
	unique_ptr<ColumnDataCollection> ordering;

	DataChunk sort_buffer;
	DataChunk arg_buffer;

	// Selection for scattering
	SelectionVector sel;
	idx_t nsel;
	idx_t offset;
};

struct SortedAggregateFunction {
	template <typename STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <typename STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static void ProjectInputs(Vector inputs[], const SortedAggregateBindData &order_bind, idx_t input_count,
	                          idx_t count, DataChunk &arg_chunk, DataChunk &sort_chunk) {
		idx_t col = 0;

		if (!order_bind.sorted_on_args) {
			arg_chunk.InitializeEmpty(order_bind.arg_types);
			for (auto &dst : arg_chunk.data) {
				dst.Reference(inputs[col++]);
			}
			arg_chunk.SetCardinality(count);
		}

		sort_chunk.InitializeEmpty(order_bind.sort_types);
		for (auto &dst : sort_chunk.data) {
			dst.Reference(inputs[col++]);
		}
		sort_chunk.SetCardinality(count);
	}

	static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		const auto order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_chunk;
		DataChunk sort_chunk;
		ProjectInputs(inputs, order_bind, input_count, count, arg_chunk, sort_chunk);

		const auto order_state = reinterpret_cast<SortedAggregateState *>(state);
		order_state->Update(order_bind, sort_chunk, arg_chunk);
	}

	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		if (!count) {
			return;
		}

		// Append the arguments to the two sub-collections
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_inputs;
		DataChunk sort_inputs;
		ProjectInputs(inputs, order_bind, input_count, count, arg_inputs, sort_inputs);

		// We have to scatter the chunks one at a time
		// so build a selection vector for each one.
		UnifiedVectorFormat svdata;
		states.ToUnifiedFormat(count, svdata);

		// Size the selection vector for each state.
		auto sdata = UnifiedVectorFormat::GetDataNoConst<SortedAggregateState *>(svdata);
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			order_state->nsel++;
		}

		// Build the selection vector for each state.
		vector<sel_t> sel_data(count);
		idx_t start = 0;
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->offset) {
				//	First one
				order_state->offset = start;
				order_state->sel.Initialize(sel_data.data() + order_state->offset);
				start += order_state->nsel;
			}
			sel_data[order_state->offset++] = sidx;
		}

		// Append nonempty slices to the arguments
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->nsel) {
				continue;
			}

			order_state->UpdateSlice(order_bind, sort_inputs, arg_inputs);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &other = const_cast<STATE &>(source);
		target.Combine(order_bind, other);
	}

	static void Window(Vector inputs[], const ValidityMask &filter_mask, AggregateInputData &aggr_input_data,
	                   idx_t input_count, data_ptr_t state, const FrameBounds &frame, const FrameBounds &prev,
	                   Vector &result, idx_t rid, idx_t bias) {
		throw InternalException("Sorted aggregates should not be generated for window clauses");
	}

	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     const idx_t offset) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &buffer_manager = order_bind.buffer_manager;
		RowLayout payload_layout;
		payload_layout.Initialize(order_bind.arg_types);
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), order_bind.arg_types);
		DataChunk sliced;
		sliced.Initialize(Allocator::DefaultAllocator(), order_bind.arg_types);

		//	 Reusable inner state
		vector<data_t> agg_state(order_bind.function.state_size());
		Vector agg_state_vec(Value::POINTER(CastPointerToValue(agg_state.data())));

		// State variables
		auto bind_info = order_bind.bind_info.get();
		ArenaAllocator allocator(Allocator::DefaultAllocator());
		AggregateInputData aggr_bind_info(bind_info, allocator);

		// Inner aggregate APIs
		auto initialize = order_bind.function.initialize;
		auto destructor = order_bind.function.destructor;
		auto simple_update = order_bind.function.simple_update;
		auto update = order_bind.function.update;
		auto finalize = order_bind.function.finalize;

		auto sdata = FlatVector::GetData<SortedAggregateState *>(states);

		vector<idx_t> state_unprocessed(count, 0);
		for (idx_t i = 0; i < count; ++i) {
			state_unprocessed[i] = sdata[i]->count;
		}

		// Sort the input payloads on (state_idx ASC, orders)
		vector<BoundOrderByNode> orders;
		orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
		                                     make_uniq<BoundConstantExpression>(Value::USMALLINT(0))));
		for (const auto &order : order_bind.orders) {
			orders.emplace_back(order.Copy());
		}

		auto global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
		global_sort->external = order_bind.external;
		auto local_sort = make_uniq<LocalSortState>();
		local_sort->Initialize(*global_sort, global_sort->buffer_manager);

		DataChunk prefixed;
		prefixed.Initialize(Allocator::DefaultAllocator(), global_sort->sort_layout.logical_types);

		//	Go through the states accumulating values to sort until we hit the sort threshold
		idx_t unsorted_count = 0;
		idx_t sorted = 0;
		for (idx_t finalized = 0; finalized < count;) {
			if (unsorted_count < order_bind.threshold) {
				auto state = sdata[finalized];
				prefixed.Reset();
				prefixed.data[0].Reference(Value::USMALLINT(finalized));
				state->Finalize(order_bind, prefixed, *local_sort);
				unsorted_count += state_unprocessed[finalized];

				// Go to the next aggregate unless this is the last one
				if (++finalized < count) {
					continue;
				}
			}

			//	If they were all empty (filtering) flush them
			//	(This can only happen on the last range)
			if (!unsorted_count) {
				break;
			}

			//	Sort all the data
			global_sort->AddLocalState(*local_sort);
			global_sort->PrepareMergePhase();
			while (global_sort->sorted_blocks.size() > 1) {
				global_sort->InitializeMergeRound();
				MergeSorter merge_sorter(*global_sort, global_sort->buffer_manager);
				merge_sorter.PerformInMergeRound();
				global_sort->CompleteMergeRound(false);
			}

			auto scanner = make_uniq<PayloadScanner>(*global_sort);
			initialize(agg_state.data());
			while (scanner->Remaining()) {
				chunk.Reset();
				scanner->Scan(chunk);
				idx_t consumed = 0;

				// Distribute the scanned chunk to the aggregates
				while (consumed < chunk.size()) {
					//	Find the next aggregate that needs data
					for (; !state_unprocessed[sorted]; ++sorted) {
						// Finalize a single value at the next offset
						agg_state_vec.SetVectorType(states.GetVectorType());
						finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);
						if (destructor) {
							destructor(agg_state_vec, aggr_bind_info, 1);
						}

						initialize(agg_state.data());
					}
					const auto input_count = MinValue(state_unprocessed[sorted], chunk.size() - consumed);
					for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
						sliced.data[col_idx].Slice(chunk.data[col_idx], consumed, consumed + input_count);
					}
					sliced.SetCardinality(input_count);

					// These are all simple updates, so use it if available
					if (simple_update) {
						simple_update(sliced.data.data(), aggr_bind_info, sliced.data.size(), agg_state.data(),
						              sliced.size());
					} else {
						// We are only updating a constant state
						agg_state_vec.SetVectorType(VectorType::CONSTANT_VECTOR);
						update(sliced.data.data(), aggr_bind_info, sliced.data.size(), agg_state_vec, sliced.size());
					}

					consumed += input_count;
					state_unprocessed[sorted] -= input_count;
				}
			}

			//	Finalize the last state for this sort
			agg_state_vec.SetVectorType(states.GetVectorType());
			finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);
			if (destructor) {
				destructor(agg_state_vec, aggr_bind_info, 1);
			}
			++sorted;

			//	Stop if we are done
			if (finalized >= count) {
				break;
			}

			//	Create a new sort
			scanner.reset();
			global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
			global_sort->external = order_bind.external;
			local_sort = make_uniq<LocalSortState>();
			local_sort->Initialize(*global_sort, global_sort->buffer_manager);
			unsorted_count = 0;
		}

		for (; sorted < count; ++sorted) {
			initialize(agg_state.data());

			// Finalize a single value at the next offset
			agg_state_vec.SetVectorType(states.GetVectorType());
			finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);

			if (destructor) {
				destructor(agg_state_vec, aggr_bind_info, 1);
			}
		}

		result.Verify(count);
	}
};

void FunctionBinder::BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
                                         const vector<unique_ptr<Expression>> &groups) {
	if (!expr.order_bys || expr.order_bys->orders.empty() || expr.children.empty()) {
		// not a sorted aggregate: return
		return;
	}
	if (context.config.enable_optimizer) {
		// for each ORDER BY - check if it is actually necessary
		// expressions that are in the groups do not need to be ORDERED BY
		// `ORDER BY` on a group has no effect, because for each aggregate, the group is unique
		// similarly, we only need to ORDER BY each aggregate once
		expression_set_t seen_expressions;
		for (auto &target : groups) {
			seen_expressions.insert(*target);
		}
		vector<BoundOrderByNode> new_order_nodes;
		for (auto &order_node : expr.order_bys->orders) {
			if (seen_expressions.find(*order_node.expression) != seen_expressions.end()) {
				// we do not need to order by this node
				continue;
			}
			seen_expressions.insert(*order_node.expression);
			new_order_nodes.push_back(std::move(order_node));
		}
		if (new_order_nodes.empty()) {
			expr.order_bys.reset();
			return;
		}
		expr.order_bys->orders = std::move(new_order_nodes);
	}
	auto &bound_function = expr.function;
	auto &children = expr.children;
	auto &order_bys = *expr.order_bys;
	auto sorted_bind = make_uniq<SortedAggregateBindData>(context, expr);

	if (!sorted_bind->sorted_on_args) {
		// The arguments are the children plus the sort columns.
		for (auto &order : order_bys.orders) {
			children.emplace_back(std::move(order.expression));
		}
	}

	vector<LogicalType> arguments;
	arguments.reserve(children.size());
	for (const auto &child : children) {
		arguments.emplace_back(child->return_type);
	}

	// Replace the aggregate with the wrapper
	AggregateFunction ordered_aggregate(
	    bound_function.name, arguments, bound_function.return_type, AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::ScatterUpdate,
	    AggregateFunction::StateCombine<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::Finalize, bound_function.null_handling, SortedAggregateFunction::SimpleUpdate, nullptr,
	    AggregateFunction::StateDestroy<SortedAggregateState, SortedAggregateFunction>, nullptr,
	    SortedAggregateFunction::Window);

	expr.function = std::move(ordered_aggregate);
	expr.bind_info = std::move(sorted_bind);
	expr.order_bys.reset();
}

} // namespace duckdb
