#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

namespace {
struct SortedAggregateBindData : public FunctionData {
	using Expressions = vector<unique_ptr<Expression>>;
	using BindInfoPtr = unique_ptr<FunctionData>;
	using OrderBys = vector<BoundOrderByNode>;

	SortedAggregateBindData(ClientContext &context, Expressions &children, AggregateFunction &aggregate,
	                        BindInfoPtr &bind_info, OrderBys &order_bys)
	    : context(context), function(aggregate), bind_info(std::move(bind_info)),
	      threshold(DBConfig::GetSetting<OrderedAggregateThresholdSetting>(context)) {
		//	Describe the arguments.
		for (const auto &child : children) {
			buffered_cols.emplace_back(buffered_cols.size());
			buffered_types.emplace_back(child->return_type);

			//	Column 0 in the sort data is the group number
			scan_cols.emplace_back(buffered_cols.size());
		}
		scan_types = buffered_types;

		//	The first sort column is the group number. It is prefixed onto the buffered data
		sort_types.emplace_back(LogicalType::USMALLINT);
		orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
		                                     make_uniq<BoundReferenceExpression>(sort_types.back(), 0U)));

		// Determine whether we are sorted on all the arguments.
		// Even if we are not, we want to share inputs for sorting.
		for (idx_t ord_idx = 0; ord_idx < order_bys.size(); ++ord_idx) {
			auto order = order_bys[ord_idx].Copy();
			bool matched = false;
			const auto &type = order.expression->return_type;

			for (idx_t arg_idx = 0; arg_idx < children.size(); ++arg_idx) {
				auto &child = children[arg_idx];
				if (child->Equals(*order.expression)) {
					order.expression = make_uniq<BoundReferenceExpression>(type, arg_idx + 1);
					matched = true;
					break;
				}
			}

			if (!matched) {
				sorted_on_args = false;
				buffered_cols.emplace_back(children.size() + ord_idx);
				buffered_types.emplace_back(type);
				order.expression = make_uniq<BoundReferenceExpression>(type, buffered_cols.size());
			}

			orders.emplace_back(std::move(order));
		}

		// Look up all the linked list functions we need
		for (auto &type : buffered_types) {
			ListSegmentFunctions funcs;
			GetSegmentDataFunctions(funcs, type);
			buffered_funcs.emplace_back(std::move(funcs));
			sort_types.emplace_back(type);
		}

		//	Only scan the argument columns after sorting
		sort = make_uniq<Sort>(context, orders, sort_types, scan_cols);
	}

	SortedAggregateBindData(ClientContext &context, BoundAggregateExpression &expr)
	    : SortedAggregateBindData(context, expr.children, expr.function, expr.bind_info, expr.order_bys->orders) {
	}

	SortedAggregateBindData(ClientContext &context, BoundWindowExpression &expr)
	    : SortedAggregateBindData(context, expr.children, *expr.aggregate, expr.bind_info, expr.arg_orders) {
	}

	SortedAggregateBindData(const SortedAggregateBindData &other)
	    : context(other.context), function(other.function), sort_types(other.sort_types), scan_cols(other.scan_cols),
	      scan_types(other.scan_types), buffered_cols(other.buffered_cols), buffered_types(other.buffered_types),
	      buffered_funcs(other.buffered_funcs), sorted_on_args(other.sorted_on_args), threshold(other.threshold) {
		if (other.bind_info) {
			bind_info = other.bind_info->Copy();
		}
		for (auto &order : other.orders) {
			orders.emplace_back(order.Copy());
		}

		sort = make_uniq<Sort>(context, orders, sort_types, scan_cols);
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

	ClientContext &context;
	AggregateFunction function;
	unique_ptr<FunctionData> bind_info;

	//! The sort expressions (all references as the expressions have been computed)
	vector<BoundOrderByNode> orders;
	//! The types of the sunk columns
	vector<LogicalType> sort_types;
	//! The sorted columns that have the arguments
	vector<column_t> scan_cols;
	//! The types of the sunk columns
	vector<LogicalType> scan_types;
	//! The shared sort specification
	unique_ptr<Sort> sort;

	//! The mapping from inputs to buffered columns
	vector<column_t> buffered_cols;
	//! The schema of the buffered data
	vector<LogicalType> buffered_types;
	//! The linked list functions for the buffered data
	vector<ListSegmentFunctions> buffered_funcs;
	//! Can we just use the inputs for sorting?
	bool sorted_on_args = true;

	//! The sort flush threshold
	const idx_t threshold;
};

struct SortedAggregateState {
	// Linked list equivalent of DataChunk
	using LinkedLists = vector<LinkedList>;
	using LinkedChunkFunctions = vector<ListSegmentFunctions>;

	//! Capacities of the various levels of buffering
	static const idx_t CHUNK_CAPACITY = STANDARD_VECTOR_SIZE;
	static const idx_t LIST_CAPACITY = MinValue<idx_t>(16, CHUNK_CAPACITY);

	SortedAggregateState() : count(0), nsel(0), offset(0) {
	}

	static inline void InitializeLinkedList(LinkedLists &linked, const vector<LogicalType> &types) {
		if (linked.empty() && !types.empty()) {
			linked.resize(types.size(), LinkedList());
		}
	}

	inline void InitializeLinkedLists(const SortedAggregateBindData &order_bind) {
		InitializeLinkedList(input_linked, order_bind.buffered_types);
	}

	static inline void InitializeChunk(Allocator &allocator, unique_ptr<DataChunk> &chunk,
	                                   const vector<LogicalType> &types) {
		if (!chunk && !types.empty()) {
			chunk = make_uniq<DataChunk>();
			chunk->Initialize(allocator, types);
		}
	}

	void InitializeChunks(const SortedAggregateBindData &order_bind) {
		// Lazy instantiation of the buffer chunks
		auto &allocator = BufferManager::GetBufferManager(order_bind.context).GetBufferAllocator();
		InitializeChunk(allocator, input_chunk, order_bind.buffered_types);
	}

	static inline void FlushLinkedList(const LinkedChunkFunctions &funcs, LinkedLists &linked, DataChunk &chunk) {
		idx_t total_count = 0;
		for (column_t i = 0; i < linked.size(); ++i) {
			funcs[i].BuildListVector(linked[i], chunk.data[i], total_count);
			chunk.SetCardinality(linked[i].total_capacity);
		}
	}

	void FlushLinkedLists(const SortedAggregateBindData &order_bind) {
		InitializeChunks(order_bind);
		FlushLinkedList(order_bind.buffered_funcs, input_linked, *input_chunk);
	}

	void InitializeCollections(const SortedAggregateBindData &order_bind) {
		input_collection = make_uniq<ColumnDataCollection>(order_bind.context, order_bind.buffered_types);
		input_append = make_uniq<ColumnDataAppendState>();
		input_collection->InitializeAppend(*input_append);
	}

	void FlushChunks(const SortedAggregateBindData &order_bind) {
		D_ASSERT(input_chunk);
		input_collection->Append(*input_append, *input_chunk);
		input_chunk->Reset();
	}

	void Resize(const SortedAggregateBindData &order_bind, idx_t n) {
		count = n;

		//	Establish the current buffering
		if (count <= LIST_CAPACITY) {
			InitializeLinkedLists(order_bind);
		}

		if (count > LIST_CAPACITY && !input_chunk && !input_collection) {
			FlushLinkedLists(order_bind);
		}

		if (count > CHUNK_CAPACITY && !input_collection) {
			InitializeCollections(order_bind);
			FlushChunks(order_bind);
		}
	}

	static void LinkedAppend(const LinkedChunkFunctions &functions, ArenaAllocator &allocator, DataChunk &input,
	                         LinkedLists &linked, SelectionVector &sel, idx_t nsel) {
		const auto count = input.size();
		for (column_t c = 0; c < input.ColumnCount(); ++c) {
			auto &func = functions[c];
			auto &linked_list = linked[c];
			RecursiveUnifiedVectorFormat input_data;
			Vector::RecursiveToUnifiedFormat(input.data[c], count, input_data);
			for (idx_t i = 0; i < nsel; ++i) {
				idx_t sidx = sel.get_index(i);
				func.AppendRow(allocator, linked_list, input_data, sidx);
			}
		}
	}

	static void LinkedAbsorb(LinkedLists &source, LinkedLists &target) {
		D_ASSERT(source.size() == target.size());
		for (column_t i = 0; i < source.size(); ++i) {
			auto &src = source[i];
			if (!src.total_capacity) {
				break;
			}

			auto &tgt = target[i];
			if (!tgt.total_capacity) {
				tgt = src;
			} else {
				// append the linked list
				tgt.last_segment->next = src.first_segment;
				tgt.last_segment = src.last_segment;
				tgt.total_capacity += src.total_capacity;
			}
		}
	}

	void Update(const AggregateInputData &aggr_input_data, DataChunk &input) {
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		Resize(order_bind, count + input.size());

		sel.Initialize(nullptr);
		nsel = input.size();

		if (input_collection) {
			//	Using collections
			input_collection->Append(*input_append, input);
		} else if (input_chunk) {
			//	Still using data chunks
			input_chunk->Append(input);
		} else {
			//	Still using linked lists
			LinkedAppend(order_bind.buffered_funcs, aggr_input_data.allocator, input, input_linked, sel, nsel);
		}

		nsel = 0;
		offset = 0;
	}

	void UpdateSlice(const AggregateInputData &aggr_input_data, DataChunk &input) {
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		Resize(order_bind, count + nsel);

		if (input_collection) {
			//	Using collections
			D_ASSERT(input_chunk);
			input_chunk->Slice(input, sel, nsel);
			FlushChunks(order_bind);
		} else if (input_chunk) {
			//	Still using data chunks
			input_chunk->Append(input, true, &sel, nsel);
		} else {
			//	Still using linked lists
			LinkedAppend(order_bind.buffered_funcs, aggr_input_data.allocator, input, input_linked, sel, nsel);
		}

		nsel = 0;
		offset = 0;
	}

	void Swap(SortedAggregateState &other) {
		std::swap(count, other.count);

		std::swap(input_collection, other.input_collection);
		std::swap(input_append, other.input_append);

		std::swap(input_chunk, other.input_chunk);

		std::swap(input_linked, other.input_linked);
	}

	void Absorb(const SortedAggregateBindData &order_bind, SortedAggregateState &other) {
		if (!other.count) {
			return;
		} else if (!count) {
			Swap(other);
			return;
		}

		//	Change to a state large enough for all the data
		Resize(order_bind, count + other.count);

		//	3x3 matrix.
		//	We can simplify the logic a bit because the target is already set for the final capacity
		if (!input_chunk) {
			//	If the combined count is still linked lists,
			//	then just move the pointers.
			//	Note that this assumes ArenaAllocator is shared and the memory will not vanish under us.
			LinkedAbsorb(other.input_linked, input_linked);

			other.Reset();
			return;
		}

		if (!other.input_chunk) {
			other.FlushLinkedLists(order_bind);
		}

		if (!input_collection) {
			//	Still using chunks, which means the source is using chunks or lists
			D_ASSERT(input_chunk);
			D_ASSERT(other.input_chunk);
			input_chunk->Append(*other.input_chunk);
		} else {
			// Using collections, so source could be using anything.
			if (other.input_collection) {
				input_collection->Combine(*other.input_collection);
			} else {
				input_collection->Append(*other.input_chunk);
			}
		}

		//	Free all memory as we have absorbed it.
		other.Reset();
	}

	void PrefixSortBuffer(DataChunk &prefixed) {
		for (column_t col_idx = 0; col_idx < input_chunk->ColumnCount(); ++col_idx) {
			prefixed.data[col_idx + 1].Reference(input_chunk->data[col_idx]);
		}
		prefixed.SetCardinality(*input_chunk);
	}

	void Finalize(const SortedAggregateBindData &order_bind, DataChunk &prefixed, ExecutionContext &context,
	              OperatorSinkInput &sink) {
		auto &sort = *order_bind.sort;
		if (input_collection) {
			ColumnDataScanState sort_state;
			input_collection->InitializeScan(sort_state);
			for (input_chunk->Reset(); input_collection->Scan(sort_state, *input_chunk); input_chunk->Reset()) {
				PrefixSortBuffer(prefixed);
				sort.Sink(context, prefixed, sink);
			}
		} else {
			//	Force chunks so we can sort
			if (!input_chunk) {
				FlushLinkedLists(order_bind);
			}

			PrefixSortBuffer(prefixed);
			sort.Sink(context, prefixed, sink);
		}

		Reset();
	}

	void Reset() {
		//	Release all memory
		input_collection.reset();
		input_chunk.reset();
		input_linked.clear();

		count = 0;
	}

	idx_t count;

	unique_ptr<ColumnDataCollection> input_collection;
	unique_ptr<ColumnDataAppendState> input_append;
	unique_ptr<DataChunk> input_chunk;
	LinkedLists input_linked;

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
	                          idx_t count, DataChunk &buffered) {
		//	Only reference the buffered columns
		buffered.InitializeEmpty(order_bind.buffered_types);
		const auto &buffered_cols = order_bind.buffered_cols;
		for (idx_t b = 0; b < buffered_cols.size(); ++b) {
			D_ASSERT(buffered_cols[b] < input_count);
			buffered.data[b].Reference(inputs[buffered_cols[b]]);
		}
		buffered.SetCardinality(count);
	}

	static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		const auto order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_input;
		ProjectInputs(inputs, order_bind, input_count, count, arg_input);

		const auto order_state = reinterpret_cast<SortedAggregateState *>(state);
		order_state->Update(aggr_input_data, arg_input);
	}

	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		if (!count) {
			return;
		}

		// Append the arguments to the two sub-collections
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_inputs;
		ProjectInputs(inputs, order_bind, input_count, count, arg_inputs);

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
			sel_data[order_state->offset++] = UnsafeNumericCast<sel_t>(sidx);
		}

		// Append nonempty slices to the arguments
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->nsel) {
				continue;
			}

			order_state->UpdateSlice(aggr_input_data, arg_inputs);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &other = const_cast<STATE &>(source); // NOLINT: absorb explicitly allows destruction
		target.Absorb(order_bind, other);
	}

	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes, Vector &result,
	                   idx_t rid) {
		throw InternalException("Sorted aggregates should not be generated for window clauses");
	}

	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     const idx_t offset) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &client = order_bind.context;

		auto &buffer_allocator = BufferManager::GetBufferManager(client).GetBufferAllocator();
		DataChunk scanned;
		scanned.Initialize(buffer_allocator, order_bind.scan_types);
		DataChunk sliced;
		sliced.Initialize(buffer_allocator, order_bind.scan_types);

		//	 Reusable inner state
		auto &aggr = order_bind.function;
		vector<data_t> agg_state(aggr.state_size(aggr));
		Vector agg_state_vec(Value::POINTER(CastPointerToValue(agg_state.data())));

		// State variables
		auto bind_info = order_bind.bind_info.get();
		AggregateInputData aggr_bind_info(bind_info, aggr_input_data.allocator);

		// Inner aggregate APIs
		auto initialize = aggr.initialize;
		auto destructor = aggr.destructor;
		auto simple_update = aggr.simple_update;
		auto update = aggr.update;
		auto finalize = aggr.finalize;

		auto sdata = FlatVector::GetData<SortedAggregateState *>(states);

		vector<idx_t> state_unprocessed(count, 0);
		for (idx_t i = 0; i < count; ++i) {
			state_unprocessed[i] = sdata[i]->count;
		}

		ThreadContext thread(client);
		ExecutionContext context(client, thread, nullptr);
		InterruptState interrupt;
		auto &sort = order_bind.sort;
		auto global_sink = sort->GetGlobalSinkState(client);
		auto local_sink = sort->GetLocalSinkState(context);

		DataChunk prefixed;
		prefixed.Initialize(buffer_allocator, order_bind.sort_types);

		//	Go through the states accumulating values to sort until we hit the sort threshold
		idx_t unsorted_count = 0;
		idx_t sorted = 0;
		for (idx_t finalized = 0; finalized < count;) {
			if (unsorted_count < order_bind.threshold) {
				auto state = sdata[finalized];
				prefixed.Reset();
				prefixed.data[0].Reference(Value::USMALLINT(UnsafeNumericCast<uint16_t>(finalized)));
				OperatorSinkInput sink {*global_sink, *local_sink, interrupt};
				state->Finalize(order_bind, prefixed, context, sink);
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
			OperatorSinkCombineInput combine {*global_sink, *local_sink, interrupt};
			order_bind.sort->Combine(context, combine);

			OperatorSinkFinalizeInput finalize_input {*global_sink, interrupt};
			order_bind.sort->Finalize(client, finalize_input);

			auto global_source = sort->GetGlobalSourceState(client, *global_sink);
			auto local_source = sort->GetLocalSourceState(context, *global_source);

			initialize(aggr, agg_state.data());
			for (;;) {
				OperatorSourceInput source {*global_source, *local_source, interrupt};
				scanned.Reset();
				if (sort->GetData(context, scanned, source) == SourceResultType::FINISHED) {
					break;
				}
				idx_t consumed = 0;

				// Distribute the scanned chunk to the aggregates
				while (consumed < scanned.size()) {
					//	Find the next aggregate that needs data
					for (; !state_unprocessed[sorted]; ++sorted) {
						// Finalize a single value at the next offset
						agg_state_vec.SetVectorType(states.GetVectorType());
						finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);
						if (destructor) {
							destructor(agg_state_vec, aggr_bind_info, 1);
						}

						initialize(aggr, agg_state.data());
					}
					const auto input_count = MinValue(state_unprocessed[sorted], scanned.size() - consumed);
					for (column_t col_idx = 0; col_idx < scanned.ColumnCount(); ++col_idx) {
						sliced.data[col_idx].Slice(scanned.data[col_idx], consumed, consumed + input_count);
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
			global_sink = sort->GetGlobalSinkState(client);
			local_sink = sort->GetLocalSinkState(context);
			unsorted_count = 0;
		}

		for (; sorted < count; ++sorted) {
			initialize(aggr, agg_state.data());

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

} // namespace

void FunctionBinder::BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
                                         const vector<unique_ptr<Expression>> &groups) {
	if (!expr.order_bys || expr.order_bys->orders.empty() || expr.children.empty()) {
		// not a sorted aggregate: return
		return;
	}
	// Remove unnecessary ORDER BY clauses and return if nothing remains
	if (context.config.enable_optimizer) {
		if (expr.order_bys->Simplify(groups)) {
			expr.order_bys.reset();
			return;
		}
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
	    bound_function.name, arguments, bound_function.GetReturnType(),
	    AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, SortedAggregateFunction,
	                                       AggregateDestructorType::LEGACY>,
	    SortedAggregateFunction::ScatterUpdate,
	    AggregateFunction::StateCombine<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::Finalize, bound_function.GetNullHandling(), SortedAggregateFunction::SimpleUpdate,
	    nullptr, AggregateFunction::StateDestroy<SortedAggregateState, SortedAggregateFunction>, nullptr,
	    SortedAggregateFunction::Window);

	expr.function = std::move(ordered_aggregate);
	expr.bind_info = std::move(sorted_bind);
	expr.order_bys.reset();
}

void FunctionBinder::BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr) {
	//	Make implicit orderings explicit
	auto &aggregate = *expr.aggregate;
	if (aggregate.order_dependent == AggregateOrderDependent::ORDER_DEPENDENT && expr.arg_orders.empty()) {
		for (auto &order : expr.orders) {
			const auto type = order.type;
			const auto null_order = order.null_order;
			auto expression = order.expression->Copy();
			expr.arg_orders.emplace_back(BoundOrderByNode(type, null_order, std::move(expression)));
		}
	}

	if (expr.arg_orders.empty() || expr.children.empty()) {
		// not a sorted aggregate: return
		return;
	}
	// Remove unnecessary ORDER BY clauses and return if nothing remains
	if (context.config.enable_optimizer) {
		if (BoundOrderModifier::Simplify(expr.arg_orders, expr.partitions)) {
			expr.arg_orders.clear();
			return;
		}
	}
	auto &children = expr.children;
	auto &arg_orders = expr.arg_orders;
	auto sorted_bind = make_uniq<SortedAggregateBindData>(context, expr);

	if (!sorted_bind->sorted_on_args) {
		// The arguments are the children plus the sort columns.
		for (auto &order : arg_orders) {
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
	    aggregate.name, arguments, aggregate.GetReturnType(), AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, SortedAggregateFunction,
	                                       AggregateDestructorType::LEGACY>,
	    SortedAggregateFunction::ScatterUpdate,
	    AggregateFunction::StateCombine<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::Finalize, aggregate.GetNullHandling(), SortedAggregateFunction::SimpleUpdate, nullptr,
	    AggregateFunction::StateDestroy<SortedAggregateState, SortedAggregateFunction>, nullptr,
	    SortedAggregateFunction::Window);

	aggregate = std::move(ordered_aggregate);
	expr.bind_info = std::move(sorted_bind);
	expr.arg_orders.clear();
}

} // namespace duckdb
