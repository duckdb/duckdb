#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate/list_aggregate.hpp"
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

	SortedAggregateBindData(ClientContext &context, Expressions &children, BoundAggregateFunction &aggregate,
	                        BindInfoPtr &bind_info, OrderBys &order_bys)
	    : context(context), function(aggregate), bind_info(std::move(bind_info)),
	      threshold(Settings::Get<OrderedAggregateThresholdSetting>(context)) {
		//	Describe the arguments.
		for (const auto &child : children) {
			buffered_cols.emplace_back(buffered_cols.size());
			buffered_types.emplace_back(child->GetReturnType());

			//	Column 0 in the sort data is the group number
			scan_cols.emplace_back(buffered_cols.size());
		}
		scan_types = buffered_types;

		//	The first sort column is the group number. It is prefixed onto the buffered data
		sort_types.emplace_back(LogicalType::USMALLINT);
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
		                    make_uniq<BoundReferenceExpression>(sort_types.back(), 0U));

		// Determine whether we are sorted on all the arguments.
		// Even if we are not, we want to share inputs for sorting.
		for (idx_t ord_idx = 0; ord_idx < order_bys.size(); ++ord_idx) {
			auto order = order_bys[ord_idx].Copy();
			bool matched = false;
			const auto &type = order.expression->GetReturnType();

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

		// The buffered rows are stored in a linked list of structs
		child_list_t<LogicalType> buffered_children;
		for (idx_t i = 0; i < buffered_types.size(); i++) {
			buffered_children.emplace_back("v" + to_string(i), buffered_types[i]);
			sort_types.emplace_back(buffered_types[i]);
		}
		buffered_struct_type = LogicalType::STRUCT(std::move(buffered_children));
		GetSegmentDataFunctions(buffered_funcs, buffered_struct_type);

		//	Only scan the argument columns after sorting
		sort = make_uniq<Sort>(context, orders, sort_types, scan_cols);
	}

	SortedAggregateBindData(ClientContext &context, BoundAggregateExpression &expr)
	    : SortedAggregateBindData(context, expr.GetChildrenMutable(), expr.FunctionMutable(), expr.BindInfoMutable(),
	                              expr.GetOrderBysMutable()->orders) {
	}

	SortedAggregateBindData(ClientContext &context, BoundWindowExpression &expr)
	    : SortedAggregateBindData(context, expr.GetChildrenMutable(), *expr.AggregateFunction(), expr.BindInfoMutable(),
	                              expr.ArgOrdersMutable()) {
	}

	SortedAggregateBindData(const SortedAggregateBindData &other)
	    : context(other.context), function(other.function), sort_types(other.sort_types), scan_cols(other.scan_cols),
	      scan_types(other.scan_types), buffered_cols(other.buffered_cols), buffered_types(other.buffered_types),
	      buffered_struct_type(other.buffered_struct_type), buffered_funcs(other.buffered_funcs),
	      sorted_on_args(other.sorted_on_args), threshold(other.threshold) {
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
	BoundAggregateFunction function;
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
	//! The struct type holding one buffered row
	LogicalType buffered_struct_type;
	//! The linked list functions for the buffered rows
	ListSegmentFunctions buffered_funcs;
	//! Can we just use the inputs for sorting?
	bool sorted_on_args = true;

	//! The sort flush threshold
	const idx_t threshold;
};

//! The sorted aggregate buffers its input rows in a linked list of structs, sharing the "list" callbacks
struct SortedAggregateState : ListAggState {};

struct SortedAggregateFunction {
	static LogicalType GetElementType(AggregateInputData &aggr_input_data) {
		return aggr_input_data.bind_data->Cast<SortedAggregateBindData>().buffered_struct_type;
	}

	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		if (!count) {
			return;
		}
		//	Pack the buffered columns into a single struct vector for the list update
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		Vector packed(order_bind.buffered_struct_type, count);
		auto &entries = StructVector::GetEntries(packed);
		const auto &buffered_cols = order_bind.buffered_cols;
		for (idx_t b = 0; b < buffered_cols.size(); ++b) {
			D_ASSERT(buffered_cols[b] < input_count);
			entries[b].Reference(inputs[buffered_cols[b]]);
		}
		FlatVector::SetSize(packed, count_t(count));
		ListUpdateFunction(&packed, aggr_input_data, 1, states, count);
	}

	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes, Vector &result,
	                   idx_t rid) {
		throw InternalException("Sorted aggregates should not be generated for window clauses");
	}

	static void WindowBatch(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                        const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames *subframes_per_row,
	                        idx_t count, Vector &result, idx_t row_idx) {
		for (idx_t rid = 0; rid < count; ++rid) {
			Window(aggr_input_data, partition, g_state, l_state, subframes_per_row[rid], result, rid);
		}
	}

	//! Sinks all buffered rows of the state into the sort, prefixed with the group number
	static void SinkState(const SortedAggregateBindData &order_bind, SortedAggregateState &state, idx_t group_number,
	                      ExecutionContext &context, OperatorSinkInput &sink, DataChunk &prefixed) {
		auto &sort = *order_bind.sort;
		ListSegmentScanState scan_state;
		order_bind.buffered_funcs.InitializeScan(state.linked_list, scan_state);
		for (;;) {
			Vector rows(order_bind.buffered_struct_type, STANDARD_VECTOR_SIZE);
			const auto chunk_count = order_bind.buffered_funcs.Scan(scan_state, rows);
			if (!chunk_count) {
				break;
			}
			auto &entries = StructVector::GetEntries(rows);
			prefixed.Reset();
			prefixed.data[0].Reference(Value::USMALLINT(UnsafeNumericCast<uint16_t>(group_number)), count_t(1));
			FlatVector::SetSize(prefixed.data[0], count_t(chunk_count));
			for (column_t col_idx = 0; col_idx < entries.size(); ++col_idx) {
				prefixed.data[col_idx + 1].Reference(entries[col_idx]);
				FlatVector::SetSize(prefixed.data[col_idx + 1], count_t(chunk_count));
			}
			sort.Sink(context, prefixed, sink);
		}
		//	Release the state - the rows are freed with the arena allocator
		state.linked_list = LinkedList();
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
		vector<data_t> agg_state(aggr.GetCallbacks().GetStateSizeCallback()(aggr));
		Vector agg_state_vec(Value::POINTER(CastPointerToValue(agg_state.data())), count_t(1));

		// State variables
		auto bind_info = order_bind.bind_info.get();
		AggregateInputData aggr_bind_info(aggr, bind_info, aggr_input_data.allocator);

		// Inner aggregate APIs
		auto initialize = aggr.GetCallbacks().GetStateInitCallback();
		auto destructor = aggr.GetCallbacks().GetStateDestructorCallback();
		auto cluster_update = aggr.GetCallbacks().GetStateClusterUpdateCallback();
		auto update = aggr.GetCallbacks().GetStateUpdateCallback();
		auto finalize = aggr.GetCallbacks().GetStateFinalizeCallback();

		auto sdata = states.Values<SortedAggregateState *>();

		vector<idx_t> state_unprocessed(count, 0);
		for (idx_t i = 0; i < count; ++i) {
			state_unprocessed[i] = sdata[i].GetValueUnsafe()->linked_list.total_capacity;
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
				auto state = sdata[finalized].GetValueUnsafe();
				OperatorSinkInput sink {*global_sink, *local_sink, interrupt};
				SinkState(order_bind, *state, finalized, context, sink, prefixed);
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

					if (cluster_update) {
						ClusteredAggr clustered;
						clustered.SetSingleRun(agg_state.data(), sliced.size());
						aggr_bind_info.clustered = &clustered;
						cluster_update(sliced.data.data(), aggr_bind_info, sliced.data.size(), clustered,
						               sliced.size());
						aggr_bind_info.clustered = nullptr;
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

		result.Verify();
	}
};

} // namespace

void FunctionBinder::BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
                                         const vector<unique_ptr<Expression>> &groups,
                                         optional_ptr<vector<GroupingSet>> grouping_sets) {
	if (!expr.GetOrderBys() || expr.GetOrderBys()->orders.empty() || expr.GetChildren().empty()) {
		// not a sorted aggregate: return
		return;
	}
	// Remove unnecessary ORDER BY clauses and return if nothing remains
	if (Settings::Get<EnableOptimizerSetting>(context)) {
		if (expr.GetOrderBysMutable()->Simplify(groups, grouping_sets)) {
			expr.GetOrderBysMutable().reset();
			return;
		}
	}
	auto &bound_function = expr.Function();
	auto &children = expr.GetChildrenMutable();
	auto &order_bys = *expr.GetOrderBysMutable();
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
		arguments.emplace_back(child->GetReturnType());
	}

	// Replace the aggregate with the wrapper
	AggregateFunction ordered_aggregate(bound_function.GetName(), arguments, bound_function.GetReturnType(),
	                                    AggregateFunction::StateSize<SortedAggregateState>,
	                                    AggregateFunction::StateInitialize<SortedAggregateState, ListFunction>,
	                                    SortedAggregateFunction::ScatterUpdate,
	                                    ListCombineFunction<SortedAggregateFunction>, SortedAggregateFunction::Finalize,
	                                    bound_function.GetProperties().GetNullHandling(), nullptr, nullptr, nullptr,
	                                    nullptr, SortedAggregateFunction::WindowBatch);

	expr.FunctionMutable().ReplaceImplementation(ordered_aggregate);
	expr.BindInfoMutable() = std::move(sorted_bind);
	expr.GetOrderBysMutable().reset();
}

void FunctionBinder::BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr) {
	//	Make implicit orderings explicit
	auto &aggregate = *expr.AggregateFunction();
	if (aggregate.GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT && expr.ArgOrders().empty()) {
		for (auto &order : expr.OrderBy()) {
			const auto type = order.type;
			const auto null_order = order.null_order;
			auto expression = order.expression->Copy();
			expr.ArgOrdersMutable().emplace_back(type, null_order, std::move(expression));
		}
	}

	if (expr.ArgOrders().empty() || expr.GetChildren().empty()) {
		// not a sorted aggregate: return
		return;
	}
	// Remove unnecessary ORDER BY clauses and return if nothing remains
	if (Settings::Get<EnableOptimizerSetting>(context)) {
		if (BoundOrderModifier::Simplify(expr.ArgOrdersMutable(), expr.PartitionsMutable(), nullptr)) {
			expr.ArgOrdersMutable().clear();
			return;
		}
	}
	auto &children = expr.GetChildrenMutable();
	auto &arg_orders = expr.ArgOrdersMutable();
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
		arguments.emplace_back(child->GetReturnType());
	}

	// Replace the aggregate with the wrapper
	AggregateFunction ordered_aggregate(
	    aggregate.GetName(), arguments, aggregate.GetReturnType(), AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, ListFunction>, SortedAggregateFunction::ScatterUpdate,
	    ListCombineFunction<SortedAggregateFunction>, SortedAggregateFunction::Finalize,
	    aggregate.GetProperties().GetNullHandling(), nullptr, nullptr, nullptr, nullptr,
	    SortedAggregateFunction::WindowBatch);
	ordered_aggregate.SetWindowCallback(SortedAggregateFunction::Window);

	aggregate.ReplaceImplementation(ordered_aggregate);
	expr.BindInfoMutable() = std::move(sorted_bind);
	expr.ArgOrdersMutable().clear();
}

} // namespace duckdb
