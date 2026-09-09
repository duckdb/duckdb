#include "duckdb/function/window/window_match_recognize.hpp"

#include "duckdb/function/match_recognize.hpp"
#include "duckdb/function/window/match_recognize_functions.hpp"
#include "duckdb/function/window/match_recognize_matcher.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//! The result's list child, in the order ResultType() declares its fields
using SpanStruct = VectorStructType<string_t, uint64_t, bool, bool, uint64_t, uint64_t, bool, bool, uint64_t>;

//! One membership of a row in a match: there is one per (row, match) pair, not one per row
struct MatchRecognizeSpan {
	idx_t symbol;
	idx_t match_number;
	idx_t match_start;
	idx_t match_end;
	bool is_match_start;
	bool excluded;
	//! An empty match covers no rows at all; this span only marks where it happened
	bool empty;
};

//! The memberships of one row, in the order the matches were found
struct MatchRecognizeRowSpans {
	struct Node {
		MatchRecognizeSpan span;
		Node *next;
	};

	Node *first = nullptr;
	Node *last = nullptr;
	idx_t count = 0;
};

//! Appends memberships on behalf of one thread. The arena is that thread's own but owned by the global
//! state, so the memberships outlive the walk however it ends, and it allocates through the buffer
//! manager so that they count against the memory limit.
struct MatchRecognizeSpanWriter {
	explicit MatchRecognizeSpanWriter(ArenaAllocator &arena_p) : arena(arena_p) {
	}

	void Append(MatchRecognizeRowSpans &row, const MatchRecognizeSpan &span) {
		auto node = reinterpret_cast<MatchRecognizeRowSpans::Node *>(
		    arena.AllocateAligned(sizeof(MatchRecognizeRowSpans::Node)));
		node->span = span;
		node->next = nullptr;
		if (row.count++ == 0) {
			row.first = node;
		} else {
			row.last->next = node;
		}
		row.last = node;
	}

	ArenaAllocator &arena;
};

struct WindowMatchRecognizeGlobalState : WindowExecutorGlobalState {
	WindowMatchRecognizeGlobalState(ClientContext &client, const WindowExecutor &executor, const idx_t payload_count,
	                                const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(client, executor, payload_count, partition_mask, order_mask),
	      row_spans(payload_count) {
		auto &config = executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();
		condition_values.resize(config.conditions.size());
		for (auto &values : condition_values) {
			values.assign(payload_count, 0);
		}
		classifiers.resize(payload_count);
		excluded_rows.resize(payload_count);
		// the name a symbol reports is the same for every row it classifies, so it is spelled once
		for (auto &symbol : config.symbols) {
			classifier_names.push_back(MatchRecognizeSymbolName(symbol));
		}
		D_ASSERT(executor.wexpr.GetReturnType().id() == LogicalTypeId::LIST);
	}

	//! An arena for a thread to fill, owned here so that what it holds survives a cut-short walk
	ArenaAllocator &SpanArena(ClientContext &client) {
		lock_guard<mutex> guard(state_lock);
		span_arenas.push_back(make_uniq<ArenaAllocator>(BufferManager::GetBufferManager(client).GetBufferAllocator()));
		return *span_arenas.back();
	}

	mutex state_lock;
	//! Set up once; the threads then take partitions from the cursor below
	bool prepared = false;
	//! Partitions are independent, so the threads that reach Finalize share them out
	vector<pair<idx_t, idx_t>> partitions;
	atomic<idx_t> next_partition {0};
	//! The variable that classified each row, written only by the thread that owns the partition
	vector<idx_t> classifiers;
	//! Whether the pattern matched each row inside a {- -}, written alongside the classifier above
	vector<uint8_t> excluded_rows;
	//! One boolean per symbol per row, filled by Sink over disjoint ranges
	vector<vector<uint8_t>> condition_values;

	//! What each symbol reports as its classifier
	vector<string> classifier_names;
	//! Where each row's memberships start, and how many there are
	vector<MatchRecognizeRowSpans> row_spans;
	//! The arenas the memberships above live in, one per thread that wrote any
	vector<unique_ptr<ArenaAllocator>> span_arenas;
};

LogicalType WindowMatchRecognizeExecutor::ResultType() {
	// One entry per match a row takes part in; the plan unnests the list, which drops the rows that
	// matched nothing
	return LogicalType::LIST(LogicalType::STRUCT({{"classifier", LogicalType::VARCHAR},
	                                              {"match_number", LogicalType::UBIGINT},
	                                              {"is_match_start", LogicalType::BOOLEAN},
	                                              {"is_match_end", LogicalType::BOOLEAN},
	                                              {"match_start", LogicalType::UBIGINT},
	                                              {"match_end", LogicalType::UBIGINT},
	                                              {"is_excluded", LogicalType::BOOLEAN},
	                                              {"is_empty", LogicalType::BOOLEAN},
	                                              // the row's place in the partition, which gives a measure's
	                                              // window a total order even when the ORDER BY has ties
	                                              {"row_index", LogicalType::UBIGINT}}));
}

//===--------------------------------------------------------------------===//
// Binding
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> WindowMatchRecognizeExecutor::Bind(BindWindowFunctionInput &input) {
	// the MATCH_RECOGNIZE binder hands its configuration over as bind data, so nothing reaching here
	// came from a MATCH_RECOGNIZE clause
	throw BinderException("%s is how the MATCH_RECOGNIZE clause is planned rather than a function to call, so it "
	                      "cannot be used directly",
	                      MatchRecognizeFun::Name);
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//
void MatchRecognizePattern::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", type);
	serializer.WritePropertyWithDefault<idx_t>(101, "symbol", symbol);
	serializer.WritePropertyWithDefault(102, "at_end", at_end);
	serializer.WritePropertyWithDefault(103, "min_count", min_count);
	serializer.WritePropertyWithDefault(104, "max_count", max_count);
	serializer.WritePropertyWithDefault(105, "excluded", excluded);
	serializer.WritePropertyWithDefault(106, "reluctant", reluctant);
	serializer.WriteList(107, "children", children.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &child) { children[i]->Serialize(child); });
	});
}

unique_ptr<MatchRecognizePattern> MatchRecognizePattern::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<MatchRecognizePattern>(deserializer.ReadProperty<MatchRecognizePatternType>(100, "type"));
	result->symbol = deserializer.ReadPropertyWithDefault<idx_t>(101, "symbol");
	result->at_end = deserializer.ReadPropertyWithDefault<bool>(102, "at_end");
	result->min_count = deserializer.ReadPropertyWithDefault<optional_idx>(103, "min_count");
	result->max_count = deserializer.ReadPropertyWithDefault<optional_idx>(104, "max_count");
	result->excluded = deserializer.ReadPropertyWithDefault<bool>(105, "excluded");
	result->reluctant = deserializer.ReadPropertyWithDefault<bool>(106, "reluctant");
	deserializer.ReadList(107, "children", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &child) { result->children.push_back(Deserialize(child)); });
	});
	return result;
}

void WindowMatchRecognizeExecutor::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                             const BoundWindowFunction &function) {
	auto &config = bind_data->Cast<MatchRecognizeFunctionData>();
	serializer.WriteObject(100, "pattern", [&](Serializer &child) { config.pattern->Serialize(child); });
	serializer.WriteProperty(101, "conditions", config.conditions);
	serializer.WriteProperty(102, "symbols", config.symbols);
	serializer.WriteProperty(103, "after_match", config.after_match);
	serializer.WriteProperty(104, "after_match_variable", config.after_match_variable);
	serializer.WriteProperty(105, "depends_on_match_number", config.depends_on_match_number);
	serializer.WritePropertyWithDefault<idx_t>(108, "match_number_field", config.match_number_field);
	serializer.WriteProperty(106, "row_scoped", config.row_scoped);
	serializer.WriteList(107, "navigations", config.navigations.size(), [&](Serializer::List &list, idx_t i) {
		auto &navigation = config.navigations[i];
		list.WriteObject([&](Serializer &child) {
			child.WriteProperty(100, "last", navigation.last);
			child.WriteProperty(101, "symbol", navigation.symbol);
			child.WriteProperty(102, "field", navigation.field);
			child.WriteProperty(103, "offset", navigation.offset);
		});
	});
}

unique_ptr<FunctionData> WindowMatchRecognizeExecutor::Deserialize(Deserializer &deserializer,
                                                                   BoundWindowFunction &function) {
	auto result = make_uniq<MatchRecognizeFunctionData>();
	deserializer.ReadObject(100, "pattern",
	                        [&](Deserializer &child) { result->pattern = MatchRecognizePattern::Deserialize(child); });
	deserializer.ReadProperty(101, "conditions", result->conditions);
	deserializer.ReadProperty(102, "symbols", result->symbols);
	deserializer.ReadProperty(103, "after_match", result->after_match);
	deserializer.ReadProperty(104, "after_match_variable", result->after_match_variable);
	deserializer.ReadProperty(105, "depends_on_match_number", result->depends_on_match_number);
	result->match_number_field = deserializer.ReadPropertyWithDefault<idx_t>(108, "match_number_field");
	deserializer.ReadProperty(106, "row_scoped", result->row_scoped);
	deserializer.ReadList(107, "navigations", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &child) {
			MatchRecognizeFunctionData::Navigation navigation;
			navigation.last = child.ReadProperty<bool>(100, "last");
			navigation.symbol = child.ReadProperty<string>(101, "symbol");
			navigation.field = child.ReadProperty<idx_t>(102, "field");
			navigation.offset = child.ReadProperty<idx_t>(103, "offset");
			result->navigations.push_back(navigation);
		});
	});
	function.SetReturnType(ResultType());
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// WindowMatchRecognizeExecutor
//===--------------------------------------------------------------------===//
void WindowMatchRecognizeExecutor::GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr) {
	// matching spans a whole partition rather than a frame, so no frame boundaries are needed
}

void WindowMatchRecognizeExecutor::GetSharing(WindowExecutor &executor, WindowSharedExpressions &shared) {
	auto &config = executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();
	// the conditions are evaluated per chunk as the rows arrive
	for (auto &child : executor.wexpr.GetChildren()) {
		executor.child_idx.emplace_back(shared.RegisterSink(child));
	}
	// a condition settled per candidate row reads arbitrary rows, so the group has to stay
	auto per_row = !config.navigations.empty();
	for (auto scoped : config.row_scoped) {
		per_row = per_row || scoped;
	}
	if (per_row) {
		for (auto &child : executor.wexpr.GetChildren()) {
			executor.aux_idx.emplace_back(shared.RegisterCollection(child, false));
		}
	}
}

unique_ptr<GlobalSinkState> WindowMatchRecognizeExecutor::GetGlobal(ClientContext &client,
                                                                    const WindowExecutor &executor,
                                                                    const idx_t payload_count,
                                                                    const ValidityMask &partition_mask,
                                                                    const ValidityMask &order_mask) {
	return make_uniq<WindowMatchRecognizeGlobalState>(client, executor, payload_count, partition_mask, order_mask);
}

//! Holds the per thread machinery Sink needs to evaluate the conditions
class MatchRecognizeLocalState : public WindowExecutorLocalState {
public:
	MatchRecognizeLocalState(ExecutionContext &context, const WindowMatchRecognizeGlobalState &gstate)
	    : WindowExecutorLocalState(context, gstate) {
		auto &config = gstate.executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();
		for (idx_t i = 0; i < config.conditions.size(); i++) {
			// a condition that depends on the match being assembled has no answer yet
			if (config.row_scoped[i]) {
				continue;
			}
			auto copied = config.conditions[i]->Copy();
			types.push_back(copied->GetReturnType());
			conditions.push_back(std::move(copied));
			condition_index.push_back(i);
		}
		if (!conditions.empty()) {
			executor = make_uniq<ExpressionExecutor>(context.client, conditions);
			result.Initialize(context.client, types);
		}
	}

	vector<unique_ptr<Expression>> conditions;
	//! The condition each of the above decides, since the ones settled per row are left out
	vector<idx_t> condition_index;
	vector<LogicalType> types;
	unique_ptr<ExpressionExecutor> executor;
	DataChunk result;
};

unique_ptr<LocalSinkState> WindowMatchRecognizeExecutor::GetLocal(ExecutionContext &context,
                                                                  const GlobalSinkState &gstate) {
	return make_uniq<MatchRecognizeLocalState>(context, gstate.Cast<WindowMatchRecognizeGlobalState>());
}

void WindowMatchRecognizeExecutor::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                        idx_t input_idx, OperatorSinkInput &sink) {
	auto &gstate = sink.global_state.Cast<WindowMatchRecognizeGlobalState>();
	auto &lstate = sink.local_state.Cast<MatchRecognizeLocalState>();
	if (!lstate.executor) {
		return;
	}

	// the conditions read the columns the window is handed, in the order it was handed them
	const auto count = sink_chunk.size();
	vector<LogicalType> column_types;
	for (auto column_idx : gstate.executor.child_idx) {
		column_types.push_back(sink_chunk.data[column_idx].GetType());
	}
	DataChunk slice;
	slice.InitializeEmpty(column_types);
	for (idx_t col = 0; col < gstate.executor.child_idx.size(); col++) {
		slice.data[col].Reference(sink_chunk.data[gstate.executor.child_idx[col]]);
	}
	slice.SetCardinalityUnsafe(count);

	lstate.result.Reset();
	lstate.executor->Execute(slice, lstate.result);
	for (idx_t i = 0; i < lstate.conditions.size(); i++) {
		auto &values = gstate.condition_values[lstate.condition_index[i]];
		for (const auto &entry : lstate.result.data[i].Values<bool>()) {
			values[input_idx + entry.GetIndex()] = entry.IsValid() && entry.GetValueUnsafe() ? 1 : 0;
		}
	}
}

//! Where to resume scanning after a match spanning [match_start, match_end]
static idx_t SkipTo(const MatchRecognizeFunctionData &config, idx_t skip_symbol, idx_t match_start, idx_t match_end,
                    const vector<idx_t> &classifiers) {
	auto resume = match_end + 1;
	switch (config.after_match) {
	case MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_NEXT_ROW:
		resume = match_start + 1;
		break;
	case MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_FIRST_VAR:
	case MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_LAST_VAR: {
		const auto first = config.after_match == MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_FIRST_VAR;
		optional_idx target;
		for (idx_t row = match_start; row <= match_end; row++) {
			if (classifiers[row] != skip_symbol) {
				continue;
			}
			target = row;
			if (first) {
				break;
			}
		}
		// the symbol carries the internal prefix, which is no help to whoever wrote the query
		const auto variable = MatchRecognizeSymbolName(config.after_match_variable);
		if (!target.IsValid()) {
			throw InvalidInputException("AFTER MATCH SKIP TO %s found a match with no row matched to %s, so there is "
			                            "nowhere to resume from",
			                            variable, variable);
		}
		resume = target.GetIndex();
		if (resume == match_start) {
			throw InvalidInputException(
			    "AFTER MATCH SKIP TO %s resumes at the row the match started on, so matching cannot advance", variable);
		}
		break;
	}
	case MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_LAST_ROW:
	case MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_DEFAULT:
		break;
	}
	// never resume at or before the row the match started on, that would not terminate
	return MaxValue(resume, match_start + 1);
}

//! Work out where the partitions are, once, for every thread that reaches Finalize
static void PrepareHashGroup(WindowMatchRecognizeGlobalState &gstate) {
	lock_guard<mutex> lock(gstate.state_lock);
	if (gstate.prepared) {
		return;
	}
	gstate.prepared = true;

	idx_t partition_start = 0;
	for (idx_t payload_idx = 1; payload_idx <= gstate.payload_count; payload_idx++) {
		const auto at_end = payload_idx == gstate.payload_count;
		if (!at_end && !gstate.partition_mask.RowIsValid(payload_idx)) {
			continue;
		}
		gstate.partitions.emplace_back(partition_start, payload_idx - 1);
		partition_start = payload_idx;
	}
}

//! Decides whether a row can be a given symbol. Conditions that do not depend on the match were
//! settled in Sink; the rest are evaluated here, against the match being assembled.
class RowConditions {
public:
	RowConditions(ExecutionContext &context, WindowMatchRecognizeGlobalState &gstate,
	              const MatchRecognizeFunctionData &config, const WindowCollection &collection)
	    : context(context), gstate(gstate), config(config), collection(collection),
	      columns_idx(gstate.executor.aux_idx), executors(config.conditions.size()) {
		for (auto &condition : config.conditions) {
			conditions.push_back(condition->Copy());
		}
		// resolving where each field's value comes from once is what lets a row be assembled by copying
		// only the fields the condition being decided reads
		for (auto &condition : config.conditions) {
			ExpressionIterator::VisitExpression<BoundReferenceExpression>(
			    *condition, [&](const BoundReferenceExpression &bound_ref) {
				    field_plan.resize(MaxValue<idx_t>(field_plan.size(), bound_ref.Index() + 1));
			    });
		}
		for (idx_t i = 0; i < config.navigations.size(); i++) {
			field_plan.resize(MaxValue<idx_t>(field_plan.size(), config.navigations[i].field + 1));
			field_plan[config.navigations[i].field] = FieldPlan {FieldSource::NAVIGATION, i};
		}
		// the collected column holds the constant the matcher rewrites per match, not the number
		field_plan.resize(MaxValue<idx_t>(field_plan.size(), config.match_number_field + 1));
		field_plan[config.match_number_field] = FieldPlan {FieldSource::MATCH_NUMBER, DConstants::INVALID_INDEX};
		for (auto &condition : config.conditions) {
			unordered_set<idx_t> seen;
			vector<idx_t> fields;
			ExpressionIterator::VisitExpression<BoundReferenceExpression>(
			    *condition, [&](const BoundReferenceExpression &bound_ref) {
				    if (seen.insert(bound_ref.Index()).second) {
					    fields.push_back(bound_ref.Index());
				    }
			    });
			condition_fields.push_back(std::move(fields));
		}
		case_insensitive_map_t<idx_t> symbol_index;
		for (idx_t i = 0; i < config.symbols.size(); i++) {
			symbol_index[config.symbols[i]] = i;
		}
		auto lookup = [&](const string &name) {
			auto entry = symbol_index.find(name);
			return entry == symbol_index.end() ? DConstants::INVALID_INDEX : entry->second;
		};
		skip_symbol = lookup(config.after_match_variable);
		for (auto &navigation : config.navigations) {
			navigation_symbols.push_back(lookup(navigation.symbol));
		}
		navigation_positions.resize(config.navigations.size());
	}

	void BeginMatch(idx_t start, idx_t number) {
		match_start = start;
		match_number = number;
		for (auto &positions : navigation_positions) {
			positions.clear();
		}
		next_row = start;
	}
	idx_t SkipSymbol() const {
		return skip_symbol;
	}

	bool Matches(idx_t index, idx_t row) {
		D_ASSERT(index < config.symbols.size());
		// The positions FIRST()/LAST() need are recorded as the match assembles rather than rescanned.
		// Testing a row again discards what was recorded from there on, which belonged to an attempt
		// the matcher has abandoned.
		if (!navigation_positions.empty()) {
			D_ASSERT(row <= next_row);
			if (row < next_row) {
				for (auto &positions : navigation_positions) {
					while (!positions.empty() && positions.back() >= row) {
						positions.pop_back();
					}
				}
			}
			for (idx_t i = 0; i < navigation_symbols.size(); i++) {
				if (navigation_symbols[i] == index) {
					navigation_positions[i].push_back(row);
				}
			}
			next_row = row + 1;
		}
		if (index >= config.row_scoped.size() || !config.row_scoped[index]) {
			return gstate.condition_values[index][row] != 0;
		}

		if (!ready) {
			Initialize();
		}
		// release the variable-size values of the previous evaluation: copying into a vector appends to
		// its storage rather than replacing what is there
		if (row_grows) {
			ResetRow();
		}

		// one row of the fields this condition reads, rather than a copy of everything collected
		for (auto field : condition_fields[index]) {
			auto &plan = field_plan[field];
			auto &target = row_chunk.data[field];
			switch (plan.source) {
			case FieldSource::MATCH_NUMBER:
				target.SetValue(0, Value::UBIGINT(match_number));
				break;
			case FieldSource::NAVIGATION: {
				const auto navigated = Navigate(config.navigations[plan.navigation_idx], plan.navigation_idx, row);
				if (navigated.IsValid()) {
					// a cursor of its own, because seeking the row being tested would move this one
					CopyField(*navigation_cursors[plan.navigation_idx], field, navigated.GetIndex(), target);
				} else {
					// the match has no such row, which is what the condition reads as NULL
					FlatVector::ValidityMutable(target).SetInvalid(0);
				}
				break;
			}
			case FieldSource::CURRENT_ROW:
				CopyField(*row_cursor, field, row, target);
				break;
			}
		}

		row_result.Reset();
		if (!executors[index]) {
			executors[index] = make_uniq<ExpressionExecutor>(context.client, *conditions[index]);
		}
		executors[index]->Execute(row_chunk, row_result);
		// the entry borrows from the iterator, so the iterator has to outlive it
		const auto results = row_result.data[0].Values<bool>();
		const auto result = results[0];
		return result.IsValid() && result.GetValueUnsafe();
	}

private:
	//! Where a field of the condition input takes its value from
	enum class FieldSource : uint8_t { CURRENT_ROW, MATCH_NUMBER, NAVIGATION };
	struct FieldPlan {
		FieldSource source = FieldSource::CURRENT_ROW;
		idx_t navigation_idx = DConstants::INVALID_INDEX;
	};

	void Initialize() {
		vector<LogicalType> types;
		for (auto column_idx : columns_idx) {
			types.push_back(collection.GetTypes()[column_idx]);
		}
		// the matcher supplies its own field, which sits after the ones the plan does
		types.resize(MaxValue<idx_t>(types.size(), config.match_number_field + 1), LogicalType::UBIGINT);
		row_chunk.Initialize(context.client, types, 1);
		// one expression is evaluated at a time here, so the result holds a single column
		row_result.Initialize(context.client, vector<LogicalType> {LogicalType::BOOLEAN}, 1);
		// only a field holding values outside the vector's own data can grow
		for (auto &type : types) {
			row_grows = row_grows || !TypeIsConstantSize(type.InternalType());
		}
		ResetRow();
		field_plan.resize(MaxValue<idx_t>(field_plan.size(), types.size()));

		if (!columns_idx.empty()) {
			row_cursor = make_uniq<WindowCursor>(collection, columns_idx);
			for (idx_t i = 0; i < config.navigations.size(); i++) {
				navigation_cursors.push_back(make_uniq<WindowCursor>(collection, columns_idx));
			}
			D_ASSERT(row_cursor->chunk.ColumnCount() == columns_idx.size());
		}
		ready = true;
	}

	//! Give the assembled row back, so that what it holds lives for one evaluation
	void ResetRow() {
		row_chunk.Reset();
		// the vectors carry their own size, and reading one row out of them means saying so here
		row_chunk.SetChildCardinality(1);
		// a condition only writes the fields it reads, so the rest must read as NULL rather than as
		// whatever the allocation held
		for (auto &field : row_chunk.data) {
			field.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::ValidityMutable(field).SetInvalid(0);
		}
	}

	//! Copy one field of one collected row: seeking can replace the cursor's chunk, so it is a copy
	static void CopyField(WindowCursor &cursor, idx_t field, idx_t row, Vector &target) {
		const auto index = cursor.Seek(row);
		VectorOperations::Copy(cursor.chunk.data[field], target, index + 1, index, 0);
	}

	//! The row FIRST()/LAST() navigates to, or an invalid index when the match has no such row
	optional_idx Navigate(const MatchRecognizeFunctionData::Navigation &navigation, idx_t navigation_idx,
	                      idx_t row) const {
		if (navigation.symbol.empty()) {
			// the match covers [match_start, row], so the offset is compared against how many rows
			// that is; added to an end first it would wrap and land back inside the match
			const auto matched = row - match_start;
			if (navigation.offset > matched) {
				return optional_idx();
			}
			return navigation.last ? optional_idx(row - navigation.offset)
			                       : optional_idx(match_start + navigation.offset);
		}
		auto &positions = navigation_positions[navigation_idx];
		if (positions.size() <= navigation.offset) {
			return optional_idx();
		}
		return navigation.last ? positions[positions.size() - 1 - navigation.offset] : positions[navigation.offset];
	}

	ExecutionContext &context;
	WindowMatchRecognizeGlobalState &gstate;
	const MatchRecognizeFunctionData &config;
	const WindowCollection &collection;
	//! The collected columns the matcher reads, in the order the conditions address them
	const vector<column_t> &columns_idx;
	vector<unique_ptr<ExpressionExecutor>> executors;
	vector<unique_ptr<Expression>> conditions;
	vector<idx_t> navigation_symbols;
	//! The rows so far classified as each navigation's variable, in match order
	vector<vector<idx_t>> navigation_positions;
	//! One past the last row a classification was recorded for
	idx_t next_row = 0;
	idx_t skip_symbol = DConstants::INVALID_INDEX;
	idx_t match_start = 0;
	idx_t match_number = 1;
	DataChunk row_chunk;
	DataChunk row_result;
	//! Whether any field of the row holds its values outside the vector's own data
	bool row_grows = false;
	//! Where each field of row_chunk takes its value from
	vector<FieldPlan> field_plan;
	//! The fields each condition reads, so that deciding one copies no more than it needs
	vector<vector<idx_t>> condition_fields;
	//! Reads the row being tested. Owned by this thread, like the ones below.
	unique_ptr<WindowCursor> row_cursor;
	//! One per navigation, because two of them can be reading two different rows at once
	vector<unique_ptr<WindowCursor>> navigation_cursors;
	bool ready = false;
};

static void ScanPartitions(ExecutionContext &context, WindowMatchRecognizeGlobalState &gstate,
                           const MatchRecognizeFunctionData &config, const WindowCollection &collection) {
	auto &classifiers = gstate.classifiers;
	MatchRecognizeSpanWriter writer(gstate.SpanArena(context.client));
	RowConditions row_conditions(context, gstate, config, collection);
	SymbolMatcher symbol_matches = [&](idx_t index, idx_t row) {
		return row_conditions.Matches(index, row);
	};

	// a condition that reads MATCH_NUMBER(), or navigates at all, depends on the attempt
	auto memo = PatternMemo::PARTITION;
	for (auto scoped : config.row_scoped) {
		memo = scoped ? PatternMemo::ATTEMPT : memo;
	}
	for (auto &navigation : config.navigations) {
		// the match as a whole starts where the attempt does, but which rows were matched to a variable
		// differs between two ways of reaching the same state
		memo = navigation.symbol.empty() ? memo : PatternMemo::HISTORY;
	}

	PatternProgram program;
	program.Compile(*config.pattern, classifiers.size());
	program.Finish();
	PatternMatcher matcher(context.client, program, symbol_matches, classifiers, gstate.excluded_rows, memo);

	// partitions are independent, so the threads reaching Finalize share them out
	while (true) {
		const auto partition_idx = gstate.next_partition++;
		if (partition_idx >= gstate.partitions.size()) {
			break;
		}
		const auto partition_start = gstate.partitions[partition_idx].first;
		const auto partition_end = gstate.partitions[partition_idx].second;
		matcher.BeginPartition();

		// scan left to right, applying AFTER MATCH SKIP after every match
		idx_t match_number = 0;
		auto row = partition_start;
		while (row <= partition_end) {
			context.client.InterruptCheck();
			row_conditions.BeginMatch(row, match_number + 1);
			if (!matcher.Match(row, partition_start, partition_end + 1)) {
				row++;
				continue;
			}
			// an empty match is still reported, but covers no rows - the scan steps past it rather than
			// skipping, or it would never move
			if (matcher.match_end <= row) {
				match_number++;
				writer.Append(gstate.row_spans[row], MatchRecognizeSpan {0, match_number, row, row, true, false, true});
				row++;
				continue;
			}
			// a match can never reach beyond its own partition
			const auto match_end = MinValue(matcher.match_end - 1, partition_end);
			match_number++;

			for (idx_t match_row = row; match_row <= match_end; match_row++) {
				writer.Append(gstate.row_spans[match_row],
				              MatchRecognizeSpan {classifiers[match_row], match_number, row, match_end,
				                                  match_row == row, gstate.excluded_rows[match_row] != 0, false});
			}
			row = SkipTo(config, row_conditions.SkipSymbol(), row, match_end, classifiers);
		}
	}
}

void WindowMatchRecognizeExecutor::Finalize(ExecutionContext &context, optional_ptr<WindowCollection> collection,
                                            OperatorSinkInput &sink) {
	auto &gstate = sink.global_state.Cast<WindowMatchRecognizeGlobalState>();
	auto &config = gstate.executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();

	// we always start with a new partition
	D_ASSERT(gstate.partition_mask.RowIsValid(0));

	PrepareHashGroup(gstate);
	ScanPartitions(context, gstate, config, *collection);
}

void WindowMatchRecognizeExecutor::GetData(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds,
                                           Vector &result, idx_t row_idx, OperatorSinkInput &sink) {
	auto &gstate = sink.global_state.Cast<WindowMatchRecognizeGlobalState>();
	// one chunk of memberships at a time rather than the whole input. Matching is over by now - every
	// thread has left Finalize - so the shared state below is only read.
	const auto count = bounds.size();
	auto writer = FlatVector::Writer<VectorListType<SpanStruct>>(result, count);
	for (idx_t i = 0; i < count; i++) {
		const auto row = row_idx + i;
		auto &row_spans = gstate.row_spans[row];
		auto node = row_spans.first;
		for (auto &membership : writer.WriteList(row_spans.count)) {
			auto &span = node->span;
			membership.WriteValue([&](auto &classifier, auto &match_number, auto &is_match_start, auto &is_match_end,
			                          auto &match_start, auto &match_end, auto &is_excluded, auto &is_empty,
			                          auto &row_index) {
				// an empty match covers no rows, so no row of it classified as anything
				if (span.empty) {
					classifier.WriteNull();
				} else {
					classifier.WriteValue(string_t(gstate.classifier_names[span.symbol]));
				}
				match_number.WriteValue(span.match_number);
				is_match_start.WriteValue(span.is_match_start);
				is_match_end.WriteValue(row == span.match_end);
				match_start.WriteValue(span.match_start);
				match_end.WriteValue(span.match_end);
				is_excluded.WriteValue(span.excluded);
				is_empty.WriteValue(span.empty);
				row_index.WriteValue(row);
			});
			node = node->next;
		}
	}
}

WindowFunction MatchRecognizeFun::GetFunction() {
	// called with the columns the conditions read; everything else arrives as bind data
	WindowFunction fun(Name, {LogicalType::ANY}, WindowMatchRecognizeExecutor::ResultType(),
	                   ExpressionType::WINDOW_FUNCTION, WindowMatchRecognizeExecutor::Bind,
	                   WindowMatchRecognizeExecutor::GetBounds, WindowMatchRecognizeExecutor::GetSharing,
	                   WindowMatchRecognizeExecutor::GetGlobal, WindowMatchRecognizeExecutor::GetLocal,
	                   WindowMatchRecognizeExecutor::Sink, WindowMatchRecognizeExecutor::Finalize,
	                   WindowMatchRecognizeExecutor::GetData);
	fun.SetVarArgs(LogicalType::ANY);

	auto &signature = fun.GetSignature();
	signature = FunctionSignature(vector<FunctionParameter>(), WindowMatchRecognizeExecutor::ResultType());
	signature.AddParameter(Identifier("columns"), LogicalType::ANY);

	fun.SetSerializeCallback(WindowMatchRecognizeExecutor::Serialize);
	fun.SetDeserializeCallback(WindowMatchRecognizeExecutor::Deserialize);

	return fun;
}

} // namespace duckdb
