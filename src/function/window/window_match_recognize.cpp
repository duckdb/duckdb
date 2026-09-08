#include "duckdb/function/window/window_match_recognize.hpp"

#include "duckdb/function/match_recognize.hpp"
#include "duckdb/function/window/match_recognize_functions.hpp"
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
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//	Column indexes into the result struct
enum MatchRecognizeResult : idx_t {
	CLASSIFIER = 0,
	MATCH_NUMBER,
	IS_MATCH_START,
	IS_MATCH_END,
	MATCH_START,
	MATCH_END,
	IS_EXCLUDED,
	IS_EMPTY,
	ROW_INDEX
};

//	MATCH_NUMBER() is the first field of the packed column struct
static constexpr idx_t MATCH_NUMBER_FIELD = 0;

//! One membership of a row in a match. Overlapping matches each give the rows they cover one of
//! these, so there are as many as there are (row, match) pairs and not as many as there are rows.
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

//! Appends memberships on behalf of one thread. The rows of a partition belong to the thread that
//! took it, so only the blocks are its own business; they are handed to the state it writes into
//! once it is done, which is before anything reads them back.
//!
//! The blocks come from the buffer manager's allocator, so that storage which grows with the
//! memberships rather than with the rows grows against the memory limit and not outside it.
struct MatchRecognizeSpanWriter {
	//! Big enough that a block is taken rarely, small enough that a pattern matching almost nothing
	//! does not reserve much for it
	static constexpr idx_t BLOCK_SPANS = 2048;

	explicit MatchRecognizeSpanWriter(ClientContext &client)
	    : allocator(BufferManager::GetBufferManager(client).GetBufferAllocator()) {
	}

	void Append(MatchRecognizeRowSpans &row, const MatchRecognizeSpan &span) {
		if (next == end) {
			blocks.push_back(allocator.Allocate(BLOCK_SPANS * sizeof(MatchRecognizeRowSpans::Node)));
			next = reinterpret_cast<MatchRecognizeRowSpans::Node *>(blocks.back().get());
			end = next + BLOCK_SPANS;
		}
		auto node = next++;
		node->span = span;
		node->next = nullptr;
		if (row.count++ == 0) {
			row.first = node;
		} else {
			row.last->next = node;
		}
		row.last = node;
	}

	Allocator &allocator;
	vector<AllocatedData> blocks;
	MatchRecognizeRowSpans::Node *next = nullptr;
	MatchRecognizeRowSpans::Node *end = nullptr;
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
		D_ASSERT(executor.wexpr.GetReturnType().id() == LogicalTypeId::LIST);
	}

	//! Take over the blocks a thread filled, so that they outlive the walk that wrote them
	void KeepSpans(MatchRecognizeSpanWriter &writer) {
		lock_guard<mutex> guard(state_lock);
		for (auto &block : writer.blocks) {
			span_blocks.push_back(std::move(block));
		}
		writer.blocks.clear();
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
	//! Materialised only when a condition has to be settled per row, and then shared by the threads
	DataChunk rows;
	//! One boolean per symbol per row. Sink fills these as rows arrive, over disjoint ranges, so the
	//! threads do not need to coordinate.
	vector<vector<uint8_t>> condition_values;

	//! Where each row's memberships start, and how many of them there are
	vector<MatchRecognizeRowSpans> row_spans;
	//! The blocks the memberships above live in
	vector<AllocatedData> span_blocks;
};

LogicalType WindowMatchRecognizeExecutor::ResultType() {
	// One entry per match a row takes part in: overlapping matches each keep their own, and the plan
	// unnests the list. Rows that matched nothing get an empty list, which unnest drops.
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
//! Point a condition's column references at the window's argument list
static void RebindToArguments(unique_ptr<Expression> &expr, const expression_map_t<idx_t> &argument_index,
                              idx_t match_number_index, bool &reads_match_number) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		// the matcher evaluates a condition per candidate row, which a subquery cannot be reduced to
		throw BinderException("A DEFINE condition may not contain a subquery");
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto entry = argument_index.find(*expr);
		if (entry == argument_index.end()) {
			throw BinderException("A DEFINE condition may only reference columns of the MATCH_RECOGNIZE input");
		}
		if (entry->second == match_number_index) {
			reads_match_number = true;
		}
		expr = make_uniq<BoundReferenceExpression>(expr->GetReturnType(), entry->second);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		RebindToArguments(child, argument_index, match_number_index, reads_match_number);
	});
}

//! Replace each pattern leaf's symbol name with its index
static void ResolvePatternSymbols(unique_ptr<Expression> &pattern, const case_insensitive_map_t<idx_t> &symbol_index) {
	if (pattern->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		auto &constant = pattern->Cast<BoundConstantExpression>();
		if (constant.GetValue().type().id() == LogicalTypeId::VARCHAR) {
			auto symbol = constant.GetValue().GetValue<string>();
			auto entry = symbol_index.find(symbol);
			if (entry == symbol_index.end()) {
				throw InternalException("MATCH_RECOGNIZE pattern symbol %s has no condition", symbol);
			}
			pattern = make_uniq<BoundConstantExpression>(Value::UBIGINT(entry->second));
		}
		return;
	}
	if (pattern->GetExpressionType() == ExpressionType::ANCHOR) {
		return;
	}
	switch (pattern->GetExpressionType()) {
	case ExpressionType::ALTERNATION: {
		auto &alternation = pattern->Cast<BoundAlternationExpression>();
		ResolvePatternSymbols(alternation.child_left, symbol_index);
		ResolvePatternSymbols(alternation.child_right, symbol_index);
		break;
	}
	case ExpressionType::CONCATENATION:
		for (auto &child : pattern->Cast<BoundConcatenationExpression>().children) {
			ResolvePatternSymbols(child, symbol_index);
		}
		break;
	case ExpressionType::QUANTIFIER:
		ResolvePatternSymbols(pattern->Cast<BoundQuantifierExpression>().child, symbol_index);
		break;
	default:
		break;
	}
}

unique_ptr<FunctionData> WindowMatchRecognizeExecutor::Bind(BindWindowFunctionInput &input) {
	auto &arguments = input.GetArguments();
	// Everything after the columns is configuration the MATCH_RECOGNIZE binder builds, so a call that
	// does not carry it did not come from one. Deserialization restores the bind data through the
	// deserialize callback and never reaches here.
	const auto configured = arguments.size() == 7 &&
	                        arguments[3]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	                        !arguments[3]->Cast<BoundConstantExpression>().GetValue().IsNull();
	if (!configured) {
		throw BinderException("%s is how the MATCH_RECOGNIZE clause is planned rather than a function to call, so it "
		                      "cannot be used directly",
		                      MatchRecognizeFun::Name);
	}
	// the casts below are only safe for the shape the MATCH_RECOGNIZE binder builds
	const bool packed = arguments[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	                    arguments[1]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	                    (arguments[2]->GetExpressionClass() == ExpressionClass::PATTERN ||
	                     arguments[2]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) &&
	                    arguments[4]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	                    arguments[5]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	                    arguments[6]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
	if (!packed) {
		throw BinderException("%s was called with something other than the configuration the MATCH_RECOGNIZE clause "
		                      "builds for it",
		                      MatchRecognizeFun::Name);
	}

	auto bind_data = make_uniq<MatchRecognizeFunctionData>();
	bind_data->after_match = static_cast<MatchRecognizeAfterMatch>(
	    arguments[5]->Cast<BoundConstantExpression>().GetValue().GetValue<uint8_t>());
	auto &skip_variable = arguments[4]->Cast<BoundConstantExpression>().GetValue();
	if (!skip_variable.IsNull()) {
		bind_data->after_match_variable = skip_variable.GetValue<string>();
	}
	for (auto &symbol : ListValue::GetChildren(arguments[3]->Cast<BoundConstantExpression>().GetValue())) {
		bind_data->symbols.push_back(symbol.GetValue<string>());
	}
	bind_data->pattern = std::move(arguments[2]);

	// the columns are packed in argument order, which is the order the conditions address them in
	expression_map_t<idx_t> argument_index;
	auto &column_pack = arguments[0]->Cast<BoundFunctionExpression>();
	for (idx_t i = 0; i < column_pack.GetChildren().size(); i++) {
		argument_index[*column_pack.GetChildren()[i]] = i;
	}

	// the conditions are only packed so that they get bound; they are evaluated by the matcher
	unordered_set<idx_t> navigation_fields;
	for (auto &navigation : ListValue::GetChildren(arguments[6]->Cast<BoundConstantExpression>().GetValue())) {
		auto &fields = StructValue::GetChildren(navigation);
		MatchRecognizeFunctionData::Navigation spec;
		spec.last = fields[0].GetValue<bool>();
		spec.symbol = fields[1].IsNull() ? string() : fields[1].GetValue<string>();
		spec.field = NumericCast<idx_t>(fields[2].GetValue<uint64_t>());
		spec.offset = NumericCast<idx_t>(fields[3].GetValue<uint64_t>());
		navigation_fields.insert(spec.field);
		bind_data->navigations.push_back(spec);
	}

	auto &condition_pack = arguments[1]->Cast<BoundFunctionExpression>();
	for (auto &condition : condition_pack.GetChildrenMutable()) {
		bool reads_match_number = false;
		RebindToArguments(condition, argument_index, 0, reads_match_number);
		bool reads_navigation = false;
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(
		    *condition, [&](const BoundReferenceExpression &bound_ref) {
			    reads_navigation = reads_navigation || navigation_fields.count(bound_ref.Index()) > 0;
		    });
		// Both kinds depend on the match being assembled, so both are settled per candidate row.
		// Re-deciding them for a whole partition after every match would be quadratic.
		bind_data->row_scoped.push_back(reads_navigation || reads_match_number);
		bind_data->depends_on_match_number = bind_data->depends_on_match_number || reads_match_number;
		bind_data->conditions.push_back(std::move(condition));
	}
	if (bind_data->conditions.size() != bind_data->symbols.size()) {
		throw BinderException("MATCH_RECOGNIZE has a condition for every pattern symbol");
	}

	// the matcher compares symbols on every candidate row, so the leaves carry an index into
	// symbols rather than the name itself
	case_insensitive_map_t<idx_t> symbol_index;
	for (idx_t i = 0; i < bind_data->symbols.size(); i++) {
		symbol_index[bind_data->symbols[i]] = i;
	}
	ResolvePatternSymbols(bind_data->pattern, symbol_index);

	auto &bound_function = input.GetBoundFunction();
	bound_function.GetArguments().resize(1);
	bound_function.SetReturnType(ResultType());

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//
// The pattern is built from expression types that only exist here, so it is written out directly
// rather than through the expression serializer.
static void SerializePattern(Serializer &serializer, const Expression &pattern) {
	serializer.WriteProperty(100, "type", pattern.GetExpressionType());
	switch (pattern.GetExpressionType()) {
	case ExpressionType::ALTERNATION: {
		auto &alternation = pattern.Cast<BoundAlternationExpression>();
		serializer.WriteObject(101, "left",
		                       [&](Serializer &child) { SerializePattern(child, *alternation.child_left); });
		serializer.WriteObject(102, "right",
		                       [&](Serializer &child) { SerializePattern(child, *alternation.child_right); });
		break;
	}
	case ExpressionType::CONCATENATION: {
		auto &concatenation = pattern.Cast<BoundConcatenationExpression>();
		serializer.WriteList(101, "children", concatenation.children.size(), [&](Serializer::List &list, idx_t i) {
			list.WriteObject([&](Serializer &child) { SerializePattern(child, *concatenation.children[i]); });
		});
		break;
	}
	case ExpressionType::QUANTIFIER: {
		auto &quantifier = pattern.Cast<BoundQuantifierExpression>();
		serializer.WriteObject(101, "child", [&](Serializer &child) { SerializePattern(child, *quantifier.child); });
		serializer.WriteProperty(102, "min_count", quantifier.min_count);
		serializer.WriteProperty(103, "max_count", quantifier.max_count);
		serializer.WriteProperty(104, "excluded", quantifier.excluded);
		serializer.WritePropertyWithDefault(105, "reluctant", quantifier.reluctant, false);
		break;
	}
	case ExpressionType::ANCHOR:
		serializer.WriteProperty(101, "at_end", pattern.Cast<BoundAnchorExpression>().at_end);
		break;
	case ExpressionType::VALUE_CONSTANT:
		serializer.WriteProperty(101, "symbol", pattern.Cast<BoundConstantExpression>().GetValue());
		break;
	default:
		throw SerializationException("Unsupported MATCH_RECOGNIZE pattern node");
	}
}

static unique_ptr<Expression> DeserializePattern(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<ExpressionType>(100, "type");
	switch (type) {
	case ExpressionType::ALTERNATION: {
		unique_ptr<Expression> left;
		unique_ptr<Expression> right;
		deserializer.ReadObject(101, "left", [&](Deserializer &child) { left = DeserializePattern(child); });
		deserializer.ReadObject(102, "right", [&](Deserializer &child) { right = DeserializePattern(child); });
		return make_uniq_base<Expression, BoundAlternationExpression>(std::move(left), std::move(right));
	}
	case ExpressionType::CONCATENATION: {
		vector<unique_ptr<Expression>> children;
		deserializer.ReadList(101, "children", [&](Deserializer::List &list, idx_t i) {
			list.ReadObject([&](Deserializer &child) { children.push_back(DeserializePattern(child)); });
		});
		return make_uniq_base<Expression, BoundConcatenationExpression>(std::move(children));
	}
	case ExpressionType::QUANTIFIER: {
		unique_ptr<Expression> child;
		deserializer.ReadObject(101, "child", [&](Deserializer &inner) { child = DeserializePattern(inner); });
		auto min_count = deserializer.ReadProperty<optional_idx>(102, "min_count");
		auto max_count = deserializer.ReadProperty<optional_idx>(103, "max_count");
		auto excluded = deserializer.ReadProperty<bool>(104, "excluded");
		auto reluctant = deserializer.ReadPropertyWithExplicitDefault<bool>(105, "reluctant", false);
		return make_uniq_base<Expression, BoundQuantifierExpression>(std::move(child), min_count, max_count, excluded,
		                                                             reluctant);
	}
	case ExpressionType::ANCHOR:
		return make_uniq_base<Expression, BoundAnchorExpression>(deserializer.ReadProperty<bool>(101, "at_end"));
	case ExpressionType::VALUE_CONSTANT:
		return make_uniq_base<Expression, BoundConstantExpression>(deserializer.ReadProperty<Value>(101, "symbol"));
	default:
		throw SerializationException("Unsupported MATCH_RECOGNIZE pattern node");
	}
}

void WindowMatchRecognizeExecutor::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                             const BoundWindowFunction &function) {
	auto &config = bind_data->Cast<MatchRecognizeFunctionData>();
	serializer.WriteObject(100, "pattern", [&](Serializer &child) { SerializePattern(child, *config.pattern); });
	serializer.WriteProperty(101, "conditions", config.conditions);
	serializer.WriteProperty(102, "symbols", config.symbols);
	serializer.WriteProperty(103, "after_match", config.after_match);
	serializer.WriteProperty(104, "after_match_variable", config.after_match_variable);
	serializer.WriteProperty(105, "depends_on_match_number", config.depends_on_match_number);
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
	deserializer.ReadObject(100, "pattern", [&](Deserializer &child) { result->pattern = DeserializePattern(child); });
	deserializer.ReadProperty(101, "conditions", result->conditions);
	deserializer.ReadProperty(102, "symbols", result->symbols);
	deserializer.ReadProperty(103, "after_match", result->after_match);
	deserializer.ReadProperty(104, "after_match_variable", result->after_match_variable);
	deserializer.ReadProperty(105, "depends_on_match_number", result->depends_on_match_number);
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
	// conditions settled per candidate row need the group kept around to read arbitrary rows from
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
			// a condition that depends on the match being assembled has no answer yet, and evaluating
			// it here would raise its errors against a match state that does not exist
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

	const auto count = sink_chunk.size();
	auto &columns = StructVector::GetEntries(sink_chunk.data[gstate.executor.child_idx[0]]);
	vector<LogicalType> column_types;
	for (auto &column : columns) {
		column_types.push_back(column.GetType());
	}
	DataChunk slice;
	slice.InitializeEmpty(column_types);
	for (idx_t col = 0; col < columns.size(); col++) {
		slice.data[col].Reference(columns[col]);
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

//! Materialise the whole hash group. Row offsets are global to it, which is what the matcher and the
//! condition evaluation both index by.
static void FetchHashGroup(ColumnDataCollection &input, DataChunk &result_chunk) {
	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
	input.InitializeScanChunk(scan_chunk);
	input.InitializeScan(scan_state);
	while (input.Scan(scan_state, scan_chunk)) {
		result_chunk.Append(scan_chunk);
	}
}

//! An instruction of the compiled pattern. Compiling the tree into a program makes "what to do
//! after this node" a position in that program rather than a place in a recursive walk, which is
//! what lets the matcher recognise a state it has already explored.
enum class PatternOp : uint8_t { SYMBOL, SPLIT, JUMP, ANCHOR, MATCH };

struct PatternInstruction {
	PatternOp op = PatternOp::MATCH;
	//! SYMBOL: the variable to test, and whether it sits inside a {- -}
	idx_t symbol = 0;
	bool excluded = false;
	//! SPLIT: where to go first, then where to go if that fails. JUMP: where to go.
	idx_t target = 0;
	idx_t alternative = 0;
	//! ANCHOR: whether it holds past the partition's last row rather than at its first
	bool at_end = false;
};

using SymbolMatcher = std::function<bool(idx_t symbol, idx_t row)>;

//! The fewest rows a pattern node can match. A repetition of it can only be reached that many times
//! fewer than there are rows, which is what keeps a counted quantifier around one from expanding into
//! repetitions that could never be taken.
static idx_t MinConsumption(const Expression &node) {
	const auto saturating_add = [](idx_t left, idx_t right) {
		return left > NumericLimits<idx_t>::Maximum() - right ? NumericLimits<idx_t>::Maximum() : left + right;
	};
	switch (node.GetExpressionType()) {
	case ExpressionType::VALUE_CONSTANT:
		// a symbol takes exactly one row
		return 1;
	case ExpressionType::CONCATENATION: {
		idx_t total = 0;
		for (auto &child : node.Cast<BoundConcatenationExpression>().children) {
			total = saturating_add(total, MinConsumption(*child));
		}
		return total;
	}
	case ExpressionType::ALTERNATION: {
		auto &alternation = node.Cast<BoundAlternationExpression>();
		return MinValue(MinConsumption(*alternation.child_left), MinConsumption(*alternation.child_right));
	}
	case ExpressionType::QUANTIFIER: {
		auto &quantifier = node.Cast<BoundQuantifierExpression>();
		if (!quantifier.min_count.IsValid() || quantifier.min_count.GetIndex() == 0) {
			return 0;
		}
		const auto child = MinConsumption(*quantifier.child);
		const auto count = quantifier.min_count.GetIndex();
		return child != 0 && count > NumericLimits<idx_t>::Maximum() / child ? NumericLimits<idx_t>::Maximum()
		                                                                     : child * count;
	}
	default:
		// an anchor takes no row
		return 0;
	}
}

struct PatternProgram {
	//! A pattern whose program grows past this cannot be matched in any useful time anyway, and the
	//! memo below is one record per instruction per row
	static constexpr idx_t MAX_INSTRUCTIONS = 1 << 20;

	vector<PatternInstruction> code;

	//! `limit` bounds a counted quantifier: a repetition matching `n` rows can be reached at most
	//! `limit / n` times before the rows run out, so more repetitions than that are unreachable
	void Compile(const Expression &node, idx_t limit, bool excluded = false) {
		switch (node.GetExpressionType()) {
		case ExpressionType::VALUE_CONSTANT: {
			PatternInstruction symbol;
			symbol.op = PatternOp::SYMBOL;
			symbol.symbol = NumericCast<idx_t>(node.Cast<BoundConstantExpression>().GetValue().GetValue<uint64_t>());
			symbol.excluded = excluded;
			Push(symbol);
			break;
		}
		case ExpressionType::ANCHOR: {
			PatternInstruction anchor;
			anchor.op = PatternOp::ANCHOR;
			anchor.at_end = node.Cast<BoundAnchorExpression>().at_end;
			Push(anchor);
			break;
		}
		case ExpressionType::CONCATENATION:
			for (auto &child : node.Cast<BoundConcatenationExpression>().children) {
				Compile(*child, limit, excluded);
			}
			break;
		case ExpressionType::ALTERNATION: {
			auto &alternation = node.Cast<BoundAlternationExpression>();
			auto split = Emit(PatternOp::SPLIT);
			code[split].target = code.size();
			Compile(*alternation.child_left, limit, excluded);
			auto jump = Emit(PatternOp::JUMP);
			code[split].alternative = code.size();
			Compile(*alternation.child_right, limit, excluded);
			code[jump].target = code.size();
			break;
		}
		case ExpressionType::QUANTIFIER: {
			auto &quantifier = node.Cast<BoundQuantifierExpression>();
			const auto inner = excluded || quantifier.excluded;
			// one repetition past what the rows allow already makes the program unsatisfiable, which is
			// what every further one would have been too
			const idx_t declared_min = quantifier.min_count.IsValid() ? quantifier.min_count.GetIndex() : 0;
			const idx_t consumption = MinConsumption(*quantifier.child);
			const idx_t reachable = consumption == 0 ? limit + 1 : limit / consumption + 1;
			const idx_t min_count = MinValue(declared_min, reachable);
			for (idx_t i = 0; i < min_count; i++) {
				Compile(*quantifier.child, limit, inner);
			}
			// the matcher takes a split's target before its alternative, so which of the two is the
			// repetition and which is the way out is what greedy and reluctant come down to
			const auto reluctant = quantifier.reluctant;
			if (!quantifier.max_count.IsValid()) {
				const auto loop = code.size();
				auto split = Emit(PatternOp::SPLIT);
				const auto again = code.size();
				Compile(*quantifier.child, limit, inner);
				code[Emit(PatternOp::JUMP)].target = loop;
				const auto leave = code.size();
				code[split].target = reluctant ? leave : again;
				code[split].alternative = reluctant ? again : leave;
				break;
			}
			const auto max_count = MinValue(quantifier.max_count.GetIndex(), min_count + reachable);
			vector<idx_t> exits;
			for (idx_t i = min_count; i < max_count; i++) {
				auto split = Emit(PatternOp::SPLIT);
				const auto again = code.size();
				(reluctant ? code[split].alternative : code[split].target) = again;
				exits.push_back(split);
				Compile(*quantifier.child, limit, inner);
			}
			for (auto exit_split : exits) {
				(reluctant ? code[exit_split].target : code[exit_split].alternative) = code.size();
			}
			break;
		}
		default:
			throw InternalException("Unsupported MATCH_RECOGNIZE pattern node");
		}
	}

	void Finish() {
		Emit(PatternOp::MATCH);
	}

private:
	idx_t Emit(PatternOp op) {
		PatternInstruction instruction;
		instruction.op = op;
		Push(instruction);
		return code.size() - 1;
	}

	void Push(const PatternInstruction &instruction) {
		if (code.size() >= MAX_INSTRUCTIONS) {
			throw InvalidInputException(
			    "The MATCH_RECOGNIZE pattern compiles to more than %llu instructions, which is more than can be "
			    "matched. Repetition counts multiply, so nesting them is what usually gets here.",
			    MAX_INSTRUCTIONS);
		}
		code.push_back(instruction);
	}
};

//! How long a walked state stays proof that the search below it is a dead end. That depends on what
//! the conditions read, because a state is only a dead end for as long as its conditions answer the
//! same way.
enum class PatternMemo : uint8_t {
	//! Conditions read nothing but the row they test, so a dead end stays one for the whole partition
	PARTITION,
	//! A condition reads MATCH_NUMBER(), which is fixed within an attempt but differs between them
	ATTEMPT,
	//! A condition navigates the match, so a state's answer depends on the rows matched before it
	HISTORY
};

//! Walks the compiled program depth first, preferring the branch a greedy quantifier wants, and
//! stops at the first way through - which is the match the standard asks for.
//!
//! A (instruction, row) pair that has been explored once and did not lead to a match cannot lead to
//! one later, so it is never explored again. That is what keeps the search polynomial where plain
//! backtracking is exponential, and it holds only while the conditions answer the same way each time
//! that pair is reached. A condition that navigates the match being assembled reads the rows matched
//! before the one it tests, so two ways of reaching the same pair can disagree and the record has to
//! go.
//!
//! What is left there is cycle detection rather than memoisation: a walk may not reach the same
//! instruction twice without matching a row in between, because everything it could do the second
//! time it already did the first. That is a property of the path being walked, not of the search as
//! a whole, so the marks belong to the backtracking state. They are kept in an undo log: taking an
//! alternative back off the stack restores the marks to what they were on the path that reached it,
//! and matching a row opens a scope of its own that the marks left behind cannot answer for.
struct PatternMatcher {
	PatternMatcher(ClientContext &context_p, const PatternProgram &program_p, const SymbolMatcher &symbol_matches_p,
	               vector<idx_t> &classifiers_p, vector<uint8_t> &excluded_rows_p, PatternMemo memo_p)
	    : context(context_p), program(program_p), symbol_matches(symbol_matches_p), classifiers(classifiers_p),
	      excluded_rows(excluded_rows_p), memo(memo_p), row_count(classifiers_p.size()) {
		if (memo == PatternMemo::HISTORY) {
			// no row is matched within a scope, so every state it marks sits at the same row and one
			// mark per instruction is enough
			history_marks.assign(program.code.size(), 0);
			return;
		}
		const auto rows = row_count + 1;
		if (program.code.size() > NumericLimits<idx_t>::Maximum() / rows) {
			throw OutOfMemoryException("The MATCH_RECOGNIZE pattern needs a record per instruction per row, which does "
			                           "not fit in memory for %llu instructions over %llu rows",
			                           program.code.size(), row_count);
		}
		explored_size = program.code.size() * rows;
		// through the buffer manager's allocator, so that it counts against the memory limit
		explored = BufferManager::GetBufferManager(context).GetBufferAllocator().Allocate(explored_size);
		ClearExplored();
	}

	//! A partition is matched within its own bounds, and the anchors and row offsets a record was
	//! taken under only hold there, so nothing is carried over from the one before
	void BeginPartition() {
		if (memo == PatternMemo::HISTORY) {
			// every attempt opens a scope of its own below, and a mark only outlives the walk that took it
			return;
		}
		NextEpoch();
	}

	//! Match starting at `start`, within the partition [`partition_start`, `input_size`)
	bool Match(idx_t start, idx_t partition_start, idx_t input_size) {
		if (memo == PatternMemo::ATTEMPT) {
			NextEpoch();
		}
		attempt_marks.clear();
		pending.clear();
		// the marks of the attempt before this one belong to walks that are over
		UnwindHistory(0);
		pending.push_back(PendingState {0, start, NextScope(), 0});
		while (!pending.empty()) {
			auto state = pending.back();
			pending.pop_back();
			auto pc = state.pc;
			auto offset = state.offset;
			// this alternative was left behind on a path that has since walked on; the marks it may
			// read are the ones that path had taken when it was pushed
			UnwindHistory(state.trail_size);
			auto walk = state.scope;
			while (true) {
				// a walk can be long, and a pattern that has to try again for every row longer still
				if (++steps >= INTERRUPT_INTERVAL) {
					steps = 0;
					context.InterruptCheck();
				}
				if (!Visit(pc, offset, walk)) {
					break;
				}
				auto &instruction = program.code[pc];
				if (instruction.op == PatternOp::MATCH) {
					// a record is only proof of a dead end when its subtree was searched to
					// exhaustion. This search stopped early, so a later start must be free to walk
					// these states again - persisting them would hide its matches.
					for (auto mark : attempt_marks) {
						explored.get()[mark] = 0;
					}
					match_end = offset;
					return true;
				}
				if (instruction.op == PatternOp::JUMP) {
					pc = instruction.target;
					continue;
				}
				if (instruction.op == PatternOp::SPLIT) {
					pending.push_back(PendingState {instruction.alternative, offset, walk, history_trail.size()});
					pc = instruction.target;
					continue;
				}
				if (instruction.op == PatternOp::ANCHOR) {
					// an anchor takes no row, so it either holds where the walk stands or it does not
					if (offset != (instruction.at_end ? input_size : partition_start)) {
						break;
					}
					pc++;
					continue;
				}
				if (offset >= input_size) {
					break;
				}
				// the row is tentatively this symbol while its condition is evaluated, which is what
				// lets LAST(X.c) see the row being tested
				classifiers[offset] = instruction.symbol;
				if (!symbol_matches(instruction.symbol, offset)) {
					break;
				}
				excluded_rows[offset] = instruction.excluded ? 1 : 0;
				pc++;
				offset++;
				// the walk now carries one more matched row, which is a history of its own
				walk = NextScope();
			}
		}
		return false;
	}

	//! One past the last row of the match, valid after Match() returned true
	idx_t match_end = 0;

private:
	//! A state still to be walked. Its scope names the stretch of the walk that reached it and
	//! matched no row, and `trail_size` the marks that stretch had taken by then.
	struct PendingState {
		idx_t pc;
		idx_t offset;
		idx_t scope;
		idx_t trail_size;
	};

	//! Record that this state is being walked, or report that it already was
	bool Visit(idx_t pc, idx_t offset, idx_t walk) {
		if (memo == PatternMemo::HISTORY) {
			auto &mark = history_marks[pc];
			if (mark == walk) {
				return false;
			}
			// what the mark said before is what an alternative pushed before now has to see again
			history_trail.push_back(HistoryMark {pc, mark});
			mark = walk;
			return true;
		}
		const auto slot_index = pc * (row_count + 1) + offset;
		auto &slot = explored.get()[slot_index];
		if (slot == epoch) {
			return false;
		}
		slot = epoch;
		if (memo == PatternMemo::PARTITION) {
			attempt_marks.push_back(slot_index);
		}
		return true;
	}

	//! Retire every record taken so far. Stepping a counter does that without touching the records
	//! themselves; only a counter that wrapped back onto a value they could still hold needs the
	//! array cleared.
	void NextEpoch() {
		if (++epoch == 0) {
			ClearExplored();
			epoch = 1;
		}
	}

	//! Open the stretch of the walk that starts where the last row was matched. Scope 0 is the one no
	//! mark was ever taken in, so counting up hands out an identity nothing can already be holding
	//! and a 64 bit counter never comes back round to one that is.
	idx_t NextScope() {
		return memo == PatternMemo::HISTORY ? ++history_scope : 0;
	}

	//! Put the marks back the way the path being resumed left them
	void UnwindHistory(idx_t trail_size) {
		while (history_trail.size() > trail_size) {
			auto &entry = history_trail.back();
			history_marks[entry.pc] = entry.scope;
			history_trail.pop_back();
		}
	}

	void ClearExplored() {
		memset(explored.get(), 0, explored_size);
	}

	//! How many instructions the walk takes between two checks for a cancelled query
	static constexpr idx_t INTERRUPT_INTERVAL = 4096;

	ClientContext &context;
	const PatternProgram &program;
	const SymbolMatcher &symbol_matches;
	vector<idx_t> &classifiers;
	vector<uint8_t> &excluded_rows;
	PatternMemo memo;
	idx_t row_count;
	idx_t steps = 0;
	//! One record per (instruction, row): the epoch in which that state was walked
	AllocatedData explored;
	idx_t explored_size = 0;
	//! Records matching this belong to the current partition or attempt
	uint8_t epoch = 0;
	//! One mark per instruction, naming the scope the walk last visited it in. No row is matched
	//! within a scope, so every state it marks sits at the same row.
	vector<idx_t> history_marks;
	idx_t history_scope = 0;
	//! What a mark said before the walk overwrote it, so that backtracking can put it back
	struct HistoryMark {
		idx_t pc;
		idx_t scope;
	};
	vector<HistoryMark> history_trail;
	//! The records this attempt wrote into the partition-wide memo, undone if it finds a match
	vector<idx_t> attempt_marks;
	vector<PendingState> pending;
};

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

// this gets called per partition
//! Work out where the partitions are, and materialise the rows if a condition has to be settled per
//! row. Both are shared by every thread that reaches Finalize, so this happens once.
static void PrepareHashGroup(ExecutionContext &context, WindowMatchRecognizeGlobalState &gstate,
                             const MatchRecognizeFunctionData &config, WindowCollection &collection) {
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

	auto per_row = !config.navigations.empty();
	for (auto scoped : config.row_scoped) {
		per_row = per_row || scoped;
	}
	if (per_row) {
		gstate.rows.Initialize(context.client, collection.inputs->Types(), gstate.payload_count);
		FetchHashGroup(*collection.inputs, gstate.rows);
	}
}

//! Match the partitions of the hash group, taking them from the shared cursor until they run out.
//! Decides whether a row can be a given symbol. Conditions that do not depend on the match were
//! settled in Sink; the rest are evaluated here, against the match being assembled.
class RowConditions {
public:
	RowConditions(ExecutionContext &context, WindowMatchRecognizeGlobalState &gstate,
	              const MatchRecognizeFunctionData &config)
	    : context(context), gstate(gstate), config(config),
	      columns_idx(gstate.executor.aux_idx.empty() ? 0 : gstate.executor.aux_idx[0]),
	      executors(config.conditions.size()) {
		for (auto &condition : config.conditions) {
			conditions.push_back(condition->Copy());
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
		// Every classification passes through here, so the occurrence positions FIRST()/LAST() need
		// can be kept as the match assembles instead of rescanning it per row. Testing a row again
		// discards what was recorded from there on: those classifications belonged to an attempt the
		// matcher has abandoned.
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

		auto &columns = StructVector::GetEntries(gstate.rows.data[columns_idx]);
		if (!ready) {
			vector<LogicalType> column_types;
			for (auto &column : columns) {
				column_types.push_back(column.GetType());
			}
			row_chunk.Initialize(context.client, column_types, 1);
			// one expression is evaluated at a time here, so the result holds a single column
			row_result.Initialize(context.client, vector<LogicalType> {LogicalType::BOOLEAN}, 1);
			// Each column is a dictionary over the group's rows whose selection is shared with the
			// vector below, so pointing it at another row costs no allocation.
			row_sel.Initialize(1);
			for (idx_t col = 0; col < columns.size(); col++) {
				row_chunk.data[col].Slice(columns[col], row_sel, 1);
			}
			navigation_sels.resize(config.navigations.size());
			navigation_sliced.assign(config.navigations.size(), false);
			for (auto &navigation_sel : navigation_sels) {
				navigation_sel.Initialize(1);
			}
			row_chunk.SetCardinalityUnsafe(1);
			ready = true;
		}

		row_sel.set_index(0, row);
		if (config.depends_on_match_number) {
			row_chunk.data[MATCH_NUMBER_FIELD].Reference(Value::UBIGINT(match_number), count_t(1));
		}
		for (idx_t i = 0; i < config.navigations.size(); i++) {
			auto &navigation = config.navigations[i];
			auto target = Navigate(navigation, i, row);
			if (target.IsValid()) {
				// a NULL reference below replaces the dictionary, so it has to be rebuilt after one
				if (!navigation_sliced[i]) {
					row_chunk.data[navigation.field].Slice(columns[navigation.field], navigation_sels[i], 1);
					navigation_sliced[i] = true;
				}
				navigation_sels[i].set_index(0, target.GetIndex());
			} else {
				row_chunk.data[navigation.field].Reference(Value(columns[navigation.field].GetType()), count_t(1));
				navigation_sliced[i] = false;
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
	//! The row FIRST()/LAST() navigates to, or an invalid index when the match has no such row
	optional_idx Navigate(const MatchRecognizeFunctionData::Navigation &navigation, idx_t navigation_idx,
	                      idx_t row) const {
		if (navigation.symbol.empty()) {
			// the match as a whole, counted from whichever end
			if (navigation.last) {
				return row < match_start + navigation.offset ? optional_idx() : optional_idx(row - navigation.offset);
			}
			const auto target = match_start + navigation.offset;
			return target > row ? optional_idx() : optional_idx(target);
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
	idx_t columns_idx;
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
	//! The selection behind the row_chunk dictionaries: entry 0 is the row being tested
	SelectionVector row_sel;
	//! One selection per navigation field, pointing at the row the navigation resolved to
	vector<SelectionVector> navigation_sels;
	//! Whether the navigation field still holds its dictionary rather than a NULL reference
	vector<bool> navigation_sliced;
	bool ready = false;
};

static void ScanPartitions(ExecutionContext &context, WindowMatchRecognizeGlobalState &gstate,
                           const MatchRecognizeFunctionData &config) {
	auto &classifiers = gstate.classifiers;
	MatchRecognizeSpanWriter writer(context.client);
	RowConditions row_conditions(context, gstate, config);
	SymbolMatcher symbol_matches = [&](idx_t index, idx_t row) {
		return row_conditions.Matches(index, row);
	};

	// a condition that reads MATCH_NUMBER(), or navigates the match at all, depends on which attempt
	// it is being tested in
	auto memo = PatternMemo::PARTITION;
	for (auto scoped : config.row_scoped) {
		memo = scoped ? PatternMemo::ATTEMPT : memo;
	}
	for (auto &navigation : config.navigations) {
		// navigating the match as a whole reads where it started, which the attempt fixes. Navigating a
		// variable's rows reads which rows were matched to it, and that is what differs between two
		// ways of reaching the same state.
		memo = navigation.symbol.empty() ? memo : PatternMemo::HISTORY;
	}

	PatternProgram program;
	program.Compile(*config.pattern, classifiers.size());
	program.Finish();
	PatternMatcher matcher(context.client, program, symbol_matches, classifiers, gstate.excluded_rows, memo);

	// Partitions are independent, so every thread that reaches Finalize takes them from a shared
	// cursor rather than one thread doing the whole hash group.
	while (true) {
		const auto partition_idx = gstate.next_partition++;
		if (partition_idx >= gstate.partitions.size()) {
			break;
		}
		const auto partition_start = gstate.partitions[partition_idx].first;
		const auto partition_end = gstate.partitions[partition_idx].second;
		matcher.BeginPartition();

		// scan the partition left to right, applying AFTER MATCH SKIP after every match. Rows that are
		// not part of any match keep a NULL struct, which filters them out downstream.
		idx_t match_number = 0;
		auto row = partition_start;
		while (row <= partition_end) {
			context.client.InterruptCheck();
			row_conditions.BeginMatch(row, match_number + 1);
			if (!matcher.Match(row, partition_start, partition_end + 1)) {
				row++;
				continue;
			}
			// a pattern that can match nothing produces an empty match, which covers no rows. It is
			// still a match and still reported, but the span only marks where it happened, and the
			// scan has to step past it rather than skip, or it would never move.
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

	// the memberships have to outlive this walk, which the blocks they sit in do not
	gstate.KeepSpans(writer);
}

void WindowMatchRecognizeExecutor::Finalize(ExecutionContext &context, optional_ptr<WindowCollection> collection,
                                            OperatorSinkInput &sink) {
	auto &gstate = sink.global_state.Cast<WindowMatchRecognizeGlobalState>();
	auto &config = gstate.executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();

	// we always start with a new partition
	D_ASSERT(gstate.partition_mask.RowIsValid(0));

	PrepareHashGroup(context, gstate, config, *collection);
	ScanPartitions(context, gstate, config);
}

void WindowMatchRecognizeExecutor::GetData(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds,
                                           Vector &result, idx_t row_idx, OperatorSinkInput &sink) {
	auto &gstate = sink.global_state.Cast<WindowMatchRecognizeGlobalState>();
	auto &symbols = gstate.executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>().symbols;
	// The list is built for the rows being read rather than for the whole input, so the memberships
	// are laid out flat a chunk at a time and never all at once. Matching is over by the time anything
	// reads here - every thread has left Finalize - so this only reads shared state.
	const auto count = bounds.size();
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		total += gstate.row_spans[row_idx + i].count;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::Reserve(result, total);
	ListVector::SetListSize(result, total);
	auto list_data = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &child = ListVector::GetChildMutable(result);
	auto &fields = StructVector::GetEntries(child);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto row = row_idx + i;
		auto &row_spans = gstate.row_spans[row];
		list_data[i].offset = offset;
		list_data[i].length = row_spans.count;
		for (auto node = row_spans.first; node; node = node->next) {
			auto &span = node->span;
			fields[CLASSIFIER].SetValue(offset, span.empty ? Value(LogicalType::VARCHAR)
			                                               : Value(MatchRecognizeSymbolName(symbols[span.symbol])));
			fields[MATCH_NUMBER].SetValue(offset, Value::UBIGINT(span.match_number));
			fields[IS_MATCH_START].SetValue(offset, Value::BOOLEAN(span.is_match_start));
			fields[IS_MATCH_END].SetValue(offset, Value::BOOLEAN(row == span.match_end));
			fields[MATCH_START].SetValue(offset, Value::UBIGINT(span.match_start));
			fields[MATCH_END].SetValue(offset, Value::UBIGINT(span.match_end));
			fields[IS_EXCLUDED].SetValue(offset, Value::BOOLEAN(span.excluded));
			fields[IS_EMPTY].SetValue(offset, Value::BOOLEAN(span.empty));
			fields[ROW_INDEX].SetValue(offset, Value::UBIGINT(row));
			offset++;
		}
	}
}

WindowFunction MatchRecognizeFun::GetFunction() {
	// Everything after the columns is configuration that Bind() moves into the function data, so a
	// bound call carries only the first argument. Declaring the rest optional keeps the signature
	// resolvable both before and after that.
	WindowFunction fun(Name, {LogicalType::ANY}, WindowMatchRecognizeExecutor::ResultType(),
	                   ExpressionType::WINDOW_FUNCTION, WindowMatchRecognizeExecutor::Bind,
	                   WindowMatchRecognizeExecutor::GetBounds, WindowMatchRecognizeExecutor::GetSharing,
	                   WindowMatchRecognizeExecutor::GetGlobal, WindowMatchRecognizeExecutor::GetLocal,
	                   WindowMatchRecognizeExecutor::Sink, WindowMatchRecognizeExecutor::Finalize,
	                   WindowMatchRecognizeExecutor::GetData);

	auto &signature = fun.GetSignature();
	signature = FunctionSignature(vector<FunctionParameter>(), WindowMatchRecognizeExecutor::ResultType());
	signature.AddParameter(Identifier("columns"), LogicalType::ANY);
	signature.AddParameter(Identifier("conditions"), LogicalType::ANY, Value());
	signature.AddParameter(Identifier("pattern"), LogicalType::ANY, Value());
	signature.AddParameter(Identifier("symbols"), LogicalType::LIST(LogicalType::VARCHAR), Value());
	signature.AddParameter(Identifier("after_match_variable"), LogicalType::VARCHAR, Value());
	signature.AddParameter(Identifier("after_match"), LogicalType::UTINYINT, Value());
	signature.AddParameter(Identifier("navigations"), LogicalType::ANY, Value());

	fun.SetSerializeCallback(WindowMatchRecognizeExecutor::Serialize);
	fun.SetDeserializeCallback(WindowMatchRecognizeExecutor::Deserialize);

	return fun;
}

} // namespace duckdb
