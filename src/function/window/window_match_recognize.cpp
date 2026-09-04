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
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

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
	IS_EMPTY
};

//	MATCH_NUMBER() is the first field of the packed column struct
static constexpr idx_t MATCH_NUMBER_FIELD = 0;

struct WindowMatchRecognizeGlobalState : WindowExecutorGlobalState {
	WindowMatchRecognizeGlobalState(ClientContext &client, const WindowExecutor &executor, const idx_t payload_count,
	                                const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(client, executor, payload_count, partition_mask, order_mask),
	      result_vec(executor.wexpr.GetReturnType(), payload_count), spans(payload_count) {
		auto &config = executor.wexpr.BindInfo()->Cast<MatchRecognizeFunctionData>();
		condition_values.resize(config.conditions.size());
		for (auto &values : condition_values) {
			values.assign(payload_count, 0);
		}
		classifiers.resize(payload_count);
		excluded_rows.resize(payload_count);
		D_ASSERT(result_vec.GetType().id() == LogicalTypeId::LIST);
	}

	//! One row of the result list
	struct Span {
		idx_t symbol;
		idx_t match_number;
		bool is_match_start;
		idx_t match_start;
		idx_t match_end;
		bool excluded;
		//! An empty match covers no rows at all; this span only marks where it happened
		bool empty;
	};

	//! Build the list vector the operator reads from the spans collected during matching
	void MaterializeSpans(const vector<string> &symbols) {
		idx_t total = 0;
		for (auto &row : spans) {
			total += row.size();
		}
		ListVector::Reserve(result_vec, total);
		ListVector::SetListSize(result_vec, total);
		auto list_data = FlatVector::GetDataMutable<list_entry_t>(result_vec);
		auto &child = ListVector::GetChildMutable(result_vec);
		auto &fields = StructVector::GetEntries(child);

		idx_t offset = 0;
		for (idx_t row = 0; row < spans.size(); row++) {
			list_data[row].offset = offset;
			list_data[row].length = spans[row].size();
			for (auto &span : spans[row]) {
				fields[CLASSIFIER].SetValue(offset, span.empty ? Value(LogicalType::VARCHAR)
				                                               : Value(MatchRecognizeSymbolName(symbols[span.symbol])));
				fields[MATCH_NUMBER].SetValue(offset, Value::UBIGINT(span.match_number));
				fields[IS_MATCH_START].SetValue(offset, Value::BOOLEAN(span.is_match_start));
				fields[IS_MATCH_END].SetValue(offset, Value::BOOLEAN(row == span.match_end));
				fields[MATCH_START].SetValue(offset, Value::UBIGINT(span.match_start));
				fields[MATCH_END].SetValue(offset, Value::UBIGINT(span.match_end));
				fields[IS_EXCLUDED].SetValue(offset, Value::BOOLEAN(span.excluded));
				fields[IS_EMPTY].SetValue(offset, Value::BOOLEAN(span.empty));
				offset++;
			}
		}
	}

	mutex state_lock;
	//! Set up once; the threads then take partitions from the cursor below
	bool prepared = false;
	//! Partitions are independent, so the threads that reach Finalize share them out
	vector<pair<idx_t, idx_t>> partitions;
	atomic<idx_t> next_partition {0};
	atomic<idx_t> completed_partitions {0};
	//! The variable that classified each row, written only by the thread that owns the partition
	vector<idx_t> classifiers;
	//! Whether the pattern matched each row inside a {- -}, written alongside the classifier above
	vector<uint8_t> excluded_rows;
	//! Materialised only when a condition has to be settled per row, and then shared by the threads
	DataChunk rows;
	//! One boolean per symbol per row. Sink fills these as rows arrive, over disjoint ranges, so the
	//! threads do not need to coordinate.
	vector<vector<uint8_t>> condition_values;

	Vector result_vec;
	vector<vector<Span>> spans;
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
	                                              {"is_empty", LogicalType::BOOLEAN}}));
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
	// Deserialization rebinds the function after the configuration arguments are gone, filling the
	// optional parameters with NULL. The bind data is restored by the deserialize callback instead.
	const auto configured = arguments.size() == 7 &&
	                        arguments[3]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	                        !arguments[3]->Cast<BoundConstantExpression>().GetValue().IsNull();
	if (!configured) {
		input.GetBoundFunction().GetArguments().resize(MinValue<idx_t>(1, arguments.size()));
		input.GetBoundFunction().SetReturnType(ResultType());
		return nullptr;
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
		break;
	}
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
		return make_uniq_base<Expression, BoundQuantifierExpression>(std::move(child), min_count, max_count, excluded);
	}
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
		for (auto &condition : config.conditions) {
			auto copied = condition->Copy();
			types.push_back(copied->GetReturnType());
			conditions.push_back(std::move(copied));
		}
		if (!conditions.empty()) {
			executor = make_uniq<ExpressionExecutor>(context.client, conditions);
			result.Initialize(context.client, types);
		}
	}

	vector<unique_ptr<Expression>> conditions;
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
		auto &values = gstate.condition_values[i];
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
enum class PatternOp : uint8_t { SYMBOL, SPLIT, JUMP, MATCH };

struct PatternInstruction {
	PatternOp op = PatternOp::MATCH;
	//! SYMBOL: the variable to test, and whether it sits inside a {- -}
	idx_t symbol = 0;
	bool excluded = false;
	//! SPLIT: where to go first, then where to go if that fails. JUMP: where to go.
	idx_t target = 0;
	idx_t alternative = 0;
};

using SymbolMatcher = std::function<bool(idx_t symbol, idx_t row)>;

struct PatternProgram {
	vector<PatternInstruction> code;

	//! `limit` bounds a counted quantifier: no repetition can match fewer than one row, so more
	//! repetitions than there are rows can never be reached
	void Compile(const Expression &node, idx_t limit, bool excluded = false) {
		switch (node.GetExpressionType()) {
		case ExpressionType::VALUE_CONSTANT: {
			PatternInstruction symbol;
			symbol.op = PatternOp::SYMBOL;
			symbol.symbol = NumericCast<idx_t>(node.Cast<BoundConstantExpression>().GetValue().GetValue<uint64_t>());
			symbol.excluded = excluded;
			code.push_back(symbol);
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
			// more mandatory repetitions than there are rows can never be satisfied, and one past the
			// row count is already enough to be sure of that
			const idx_t declared_min = quantifier.min_count.IsValid() ? quantifier.min_count.GetIndex() : 0;
			const idx_t min_count = MinValue(declared_min, limit + 1);
			for (idx_t i = 0; i < min_count; i++) {
				Compile(*quantifier.child, limit, inner);
			}
			if (!quantifier.max_count.IsValid()) {
				// greedy: going round again is preferred over leaving the loop
				const auto loop = code.size();
				auto split = Emit(PatternOp::SPLIT);
				code[split].target = code.size();
				Compile(*quantifier.child, limit, inner);
				code[Emit(PatternOp::JUMP)].target = loop;
				code[split].alternative = code.size();
				break;
			}
			const auto max_count = MinValue(quantifier.max_count.GetIndex(), min_count + limit);
			vector<idx_t> exits;
			for (idx_t i = min_count; i < max_count; i++) {
				auto split = Emit(PatternOp::SPLIT);
				code[split].target = code.size();
				exits.push_back(split);
				Compile(*quantifier.child, limit, inner);
			}
			for (auto exit_split : exits) {
				code[exit_split].alternative = code.size();
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
		code.push_back(instruction);
		return code.size() - 1;
	}
};

//! Walks the compiled program depth first, preferring the branch a greedy quantifier wants, and
//! stops at the first way through - which is the match the standard asks for.
//!
//! A (instruction, row) pair that has been explored once and did not lead to a match cannot lead to
//! one later, so it is never explored again. That is what keeps the search polynomial where plain
//! backtracking is exponential, and it holds only while a variable's condition depends on nothing
//! but the row it is testing. A condition that navigates the match being assembled, or reads
//! MATCH_NUMBER(), depends on the rows matched so far, so for those the record is dropped between
//! attempts and the matcher backtracks in the ordinary way.
struct PatternMatcher {
	PatternMatcher(const PatternProgram &program_p, const SymbolMatcher &symbol_matches_p, vector<idx_t> &classifiers_p,
	               vector<uint8_t> &excluded_rows_p, bool conditions_are_row_local_p)
	    : program(program_p), symbol_matches(symbol_matches_p), classifiers(classifiers_p),
	      excluded_rows(excluded_rows_p), conditions_are_row_local(conditions_are_row_local_p),
	      row_count(classifiers_p.size()), explored(program_p.code.size() * (classifiers_p.size() + 1), 0),
	      epoch(conditions_are_row_local_p ? 1 : 0) {
	}

	//! Match starting at `start`, with `input_size` the first row beyond the partition
	bool Match(idx_t start, idx_t input_size) {
		if (!conditions_are_row_local) {
			// stepping the epoch retires every record at once; the array itself only has to be
			// cleared when the epoch wraps around to a value old records could still hold
			if (++epoch == 0) {
				std::fill(explored.begin(), explored.end(), 0);
				epoch = 1;
			}
		}
		attempt_marks.clear();
		pending.clear();
		pending.emplace_back(0, start);
		while (!pending.empty()) {
			auto state = pending.back();
			pending.pop_back();
			auto pc = state.first;
			auto offset = state.second;
			while (true) {
				const auto slot_index = pc * (row_count + 1) + offset;
				auto &slot = explored[slot_index];
				if (slot == epoch) {
					break;
				}
				slot = epoch;
				if (conditions_are_row_local) {
					attempt_marks.push_back(slot_index);
				}
				auto &instruction = program.code[pc];
				if (instruction.op == PatternOp::MATCH) {
					// a record is only proof of a dead end when its subtree was searched to
					// exhaustion. This search stopped early, so a later start must be free to walk
					// these states again - persisting them would hide its matches.
					for (auto mark : attempt_marks) {
						explored[mark] = 0;
					}
					match_end = offset;
					return true;
				}
				if (instruction.op == PatternOp::JUMP) {
					pc = instruction.target;
					continue;
				}
				if (instruction.op == PatternOp::SPLIT) {
					pending.emplace_back(instruction.alternative, offset);
					pc = instruction.target;
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
			}
		}
		return false;
	}

	//! One past the last row of the match, valid after Match() returned true
	idx_t match_end = 0;

private:
	const PatternProgram &program;
	const SymbolMatcher &symbol_matches;
	vector<idx_t> &classifiers;
	vector<uint8_t> &excluded_rows;
	bool conditions_are_row_local;
	idx_t row_count;
	//! One record per (instruction, row): the epoch in which that state was walked
	vector<uint8_t> explored;
	//! Records matching this belong to the current attempt; the partition-wide memo of row-local
	//! conditions never retires, so there it stays at 1
	uint8_t epoch;
	//! The records this attempt wrote into the partition-wide memo, undone if it finds a match
	vector<idx_t> attempt_marks;
	vector<pair<idx_t, idx_t>> pending;
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
		if (target.IsValid()) {
			resume = target.GetIndex();
			if (resume == match_start) {
				throw InvalidInputException(
				    "AFTER MATCH SKIP TO %s resumes at the row the match started on, so matching cannot advance",
				    config.after_match_variable);
			}
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
	RowConditions row_conditions(context, gstate, config);
	SymbolMatcher symbol_matches = [&](idx_t index, idx_t row) {
		return row_conditions.Matches(index, row);
	};

	// a condition that navigates the match, or reads MATCH_NUMBER(), depends on the rows matched so
	// far rather than only on the row it is testing
	bool conditions_are_row_local = true;
	for (auto scoped : config.row_scoped) {
		conditions_are_row_local = conditions_are_row_local && !scoped;
	}

	PatternProgram program;
	program.Compile(*config.pattern, classifiers.size());
	program.Finish();
	PatternMatcher matcher(program, symbol_matches, classifiers, gstate.excluded_rows, conditions_are_row_local);

	// Partitions are independent, so every thread that reaches Finalize takes them from a shared
	// cursor rather than one thread doing the whole hash group.
	while (true) {
		const auto partition_idx = gstate.next_partition++;
		if (partition_idx >= gstate.partitions.size()) {
			break;
		}
		const auto partition_start = gstate.partitions[partition_idx].first;
		const auto partition_end = gstate.partitions[partition_idx].second;

		// scan the partition left to right, applying AFTER MATCH SKIP after every match. Rows that are
		// not part of any match keep a NULL struct, which filters them out downstream.
		idx_t match_number = 0;
		auto row = partition_start;
		while (row <= partition_end) {
			row_conditions.BeginMatch(row, match_number + 1);
			if (!matcher.Match(row, partition_end + 1)) {
				row++;
				continue;
			}
			// a pattern that can match nothing produces an empty match, which covers no rows. It is
			// still a match and still reported, but the span only marks where it happened, and the
			// scan has to step past it rather than skip, or it would never move.
			if (matcher.match_end <= row) {
				match_number++;
				gstate.spans[row].push_back(
				    WindowMatchRecognizeGlobalState::Span {0, match_number, true, row, row, false, true});
				row++;
				continue;
			}
			// a match can never reach beyond its own partition
			const auto match_end = MinValue(matcher.match_end - 1, partition_end);
			match_number++;

			for (idx_t match_row = row; match_row <= match_end; match_row++) {
				gstate.spans[match_row].push_back(
				    WindowMatchRecognizeGlobalState::Span {classifiers[match_row], match_number, match_row == row, row,
				                                           match_end, gstate.excluded_rows[match_row] != 0, false});
			}
			row = SkipTo(config, row_conditions.SkipSymbol(), row, match_end, classifiers);
		}

		// the thread that finishes the last partition publishes the result
		if (++gstate.completed_partitions == gstate.partitions.size()) {
			gstate.MaterializeSpans(config.symbols);
		}
	}
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
	// the spans were materialised in Finalize, which every thread has left by the time any of them
	// reads here, so this only reads shared state and does not have to be serialised
	result.Slice(gstate.result_vec, row_idx, row_idx + bounds.size());
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
