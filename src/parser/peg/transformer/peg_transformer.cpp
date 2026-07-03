#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

TransformFrameResultTarget::TransformFrameResultTarget(transform_frame_index_t frame_index_p, idx_t slot_p)
    : frame_index(frame_index_p), slot(slot_p) {
}

TransformStackFrame::TransformStackFrame(transform_frame_index_t frame_index_p, ParseResult &parse_result_p,
                                         const TransformFrameOps &ops_p,
                                         optional<TransformFrameResultTarget> result_target_p)
    : frame_index(frame_index_p), parse_result(parse_result_p), ops(ops_p), result_target(result_target_p) {
}

void TransformStackFrame::ReserveChildSlots(idx_t count) {
	child_results.resize(count);
}

void TransformStackFrame::SetChildResult(idx_t slot, unique_ptr<TransformResultValue> result) {
	if (slot >= child_results.size()) {
		throw InternalException("Invalid trampoline transformer result slot %llu for rule '%s'", slot, ops.name);
	}
	if (!result) {
		throw InternalException("Cannot set nullptr trampoline transformer result for slot %llu in rule '%s'", slot,
		                        ops.name);
	}
	if (child_results[slot]) {
		throw InternalException("Duplicate trampoline transformer result for slot %llu in rule '%s'", slot, ops.name);
	}
	child_results[slot] = std::move(result);
}

TransformStack::TransformStack(PEGTransformer &transformer_p) : transformer(transformer_p) {
}

transform_frame_index_t TransformStack::PushFrame(ParseResult &parse_result, const TransformFrameOps &ops,
                                                  optional<TransformFrameResultTarget> result_target) {
	if (!ops.initialize || !ops.finalize) {
		throw InternalException("Incomplete trampoline transformer ops for rule '%s'", ops.name);
	}
	auto frame_index = frames.size();
	if (result_target && result_target->frame_index >= frame_index) {
		throw InternalException("Invalid trampoline transformer parent frame index %llu for frame %llu",
		                        result_target->frame_index, frame_index);
	}
	frames.push_back(make_uniq<TransformStackFrame>(frame_index, parse_result, ops, result_target));
	frame_stack.push_back(frame_index);
	return frame_index;
}

TransformStackFrame &TransformStack::GetFrame(transform_frame_index_t frame_index) {
	if (frame_index >= frames.size() || !frames[frame_index]) {
		throw InternalException("Invalid trampoline transformer frame index %llu", frame_index);
	}
	return *frames[frame_index];
}

const TransformStackFrame &TransformStack::GetFrame(transform_frame_index_t frame_index) const {
	if (frame_index >= frames.size() || !frames[frame_index]) {
		throw InternalException("Invalid trampoline transformer frame index %llu", frame_index);
	}
	return *frames[frame_index];
}

unique_ptr<TransformResultValue> TransformStack::ExecuteInternal(ParseResult &parse_result,
                                                                 const TransformFrameOps &ops) {
	D_ASSERT(frames.empty());
	D_ASSERT(frame_stack.empty());
	if (!frames.empty() || !frame_stack.empty()) {
		throw InternalException("Cannot execute a non-empty trampoline transformer stack");
	}

	PushFrame(parse_result, ops, optional<TransformFrameResultTarget>());
	while (!frame_stack.empty()) {
		auto frame_index = frame_stack.back();
		auto &frame = GetFrame(frame_index);
		switch (frame.state) {
		case TransformFrameState::INITIALIZE:
			frame.state = TransformFrameState::WAITING;
			frame.ops.initialize(transformer, *this, frame);
			break;
		case TransformFrameState::WAITING: {
			auto result = frame.ops.finalize(transformer, *this, frame);
			if (!result) {
				throw InternalException("Trampoline transformer finalize for rule '%s' returned nullptr",
				                        frame.ops.name);
			}
			frame_stack.pop_back();
			if (!frame.result_target) {
				return result;
			}
			DeliverResult(frame, std::move(result));
			break;
		}
		default:
			throw InternalException("Invalid trampoline transformer frame state for rule '%s'", frame.ops.name);
		}
	}
	throw InternalException("Trampoline transformer stack completed without a root result");
}

void TransformStack::DeliverResult(TransformStackFrame &frame, unique_ptr<TransformResultValue> result) {
	if (!frame.result_target) {
		throw InternalException("Cannot deliver trampoline transformer result for root frame '%s'", frame.ops.name);
	}
	auto &target = frame.result_target.value();
	auto &parent = GetFrame(target.frame_index);
	parent.SetChildResult(target.slot, std::move(result));
}

string TransformStack::FormatFrame(transform_frame_index_t frame_index) const {
	auto &frame = GetFrame(frame_index);
	stringstream result;
	result << "#" << frame.frame_index << " " << frame.ops.name;
	if (!frame.parse_result.name.empty() && frame.parse_result.name != frame.ops.name) {
		result << " parse_result=" << frame.parse_result.name;
	}
	result << " state=" << EnumUtil::ToString(frame.state);
	if (frame.parse_result.offset.IsValid()) {
		result << " offset=" << frame.parse_result.offset.GetIndex();
	}
	if (frame.result_target) {
		result << " parent=#" << frame.result_target->frame_index << " slot=" << frame.result_target->slot;
	}
	return result.str();
}

string TransformStack::FormatParentChain(transform_frame_index_t frame_index) const {
	stringstream result;
	optional_idx current(frame_index);
	bool first = true;
	while (current.IsValid()) {
		if (!first) {
			result << " <- ";
		}
		result << FormatFrame(current.GetIndex());
		first = false;
		auto &frame = GetFrame(current.GetIndex());
		if (!frame.result_target) {
			break;
		}
		current = frame.result_target->frame_index;
	}
	return result.str();
}

string TransformStack::FormatStack() const {
	stringstream result;
	for (idx_t i = 0; i < frame_stack.size(); i++) {
		if (i > 0) {
			result << "\n";
		}
		result << FormatFrame(frame_stack[i]);
	}
	return result.str();
}

void PEGTransformer::ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type) {
	// Mixing positional/auto-increment and named parameters is not supported
	if (last_type == PreparedParamType::INVALID) {
		return;
	}
	if (last_type == PreparedParamType::NAMED) {
		if (new_type != PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
	if (last_type != PreparedParamType::NAMED) {
		if (new_type == PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
}

bool PEGTransformer::GetParam(const Identifier &identifier, idx_t &index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	auto entry = named_parameter_map.find(identifier);
	if (entry == named_parameter_map.end()) {
		return false;
	}
	index = entry->second;
	return true;
}

void PEGTransformer::SetParam(const Identifier &identifier, idx_t index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	last_param_type = type;
	D_ASSERT(!named_parameter_map.count(identifier));
	named_parameter_map[identifier] = index;
}

void PEGTransformer::ClearParameters() {
	prepared_statement_parameter_index = 0;
	last_param_type = PreparedParamType::INVALID;
	named_parameter_map.clear();
	has_anonymous_parameters = false;
}

void PEGTransformer::Clear() {
	ClearParameters();
	pivot_entries.clear();
	stored_cte_map.clear();
}

idx_t PEGTransformer::ParamCount() const {
	return prepared_statement_parameter_index;
}

void PEGTransformer::SetParamCount(idx_t new_count) {
	prepared_statement_parameter_index = new_count;
}

unique_ptr<SQLStatement> PEGTransformer::GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTypeInfo>();
	info->temporary = true;
	info->internal = false;
	info->SetQualifiedName(QualifiedName(Identifier(std::move(entry->enum_name))));
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

	// generate the query that will result in the enum creation
	unique_ptr<QueryNode> subselect;
	if (!entry->subquery) {
		auto select_node = std::move(entry->base);
		auto columnref = entry->column->Copy();
		auto cast = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(columnref));
		select_node->select_list.push_back(std::move(cast));

		auto is_not_null =
		    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(entry->column));
		select_node->where_clause = std::move(is_not_null);

		// order by the column
		select_node->modifiers.push_back(make_uniq<DistinctModifier>());
		auto modifier = make_uniq<OrderModifier>();
		modifier->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT,
		                              make_uniq<ConstantExpression>(Value::INTEGER(1)));
		select_node->modifiers.push_back(std::move(modifier));
		subselect = std::move(select_node);
	} else {
		subselect = std::move(entry->subquery);
	}

	auto select = make_uniq<SelectStatement>();
	select->node = std::move(subselect);
	info->query = std::move(select);
	info->type = LogicalType::INVALID;

	result->info = std::move(info);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformer::CreatePivotStatement(unique_ptr<SQLStatement> statement) {
	auto result = make_uniq<MultiStatement>();
	for (auto &pivot : pivot_entries) {
		if (pivot->has_parameters) {
			throw ParserException(
			    "PIVOT statements with pivot elements extracted from the data cannot have parameters in their source.\n"
			    "In order to use parameters the PIVOT values must be manually specified, e.g.:\n"
			    "PIVOT ... ON %s IN (val1, val2, ...)",
			    pivot->column->ToString());
		}
		auto enum_stmt = GenerateCreateEnumStmt(std::move(pivot));
		enum_stmt->query = enum_stmt->ToString();
		result->statements.push_back(std::move(enum_stmt));
	}
	result->stmt_location = statement->stmt_location;
	result->stmt_length = statement->stmt_length;
	statement->query = statement->ToString();
	result->statements.push_back(std::move(statement));
	return std::move(result);
}

void PEGTransformer::PivotEntryCheck(const string &type) {
	if (!pivot_entries.empty()) {
		throw ParserException(
		    "PIVOT statements with pivot elements extracted from the data cannot be used in %ss.\nIn order to use "
		    "PIVOT in a %s the PIVOT values must be manually specified, e.g.:\nPIVOT ... ON %s IN (val1, val2, ...)",
		    type, type, pivot_entries[0]->column->ToString());
	}
}

void PEGTransformer::ExtractCTEsRecursive(CommonTableExpressionMap &cte_map) {
	// Traverse the stack from the most recent scope back to the global scope
	// Use reverse iterator if you push new scopes to the back
	for (auto it = stored_cte_map.rbegin(); it != stored_cte_map.rend(); ++it) {
		auto &current_scope = it->get();
		for (auto &entry : current_scope.map) {
			// Check if this CTE name is already in our result map
			if (cte_map.map.find(entry.first) == cte_map.map.end()) {
				cte_map.map[entry.first] = entry.second->Copy();
			}
		}
	}
}

bool PEGTransformer::IsWindowFrameDefault(WindowBoundary start, WindowBoundary end) {
	bool start_is_default = (start == WindowBoundary::UNBOUNDED_PRECEDING);
	bool end_is_default = (end == WindowBoundary::CURRENT_ROW_RANGE);
	return start_is_default && end_is_default;
}

unique_ptr<WindowExpression> PEGTransformer::GetWindowClause(const Identifier &window_name) {
	auto it = window_clauses.find(window_name);
	if (it == window_clauses.end()) {
		throw ParserException("window \"%s\" does not exist", window_name);
	}
	return unique_ptr_cast<ParsedExpression, WindowExpression>(it->second->Copy());
}

void PEGTransformer::SetQueryLocation(ParsedExpression &expr, optional_idx query_location) {
	expr.SetQueryLocation(query_location);
}

void PEGTransformer::SetQueryLocation(TableRef &ref, optional_idx query_location) {
	ref.query_location = query_location;
}

} // namespace duckdb
