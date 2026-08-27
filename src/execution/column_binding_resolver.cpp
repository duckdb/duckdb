#include "duckdb/execution/column_binding_resolver.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

struct ColumnBindingVerificationState {
	explicit ColumnBindingVerificationState(LogicalOperator &root) {
		LogicalPlanCompilerPath root_path;
		IndexOperator(root, root_path);
	}

	reference_map_t<LogicalOperator, LogicalPlanCompilerPath> operator_paths;
	reference_map_t<Expression, LogicalPlanCompilerPath> expression_paths;
	reference_set_t<LogicalOperator> resolved_inputs;
	reference_set_t<LogicalOperator> resolved_outputs;
	vector<LogicalPlanCompilerIssue> issues;

	const LogicalPlanCompilerPath &GetPath(LogicalOperator &op) const {
		auto entry = operator_paths.find(reference<LogicalOperator>(op));
		if (entry == operator_paths.end()) {
			throw InternalException("Logical operator is missing from the compiler verification path index");
		}
		return entry->second;
	}

	const LogicalPlanCompilerPath &GetPath(Expression &expr) const {
		auto entry = expression_paths.find(reference<Expression>(expr));
		if (entry == expression_paths.end()) {
			throw InternalException("Expression is missing from the compiler verification path index");
		}
		return entry->second;
	}

	void AddInvalidBinding(BoundColumnRefExpression &expr, const vector<ColumnBinding> &bindings) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INVALID_BINDING;
		issue.path = GetPath(expr);
		AddBindingFacts(issue, expr.Binding());
		issue.message = StringUtil::Format(
		    "Failed to bind column reference %s [table=%llu, column=%llu] against %llu bindings", expr.GetAlias(),
		    expr.Binding().table_index.index, expr.Binding().column_index.GetIndexUnsafe(), bindings.size());
		issues.push_back(std::move(issue));
	}

	void AddTypeMismatch(BoundColumnRefExpression &expr, const LogicalType &expected_type) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::TYPE_MISMATCH;
		issue.path = GetPath(expr);
		issue.construct =
		    LogicalPlanCompilerConstructIdentity::BindingTypeMismatch(expected_type, expr.GetReturnType());
		AddBindingFacts(issue, expr.Binding());
		issue.message = StringUtil::Format(
		    "Failed to bind column reference %s [table=%llu, column=%llu]: inequal types (%s != %s)", expr.GetAlias(),
		    expr.Binding().table_index.index, expr.Binding().column_index.GetIndexUnsafe(),
		    expr.GetReturnType().ToString(), expected_type.ToString());
		issues.push_back(std::move(issue));
	}

	void AddIncompleteBindingType(BoundColumnRefExpression &expr, const LogicalType &expected_type) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
		issue.path = GetPath(expr);
		issue.construct = LogicalPlanCompilerConstructIdentity::Expression(expr.GetExpressionClass());
		issue.facts.emplace_back("invariant", Value("incomplete_binding_type"));
		issue.facts.emplace_back("expected_type_complete", Value::BOOLEAN(expected_type.IsComplete()));
		issue.facts.emplace_back("actual_type_complete", Value::BOOLEAN(expr.GetReturnType().IsComplete()));
		AddBindingFacts(issue, expr.Binding());
		issue.message = StringUtil::Format(
		    "Failed to bind column reference %s [table=%llu, column=%llu]: expected and actual types must be complete",
		    expr.GetAlias(), expr.Binding().table_index.index, expr.Binding().column_index.GetIndexUnsafe());
		issues.push_back(std::move(issue));
	}

	void AddBindingTypeArityMismatch(BoundColumnRefExpression &expr, idx_t binding_count, idx_t type_count) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
		issue.path = GetPath(expr);
		issue.construct = LogicalPlanCompilerConstructIdentity::Expression(expr.GetExpressionClass());
		issue.facts.emplace_back("invariant", Value("binding_type_arity"));
		issue.facts.emplace_back("binding_count", Value::UBIGINT(binding_count));
		issue.facts.emplace_back("type_count", Value::UBIGINT(type_count));
		AddBindingFacts(issue, expr.Binding());
		issue.message = StringUtil::Format(
		    "Failed to bind column reference %s [table=%llu, column=%llu]: inequal num bindings/types (%llu != %llu)",
		    expr.GetAlias(), expr.Binding().table_index.index, expr.Binding().column_index.GetIndexUnsafe(),
		    binding_count, type_count);
		issues.push_back(std::move(issue));
	}

	void AddMalformedExtensionArity(LogicalExtensionOperator &op, const string &identifier, idx_t binding_count,
	                                idx_t type_count) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT;
		issue.path = GetPath(op);
		issue.construct = LogicalPlanCompilerConstructIdentity::Extension(identifier);
		issue.facts.emplace_back("binding_count", Value::UBIGINT(binding_count));
		issue.facts.emplace_back("type_count", Value::UBIGINT(type_count));
		issue.message = StringUtil::Format("Logical extension operator returned %llu bindings and %llu types",
		                                   binding_count, type_count);
		issues.push_back(std::move(issue));
	}

	void AddMalformedExtensionType(LogicalExtensionOperator &op, const string &identifier, idx_t type_index) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT;
		issue.path = GetPath(op);
		issue.construct = LogicalPlanCompilerConstructIdentity::Extension(identifier);
		issue.facts.emplace_back("invalid_type_index", Value::UBIGINT(type_index));
		issue.message =
		    StringUtil::Format("Logical extension operator returned an invalid type at index %llu", type_index);
		issues.push_back(std::move(issue));
	}

	void AddMalformedExtensionBinding(LogicalExtensionOperator &op, const string &identifier, idx_t binding_index,
	                                  const ColumnBinding &binding) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT;
		issue.path = GetPath(op);
		issue.construct = LogicalPlanCompilerConstructIdentity::Extension(identifier);
		issue.facts.emplace_back("invalid_binding_index", Value::UBIGINT(binding_index));
		issue.facts.emplace_back("table_index_valid", Value::BOOLEAN(binding.table_index.IsValid()));
		issue.facts.emplace_back("column_index_valid", Value::BOOLEAN(binding.column_index.IsValid()));
		issue.message =
		    StringUtil::Format("Logical extension operator returned an invalid binding at index %llu", binding_index);
		issues.push_back(std::move(issue));
	}

	void AddDuplicateExtensionBinding(LogicalExtensionOperator &op, const string &identifier, idx_t first_index,
	                                  idx_t duplicate_index, const ColumnBinding &binding, bool types_available,
	                                  bool types_equal) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT;
		issue.path = GetPath(op);
		issue.construct = LogicalPlanCompilerConstructIdentity::Extension(identifier);
		issue.facts.emplace_back("first_binding_index", Value::UBIGINT(first_index));
		issue.facts.emplace_back("duplicate_binding_index", Value::UBIGINT(duplicate_index));
		issue.facts.emplace_back("table_index", Value::UBIGINT(binding.table_index.index));
		issue.facts.emplace_back("column_index", Value::UBIGINT(binding.column_index.GetIndexUnsafe()));
		issue.facts.emplace_back("types_available", Value::BOOLEAN(types_available));
		issue.facts.emplace_back("types_equal", Value::BOOLEAN(types_equal));
		issue.message = StringUtil::Format(
		    "Logical extension operator returned duplicate binding [table=%llu, column=%llu] at indexes %llu and %llu",
		    binding.table_index.index, binding.column_index.GetIndexUnsafe(), first_index, duplicate_index);
		issues.push_back(std::move(issue));
	}

	void AddMissingExtensionIdentifier(LogicalExtensionOperator &op) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
		issue.path = GetPath(op);
		issue.construct = LogicalPlanCompilerConstructIdentity::LogicalOperator(op.type);
		issue.facts.emplace_back("invariant", Value("missing_type_binding_verification_identifier"));
		issue.message = "An extension operator that supports type-binding verification must provide a verification "
		                "identifier";
		issues.push_back(std::move(issue));
	}

	void AddDuplicateTableIndex(LogicalOperator &op, TableIndex table_index) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
		issue.path = GetPath(op);
		issue.construct = GetOperatorConstruct(op);
		issue.facts.emplace_back("invariant", Value("duplicate_table_index"));
		issue.facts.emplace_back("table_index", Value::UBIGINT(table_index.index));
		issue.message = StringUtil::Format("Duplicate table index \"%lld\" found", table_index.index);
		issues.push_back(std::move(issue));
	}

	void AddInvalidTableIndex(LogicalOperator &op, idx_t table_index_ordinal, TableIndex table_index) {
		LogicalPlanCompilerIssue issue;
		issue.code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
		issue.path = GetPath(op);
		issue.construct = GetOperatorConstruct(op);
		issue.facts.emplace_back("invariant", Value("invalid_table_index"));
		issue.facts.emplace_back("table_index_ordinal", Value::UBIGINT(table_index_ordinal));
		issue.facts.emplace_back("table_index", Value::UBIGINT(table_index.index));
		issue.facts.emplace_back("table_index_valid", Value::BOOLEAN(false));
		issue.message = StringUtil::Format("Invalid table index at ownership ordinal %llu", table_index_ordinal);
		issues.push_back(std::move(issue));
	}

	bool HasResolvedInputs(LogicalOperator &op) const {
		return resolved_inputs.find(reference<LogicalOperator>(op)) != resolved_inputs.end();
	}

	bool HasResolvedOutputs(LogicalOperator &op) const {
		return resolved_outputs.find(reference<LogicalOperator>(op)) != resolved_outputs.end();
	}

private:
	static void AddBindingFacts(LogicalPlanCompilerIssue &issue, const ColumnBinding &binding) {
		issue.facts.emplace_back("table_index", Value::UBIGINT(binding.table_index.index));
		issue.facts.emplace_back("column_index", Value::UBIGINT(binding.column_index.GetIndexUnsafe()));
		issue.facts.emplace_back("table_index_valid", Value::BOOLEAN(binding.table_index.IsValid()));
		issue.facts.emplace_back("column_index_valid", Value::BOOLEAN(binding.column_index.IsValid()));
	}

	static LogicalPlanCompilerConstructIdentity GetOperatorConstruct(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			auto &extension_op = op.Cast<LogicalExtensionOperator>();
			auto &identifier = extension_op.GetTypeBindingVerificationIdentifier();
			if (!identifier.empty()) {
				return LogicalPlanCompilerConstructIdentity::Extension(identifier);
			}
		}
		return LogicalPlanCompilerConstructIdentity::LogicalOperator(op.type);
	}

	void IndexExpression(Expression &expr, const LogicalPlanCompilerPath &path) {
		expression_paths.emplace(reference<Expression>(expr), path);
		idx_t child_index = 0;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			auto child_path = path;
			child_path.components.push_back({LogicalPlanCompilerPathComponentType::EXPRESSION_CHILD, child_index++});
			IndexExpression(child, child_path);
		});
	}

	void IndexOperator(LogicalOperator &op, const LogicalPlanCompilerPath &path) {
		operator_paths.emplace(reference<LogicalOperator>(op), path);
		idx_t expression_index = 0;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
			auto expression_path = path;
			expression_path.components.push_back(
			    {LogicalPlanCompilerPathComponentType::OPERATOR_EXPRESSION, expression_index++});
			IndexExpression(**expression, expression_path);
		});
		for (idx_t child_index = 0; child_index < op.children.size(); child_index++) {
			auto child_path = path;
			child_path.components.push_back({LogicalPlanCompilerPathComponentType::OPERATOR_CHILD, child_index});
			IndexOperator(*op.children[child_index], child_path);
		}
	}
};

ColumnBindingResolver::ColumnBindingResolver(bool verify_only) : verify_only(verify_only), verification_state(nullptr) {
}

ColumnBindingResolver::ColumnBindingResolver(ColumnBindingVerificationState &verification_state)
    : verify_only(true), verification_state(verification_state) {
}

void ColumnBindingResolver::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op.Cast<LogicalComparisonJoin>();

		VisitOperator(*comp_join.children[0]);
		auto left_bindings = bindings;
		auto left_types = types;
		for (auto &cond : comp_join.conditions) {
			if (cond.IsComparison()) {
				VisitExpression(&cond.LeftReference());
			}
		}

		for (auto &expr : comp_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}

		VisitOperator(*comp_join.children[1]);
		auto right_bindings = bindings;
		auto right_types = types;
		for (auto &cond : comp_join.conditions) {
			if (cond.IsComparison()) {
				VisitExpression(&cond.RightReference());
			}
		}

		// combine bindings to resolve predicate
		auto combined_bindings = left_bindings;
		combined_bindings.insert(combined_bindings.end(), right_bindings.begin(), right_bindings.end());
		auto combined_types = left_types;
		combined_types.insert(combined_types.end(), right_types.begin(), right_types.end());

		bindings = combined_bindings;
		types = combined_types;
		for (auto &cond : comp_join.conditions) {
			if (!cond.IsComparison()) {
				VisitExpression(&cond.JoinExpressionReference());
			}
		}

		// update to join output bindings
		bindings = op.GetColumnBindings();
		types = op.types;

		return;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		// get bindings from the duplicate-eliminated side
		auto &delim_side = comp_join.delim_flipped ? *comp_join.children[1] : *comp_join.children[0];
		VisitOperator(delim_side);
		auto delim_bindings = bindings;
		auto delim_types = types;
		for (auto &cond : comp_join.conditions) {
			if (cond.IsComparison()) {
				auto &expr = comp_join.delim_flipped ? cond.RightReference() : cond.LeftReference();
				VisitExpression(&expr);
			}
		}
		// visit the duplicate eliminated columns
		for (auto &expr : comp_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}
		// now the other side
		auto &other_side = comp_join.delim_flipped ? *comp_join.children[0] : *comp_join.children[1];
		VisitOperator(other_side);
		auto other_bindings = bindings;
		auto other_types = types;
		for (auto &cond : comp_join.conditions) {
			if (cond.IsComparison()) {
				auto &expr = comp_join.delim_flipped ? cond.LeftReference() : cond.RightReference();
				VisitExpression(&expr);
			}
		}

		// arbitrary expressions are resolved against both join sides in logical left/right order
		auto combined_bindings = comp_join.delim_flipped ? other_bindings : delim_bindings;
		auto &right_bindings = comp_join.delim_flipped ? delim_bindings : other_bindings;
		combined_bindings.insert(combined_bindings.end(), right_bindings.begin(), right_bindings.end());
		auto combined_types = comp_join.delim_flipped ? other_types : delim_types;
		auto &right_types = comp_join.delim_flipped ? delim_types : other_types;
		combined_types.insert(combined_types.end(), right_types.begin(), right_types.end());
		bindings = std::move(combined_bindings);
		types = std::move(combined_types);
		for (auto &cond : comp_join.conditions) {
			if (!cond.IsComparison()) {
				VisitExpression(&cond.JoinExpressionReference());
			}
		}
		// finally update the bindings with the result bindings of the join
		bindings = op.GetColumnBindings();
		types = op.types;
		return;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		// ANY join, this join is different because we evaluate the expression on the bindings of BOTH join sides at
		// once i.e. we set the bindings first to the bindings of the entire join, and then resolve the expressions of
		// this operator
		VisitOperatorChildren(op);
		bindings = op.GetColumnBindings();
		types = op.types;
		auto &any_join = op.Cast<LogicalAnyJoin>();
		if (any_join.join_type == JoinType::SEMI || any_join.join_type == JoinType::ANTI) {
			auto right_bindings = op.children[1]->GetColumnBindings();
			bindings.insert(bindings.end(), right_bindings.begin(), right_bindings.end());
			auto &right_types = op.children[1]->types;
			types.insert(types.end(), right_types.begin(), right_types.end());
		}
		if (any_join.join_type == JoinType::RIGHT_SEMI || any_join.join_type == JoinType::RIGHT_ANTI) {
			throw InternalException("RIGHT SEMI/ANTI any join not supported yet");
		}
		VisitOperatorExpressions(op);

		//	Restore bindings for the caller
		bindings = op.GetColumnBindings();
		types = op.types;
		return;
	}
	case LogicalOperatorType::LOGICAL_CREATE_INDEX: {
		// CREATE INDEX statement, add the columns of the table with table index 0 to the binding set
		// afterwards bind the expressions of the CREATE INDEX statement
		auto &create_index = op.Cast<LogicalCreateIndex>();
		bindings = LogicalOperator::GenerateColumnBindings(TableIndex(0),
		                                                   create_index.table.GetColumns().LogicalColumnCount());
		// TODO: fill types in too (clearing skips type checks)
		types.clear();
		VisitOperatorExpressions(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		//! We first need to update the current set of bindings and then visit operator expressions
		bindings = op.GetColumnBindings();
		types = op.types;
		VisitOperatorExpressions(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		//! We want to execute the normal path, but also add a dummy 'excluded' binding if there is a
		// ON CONFLICT DO UPDATE clause
		auto &insert_op = op.Cast<LogicalInsert>();
		if (insert_op.on_conflict_info.action_type != OnConflictAction::THROW) {
			// Get the bindings from the children
			VisitOperatorChildren(op);
			auto column_count = insert_op.table.GetColumns().PhysicalColumnCount();
			auto dummy_bindings =
			    LogicalOperator::GenerateColumnBindings(insert_op.on_conflict_info.excluded_table_index, column_count);
			// Now insert our dummy bindings at the start of the bindings,
			// so the first 'column_count' indices of the chunk are reserved for our 'excluded' columns
			bindings.insert(bindings.begin(), dummy_bindings.begin(), dummy_bindings.end());
			// TODO: fill types in too (clearing skips type checks)
			types.clear();
			if (insert_op.on_conflict_info.on_conflict_condition) {
				VisitExpression(&insert_op.on_conflict_info.on_conflict_condition);
			}
			if (insert_op.on_conflict_info.do_update_condition) {
				VisitExpression(&insert_op.on_conflict_info.do_update_condition);
			}
			VisitOperatorExpressions(op);
			bindings = op.GetColumnBindings();
			types = op.types;
			return;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR: {
		auto &ext_op = op.Cast<LogicalExtensionOperator>();
		if (ext_op.SupportsTypeBindingVerification()) {
			bindings.clear();
			types.clear();
			VisitOperatorChildren(op);
			VisitOperatorExpressions(op);
			bindings = op.GetColumnBindings();
			types = op.types;
			return;
		}
		// Just to be very sure, we clear before and after resolving extension operator column bindings
		// This skips checks, but makes sure we don't break any extension operators with type verification
		types.clear();
		ext_op.ResolveColumnBindings(*this, bindings);
		types.clear();
		return;
	}
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		VisitOperatorChildren(op);
		bindings = op.GetColumnBindings();

		types.clear();
		VisitOperatorExpressions(op);
		types = op.types;
		return;
	}
	default:
		break;
	}

	// general case
	// first visit the children of this operator
	VisitOperatorChildren(op);
	// now visit the expressions of this operator to resolve any bound column references
	VisitOperatorExpressions(op);
	// finally update the current set of bindings to the current set of column bindings
	bindings = op.GetColumnBindings();
	types = op.types;
}

unique_ptr<Expression> ColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	D_ASSERT(expr.Depth() == 0);
	// check the current set of column bindings to see which index corresponds to the column reference
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (expr.Binding() == bindings[i]) {
			if (!types.empty()) {
				if (bindings.size() != types.size()) {
					if (verification_state) {
						verification_state->AddBindingTypeArityMismatch(expr, bindings.size(), types.size());
						return nullptr;
					}
					throw InternalException(
					    "Failed to bind column reference %s [%d.%d]: inequal num bindings/types (%llu != %llu)",
					    expr.GetAlias(), expr.Binding().table_index.index, expr.Binding().column_index, bindings.size(),
					    types.size());
				}
				if (verification_state && (!types[i].IsComplete() || !expr.GetReturnType().IsComplete())) {
					verification_state->AddIncompleteBindingType(expr, types[i]);
					return nullptr;
				}
				if (expr.GetReturnType() != types[i]) {
					if (verification_state) {
						verification_state->AddTypeMismatch(expr, types[i]);
						return nullptr;
					}
					throw InternalException("Failed to bind column reference %s [%d.%d]: inequal types (%s != %s)",
					                        expr.GetAlias(), expr.Binding().table_index.index,
					                        expr.Binding().column_index, expr.GetReturnType().ToString(),
					                        types[i].ToString());
				}
			}
			if (verify_only) {
				// in verification mode
				return nullptr;
			}
			return make_uniq<BoundReferenceExpression>(expr.GetAlias(), expr.GetReturnType(), i);
		}
	}
	if (verification_state) {
		verification_state->AddInvalidBinding(expr, bindings);
		return nullptr;
	}
	// LCOV_EXCL_START
	// could not bind the column reference, this should never happen and indicates a bug in the code
	// generate an error message
	throw InternalException("Failed to bind column reference %s [%d.%d] (bindings: %s)", expr.GetAlias(),
	                        expr.Binding().table_index.index, expr.Binding().column_index,
	                        LogicalOperator::ColumnBindingsToString(bindings));
	// LCOV_EXCL_STOP
}

void ColumnBindingResolver::Verify(ClientContext &context, LogicalOperator &op) {
	if (!Settings::Get<DebugVerifyColumnBindingsSetting>(context)) {
		return;
	}
	string first_error;
	auto result = VerifyAlwaysInternal(op, first_error);
	if (result.HasError()) {
		throw InternalException("%s", first_error);
	}
}

bool ColumnBindingResolver::ResolveOperatorTypes(LogicalOperator &op,
                                                 ColumnBindingVerificationState &verification_state) {
	op.types.clear();
	bool children_resolved = true;
	for (auto &child : op.children) {
		if (!ResolveOperatorTypes(*child, verification_state)) {
			children_resolved = false;
		}
	}
	if (!children_resolved) {
		return false;
	}
	verification_state.resolved_inputs.insert(reference<LogicalOperator>(op));
	op.ResolveTypes();
	auto bindings = op.GetColumnBindings();
	if (op.type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
		auto &extension_op = op.Cast<LogicalExtensionOperator>();
		if (extension_op.SupportsTypeBindingVerification()) {
			auto &identifier = extension_op.GetTypeBindingVerificationIdentifier();
			if (identifier.empty()) {
				verification_state.AddMissingExtensionIdentifier(extension_op);
				return false;
			}
			bool output_valid = true;
			if (bindings.size() != op.types.size()) {
				verification_state.AddMalformedExtensionArity(extension_op, identifier, bindings.size(),
				                                              op.types.size());
				output_valid = false;
			}
			column_binding_map_t<idx_t> binding_indexes;
			for (idx_t binding_index = 0; binding_index < bindings.size(); binding_index++) {
				auto &binding = bindings[binding_index];
				if (!binding.table_index.IsValid() || !binding.column_index.IsValid()) {
					verification_state.AddMalformedExtensionBinding(extension_op, identifier, binding_index, binding);
					output_valid = false;
					continue;
				}
				auto entry = binding_indexes.find(binding);
				if (entry != binding_indexes.end()) {
					auto first_index = entry->second;
					auto types_available = first_index < op.types.size() && binding_index < op.types.size();
					auto types_equal = types_available && op.types[first_index] == op.types[binding_index];
					verification_state.AddDuplicateExtensionBinding(
					    extension_op, identifier, first_index, binding_index, binding, types_available, types_equal);
					output_valid = false;
				} else {
					binding_indexes.emplace(binding, binding_index);
				}
			}
			for (idx_t type_index = 0; type_index < op.types.size(); type_index++) {
				if (!op.types[type_index].IsComplete()) {
					verification_state.AddMalformedExtensionType(extension_op, identifier, type_index);
					output_valid = false;
				}
			}
			if (!output_valid) {
				return false;
			}
		}
	}
	D_ASSERT(op.types.size() == bindings.size());
	verification_state.resolved_outputs.insert(reference<LogicalOperator>(op));
	return true;
}

void ColumnBindingResolver::VerifyColumnBindings(LogicalOperator &op,
                                                 ColumnBindingVerificationState &verification_state) {
	if (verification_state.HasResolvedOutputs(op) ||
	    (verification_state.HasResolvedInputs(op) && op.type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR &&
	     op.Cast<LogicalExtensionOperator>().SupportsTypeBindingVerification())) {
		ColumnBindingResolver resolver(verification_state);
		resolver.VisitOperator(op);
		return;
	}
	for (auto &child : op.children) {
		VerifyColumnBindings(*child, verification_state);
	}
}

static void VerifyTableIndexes(LogicalOperator &op, ColumnBindingVerificationState &verification_state,
                               unordered_map<TableIndex, LogicalPlanCompilerPath> &seen_indexes) {
	for (auto &child : op.children) {
		VerifyTableIndexes(*child, verification_state, seen_indexes);
	}
	auto table_indexes = op.GetTableIndex();
	for (idx_t table_index_ordinal = 0; table_index_ordinal < table_indexes.size(); table_index_ordinal++) {
		auto index = table_indexes[table_index_ordinal];
		if (!index.IsValid()) {
			verification_state.AddInvalidTableIndex(op, table_index_ordinal, index);
			continue;
		}
		if (seen_indexes.find(index) != seen_indexes.end()) {
			verification_state.AddDuplicateTableIndex(op, index);
		} else {
			seen_indexes.emplace(index, verification_state.GetPath(op));
		}
	}
}

LogicalPlanCompilerResult<LogicalPlanVerificationSuccess> ColumnBindingResolver::VerifyAlways(LogicalOperator &op) {
	return VerifyAlwaysInternal(op, nullptr);
}

LogicalPlanCompilerResult<LogicalPlanVerificationSuccess>
ColumnBindingResolver::VerifyAlwaysInternal(LogicalOperator &op, optional_ptr<string> first_error) {
	ColumnBindingVerificationState verification_state(op);
	ResolveOperatorTypes(op, verification_state);
	VerifyColumnBindings(op, verification_state);

	unordered_map<TableIndex, LogicalPlanCompilerPath> seen_indexes;
	VerifyTableIndexes(op, verification_state, seen_indexes);
	if (!verification_state.issues.empty()) {
		if (first_error) {
			*first_error = verification_state.issues[0].message;
		}
		return LogicalPlanCompilerResult<LogicalPlanVerificationSuccess>::Failure(std::move(verification_state.issues));
	}
	return LogicalPlanCompilerResult<LogicalPlanVerificationSuccess>::Success(LogicalPlanVerificationSuccess());
}

} // namespace duckdb
