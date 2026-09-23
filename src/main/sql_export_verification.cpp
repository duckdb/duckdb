#include "duckdb/main/sql_export_verification.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"

namespace duckdb {

static constexpr const char *SQL_EXPORT_STATE = "sql_export_verification";

shared_ptr<SQLExportVerificationState> SQLExportVerificationState::Get(ClientContext &context) {
	return context.registered_state->Get<SQLExportVerificationState>(SQL_EXPORT_STATE);
}

shared_ptr<SQLExportVerificationState> SQLExportVerificationState::GetOrCreate(ClientContext &context) {
	return context.registered_state->GetOrCreate<SQLExportVerificationState>(SQL_EXPORT_STATE);
}

void SQLExportVerificationState::Remove(ClientContext &context) {
	context.registered_state->Remove(SQL_EXPORT_STATE);
}

vector<SQLExportVerificationRecord> SQLExportVerificationState::TakeRecords() {
	vector<SQLExportVerificationRecord> result;
	result.swap(records);
	if (!active_record) {
		statement_count = 0;
	}
	return result;
}

void SQLExportVerificationState::QueryBegin(ClientContext &context) {
	D_ASSERT(!active_record);
	auto statement_index = statement_count++;
	auto mode = Settings::Get<DebugVerifySqlExportSetting>(context);
	if (mode == DebugSQLExportVerification::OFF) {
		return;
	}
	active_record.emplace();
	active_record->mode = mode;
	active_record->statement_index = statement_index;
}

void SQLExportVerificationState::Publish(SQLExportVerificationRecord record) {
	if (active_record) {
		record.statement_index = active_record->statement_index;
		*active_record = std::move(record);
	}
}

void SQLExportVerificationState::QueryEnd(ClientContext &, optional_ptr<ErrorData> error) {
	if (!active_record) {
		return;
	}
	auto &record = *active_record;
	record.query_error = error && error->HasError();
	if (record.route != SQLExportExecutionRoute::NONE) {
		record.execution = record.query_error ? SQLExportExecutionStatus::ERRORED : SQLExportExecutionStatus::SUCCEEDED;
	}
	records.push_back(std::move(record));
	active_record.reset();
}

SQLExportVerification::SQLExportVerification(ClientContext &context_p, DebugSQLExportVerification mode)
    : context(context_p), observer(SQLExportVerificationState::Get(context_p)) {
	record.mode = mode;
}

SQLExportVerification::~SQLExportVerification() = default;

bool SQLExportVerification::HasVerifierException() const {
	return record.strict_failure || verifier_exception;
}

string SQLExportVerification::ErrorQuery() const {
	if (record.route == SQLExportExecutionRoute::ORIGINAL_FALLBACK ||
	    record.route == SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE) {
		return string();
	}
	return record.generated_sql;
}

void SQLExportVerification::BeginPlanningAttempt() {
	auto mode = record.mode;
	auto export_count = record.export_count;
	record = SQLExportVerificationRecord();
	record.mode = mode;
	record.export_count = export_count;
	generated_planner.reset();
	input_profile_path.reset();
	verifier_exception = false;
}

void SQLExportVerification::Publish(bool planning_succeeded) {
	if (!planning_succeeded) {
		record.route = SQLExportExecutionRoute::NONE;
		record.propagated_error = !record.strict_failure;
		record.strict_failure |=
		    record.mode == DebugSQLExportVerification::VERIFY_STRICT && record.eligible && verifier_exception;
	}
	if (observer) {
		if (!observer->retain_failure_sql) {
			record.generated_sql.clear();
		}
		observer->Publish(std::move(record));
	}
}

void SQLExportVerification::Failure(SQLExportOutcome outcome, const string &code) {
	record.outcome = outcome;
	record.code = code;
	if (record.mode == DebugSQLExportVerification::VERIFY_STRICT) {
		record.strict_failure = true;
		record.route = SQLExportExecutionRoute::NONE;
		throw InvalidInputException("SQL export verification failed: %s (%s)", code, record.phase);
	}
	record.route = SQLExportExecutionRoute::ORIGINAL_FALLBACK;
}

static void CollectSQLExportInventory(const LogicalOperator &root, vector<SQLExportInventoryEntry> &entries,
                                      optional<LogicalPlanVerificationPath> &input_profile_path) {
	entries.clear();
	input_profile_path.reset();
	std::function<void(const Expression &, const LogicalPlanVerificationPath &)> visit_expression;
	visit_expression = [&](const Expression &expression, const LogicalPlanVerificationPath &path) {
		entries.push_back({"expression", EnumUtil::ToString(expression.GetExpressionClass()), path});
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY ||
		    (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		     expression.Cast<BoundColumnRefExpression>().Depth() != 0)) {
			if (!input_profile_path) {
				input_profile_path = path;
			}
		}
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &function = expression.Cast<BoundFunctionExpression>().Function();
			entries.push_back({"function", function.GetName().GetIdentifierName(), path});
		} else if (expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE) {
			auto &function = expression.Cast<BoundAggregateExpression>().Function();
			entries.push_back({"function", function.GetName().GetIdentifierName(), path});
		} else if (expression.GetExpressionClass() == ExpressionClass::BOUND_WINDOW) {
			auto &window = expression.Cast<BoundWindowExpression>();
			if (window.AggregateFunction()) {
				entries.push_back({"function", window.AggregateFunction()->GetName().GetIdentifierName(), path});
			}
			if (window.WindowFunction()) {
				entries.push_back({"function", window.WindowFunction()->GetName().GetIdentifierName(), path});
			}
		}
		idx_t ordinal = 0;
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_LAMBDA) {
			auto &lambda = expression.Cast<BoundLambdaExpression>();
			visit_expression(*lambda.LambdaExpr(), SQLExportHelpers::ChildPath(path, ordinal++));
			for (auto &capture : lambda.Captures()) {
				visit_expression(*capture, SQLExportHelpers::ChildPath(path, ordinal++));
			}
			return;
		}
		ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
			visit_expression(child, SQLExportHelpers::ChildPath(path, ordinal++));
		});
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &function = expression.Cast<BoundFunctionExpression>();
			if (function.Function().GetBindCallback() == TableFilterFunctions::Bind && function.BindInfo()) {
				optional_ptr<const Expression> child;
				if (function.Function().GetName() == OptionalFilterScalarFun::NAME) {
					child = function.BindInfo()->Cast<OptionalFilterFunctionData>().child_filter_expr.get();
				} else if (function.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME) {
					child = function.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>().child_filter_expr.get();
				}
				if (child) {
					visit_expression(*child, SQLExportHelpers::ChildPath(path, ordinal++));
				}
			}
		}
	};
	std::function<void(const LogicalOperator &, const LogicalPlanVerificationPath &)> visit_operator;
	visit_operator = [&](const LogicalOperator &op, const LogicalPlanVerificationPath &path) {
		entries.push_back({"operator", LogicalOperatorToString(op.type), path});
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && !input_profile_path) {
			input_profile_path = path;
		}
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			entries.push_back({"source", op.Cast<LogicalGet>().function.GetName().GetIdentifierName(), path});
		}
		idx_t ordinal = 0;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
			visit_expression(**expression,
			                 SQLExportHelpers::ChildPath(
			                     path, ordinal++, LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION));
		});
		auto visit_owned_expression = [&](const Expression &expression) {
			visit_expression(
			    expression, SQLExportHelpers::ChildPath(path, ordinal++,
			                                            LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION));
		};
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &filters = op.Cast<LogicalGet>().table_filters;
			for (auto &entry : filters) {
				visit_owned_expression(
				    *ExpressionFilter::GetExpressionFilter(entry.Filter(), "SQL export inventory").expr);
			}
			for (auto &filter : filters.GetMultiColumnFilters()) {
				visit_owned_expression(*ExpressionFilter::GetExpressionFilter(*filter, "SQL export inventory").expr);
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_PIVOT) {
			for (auto &aggregate : op.Cast<LogicalPivot>().bound_pivot.aggregates) {
				visit_owned_expression(*aggregate);
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		           op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		           op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			if (join.filter_pushdown) {
				for (auto &aggregate : join.filter_pushdown->min_max_aggregates) {
					visit_owned_expression(*aggregate);
				}
			}
		}
		for (idx_t i = 0; i < op.children.size(); i++) {
			visit_operator(*op.children[i], SQLExportHelpers::ChildPath(
			                                    path, i, LogicalPlanVerificationPathComponentType::OPERATOR_CHILD));
		}
	};
	visit_operator(root, {});
}

static SQLExportOutcome ExportOutcome(LogicalPlanVerificationIssueCode code) {
	switch (code) {
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION:
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION:
		return SQLExportOutcome::UNSUPPORTED_EXPRESSION;
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR:
		return SQLExportOutcome::UNSUPPORTED_OPERATOR;
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE:
		return SQLExportOutcome::UNSUPPORTED_SOURCE;
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION:
		return SQLExportOutcome::UNSUPPORTED_EXTENSION;
	case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		return SQLExportOutcome::UNSUPPORTED_EXPORT_FEATURE;
	default:
		return SQLExportOutcome::EXPORT_ERROR;
	}
}

static bool CompatibleProperties(const StatementProperties &original, const StatementProperties &generated) {
	for (auto &entry : generated.modified_databases) {
		auto allowed = original.modified_databases.find(entry.first);
		if (allowed == original.modified_databases.end() || allowed->second.identity != entry.second.identity ||
		    !allowed->second.modifications.Contains(entry.second.modifications)) {
			return false;
		}
	}
	const bool has_bound_parameters = generated.bound_all_parameters && generated.parameter_count == 0;
	const bool has_matching_result =
	    generated.return_type == original.return_type && generated.result_eagerness == original.result_eagerness;
	const bool has_matching_transaction = generated.requires_valid_transaction == original.requires_valid_transaction;
	return has_bound_parameters && has_matching_result && has_matching_transaction;
}

static SQLExportComparability ClassifyComparability(LogicalOperator &op) {
	auto repeatability = ClassifyLogicalOperatorRepeatability(op);
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    repeatability == LogicalOperatorRepeatability::REPEATABLE) {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			const bool can_throw = condition.IsComparison()
			                           ? condition.GetLHS().CanThrow() || condition.GetRHS().CanThrow()
			                           : condition.GetJoinExpression().CanThrow();
			if (can_throw) {
				repeatability = LogicalOperatorRepeatability::UNKNOWN;
			}
		}
	}
	for (auto &child : op.children) {
		auto child_result = ClassifyComparability(*child);
		if (child_result == SQLExportComparability::NON_REPEATABLE) {
			return child_result;
		}
		if (child_result == SQLExportComparability::UNKNOWN &&
		    repeatability == LogicalOperatorRepeatability::REPEATABLE) {
			repeatability = LogicalOperatorRepeatability::UNKNOWN;
		}
	}
	switch (repeatability) {
	case LogicalOperatorRepeatability::REPEATABLE:
		return SQLExportComparability::COMPARABLE;
	case LogicalOperatorRepeatability::NON_REPEATABLE:
		return SQLExportComparability::NON_REPEATABLE;
	case LogicalOperatorRepeatability::UNKNOWN:
		return SQLExportComparability::UNKNOWN;
	}
	throw InternalException("Unknown logical operator repeatability");
}

static bool DependenciesAreCurrent(ClientContext &context, const StatementProperties &properties) {
	for (auto &entry : properties.read_databases) {
		if (!CheckCatalogIdentity(context, entry.first, entry.second)) {
			return false;
		}
	}
	for (auto &entry : properties.modified_databases) {
		if (!CheckCatalogIdentity(context, entry.first, entry.second.identity)) {
			return false;
		}
	}
	return true;
}

static bool CompatibleTypes(const vector<LogicalType> &left, const vector<LogicalType> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!left[i].EqualsIncludingCollation(right[i])) {
			return false;
		}
	}
	return true;
}

void SQLExportVerification::Verify(Planner &planner, StatementType type, bool has_parameters) {
	record.route = SQLExportExecutionRoute::ORIGINAL_NOT_APPLICABLE;
	record.code = "STATEMENT_TYPE";
	record.phase = "ELIGIBILITY";
	if (type != StatementType::SELECT_STATEMENT) {
		return;
	}
	if (has_parameters || planner.properties.parameter_count || !planner.properties.bound_all_parameters) {
		record.code = "PARAMETERS";
		return;
	}
	if (planner.properties.return_type != StatementReturnType::QUERY_RESULT) {
		record.code = "STATEMENT_PROPERTIES";
		return;
	}
	record.eligible = true;
	record.comparability = ClassifyComparability(*planner.plan);
	record.route = SQLExportExecutionRoute::NONE;
	record.phase = "INVENTORY";
	record.code = "INVENTORY_EXCEPTION";
	record.outcome = SQLExportOutcome::EXPORT_ERROR;
	try {
		CollectSQLExportInventory(*planner.plan, record.inventory, input_profile_path);
		if (record.export_count) {
			record.phase = "ELIGIBILITY";
			Failure(SQLExportOutcome::UNSUPPORTED_EXPORT_FEATURE, "PLANNING_RETRY_AFTER_EXPORT");
			return;
		}
		RoundTrip(planner);
	} catch (...) {
		verifier_exception = true;
		throw;
	}
}

void SQLExportVerification::RoundTrip(Planner &planner) {
	record.phase = "PLAN_EXPORT";
	record.outcome = SQLExportOutcome::EXPORT_ERROR;
	record.code = "EXPORT_EXCEPTION";
	record.export_count++;
	LogicalPlanSQLExportOptions options;
	options.output_names = planner.names;
	auto exported = LogicalPlanSQLExporter::Export(context, *planner.plan, options);
	if (input_profile_path) {
		record.path = input_profile_path;
		Failure(SQLExportOutcome::UNSUPPORTED_INPUT_PROFILE, "UNDECORRELATED_INPUT");
		return;
	}
	if (exported.HasError()) {
		record.issues = exported.GetIssues();
		auto &issue = record.issues.front();
		record.path = issue.path;
		record.phase = EnumUtil::ToString(issue.phase);
		Failure(ExportOutcome(issue.code), EnumUtil::ToString(issue.code));
		return;
	}
	record.phase = "SERIALIZE";
	record.outcome = SQLExportOutcome::SERIALIZE_ERROR;
	record.code = "SERIALIZE_EXCEPTION";
	auto &relation = exported.GetValue();
	record.generated_sql = relation.query->ToString();
	record.generated = true;
	record.phase = "REPARSE";
	record.outcome = SQLExportOutcome::REPARSE_ERROR;
	record.code = "REPARSE_EXCEPTION";
	Parser parser(context.GetParserOptions());
	try {
		parser.ParseQuery(record.generated_sql);
	} catch (const std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::PARSER ||
		    (ExtensionCallbackManager::Get(context).HasParserExtensions() ||
		     !ExtensionCallbackManager::Get(context).GrammarExtensions().empty())) {
			throw;
		}
		Failure(SQLExportOutcome::REPARSE_ERROR, "GENERATED_PARSE_ERROR");
		return;
	}
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT ||
	    !parser.statements[0]->named_param_map.empty()) {
		Failure(SQLExportOutcome::REPARSE_ERROR, "GENERATED_STATEMENT_COUNT_OR_TYPE");
		return;
	}
	record.phase = "REBIND";
	record.outcome = SQLExportOutcome::REBIND_ERROR;
	record.code = "REBIND_EXCEPTION";
	generated_planner = make_uniq<Planner>(context);
	// Binding hooks use ordinary exceptions too; thrown failures retain their original propagation.
	{
		auto timer = QueryProfiler::Get(context).StartTimer<MetricPlannerTotalTime>();
		generated_planner->CreatePlan(std::move(parser.statements[0]));
	}
	if (!generated_planner->properties.bound_all_parameters) {
		Failure(SQLExportOutcome::REBIND_ERROR, "GENERATED_UNBOUND_PARAMETERS");
		return;
	}
	D_ASSERT(generated_planner->plan);
	if (!CompatibleProperties(planner.properties, generated_planner->properties)) {
		record.phase = "SCHEMA";
		Failure(SQLExportOutcome::OUTPUT_SCHEMA_MISMATCH, "GENERATED_STATEMENT_PROPERTIES");
		return;
	}
	record.phase = "REOPTIMIZE";
	record.outcome = SQLExportOutcome::REOPTIMIZE_ERROR;
	record.code = "REOPTIMIZE_EXCEPTION";
	generated_planner->Optimize();
	record.phase = "SCHEMA";
	generated_planner->plan->ResolveOperatorTypes();
	// Optimizer hooks can register dependencies on the binder after CreatePlan copied its properties.
	auto &final_properties = generated_planner->binder->GetStatementProperties();
	const bool has_matching_types = CompatibleTypes(generated_planner->plan->types, generated_planner->types) &&
	                                CompatibleTypes(planner.types, generated_planner->types);
	const bool has_matching_columns =
	    generated_planner->names == planner.names && relation.fields.size() == planner.types.size();
	if (!has_matching_types || !has_matching_columns || !CompatibleProperties(planner.properties, final_properties)) {
		Failure(SQLExportOutcome::OUTPUT_SCHEMA_MISMATCH, "GENERATED_SCHEMA_OR_PROPERTIES");
		return;
	}
	if (!DependenciesAreCurrent(context, final_properties)) {
		record.phase = "DEPENDENCIES";
		Failure(SQLExportOutcome::REBIND_ERROR, "GENERATED_DEPENDENCY_INVALIDATED");
		return;
	}
	for (idx_t i = 0; i < planner.types.size(); i++) {
		if (!relation.fields[i].type.EqualsIncludingCollation(planner.types[i])) {
			Failure(SQLExportOutcome::OUTPUT_SCHEMA_MISMATCH, "EXPORTED_FIELD_TYPE");
			return;
		}
	}
	record.outcome = SQLExportOutcome::STRUCTURALLY_VALIDATED;
	record.code = "STRUCTURALLY_VALIDATED";
	record.phase = "COMPLETE";
	// Install the generated plan and the metadata that governs its validity as one unit.
	planner.plan = std::move(generated_planner->plan);
	planner.names = generated_planner->names;
	planner.types = generated_planner->types;
	planner.properties = final_properties;
	planner.binder = generated_planner->binder;
	record.route = SQLExportExecutionRoute::GENERATED;
}

} // namespace duckdb
