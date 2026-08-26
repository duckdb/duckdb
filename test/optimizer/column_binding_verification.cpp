#include "catch.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

class VerificationExtensionOperator : public LogicalExtensionOperator {
public:
	VerificationExtensionOperator(string name_p, bool verify_types_p, vector<ColumnBinding> bindings_p,
	                              vector<LogicalType> resolved_types_p, vector<TableIndex> table_indexes_p = {},
	                              vector<unique_ptr<Expression>> expressions_p = {})
	    : LogicalExtensionOperator(std::move(expressions_p)), name(std::move(name_p)), verify_types(verify_types_p),
	      bindings(std::move(bindings_p)), resolved_types(std::move(resolved_types_p)),
	      table_indexes(std::move(table_indexes_p)) {
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}

	vector<TableIndex> GetTableIndex() const override {
		return table_indexes;
	}

	bool SupportsTypeVerification() const override {
		return verify_types;
	}

	string GetExtensionName() const override {
		return name;
	}

	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &) override {
		throw NotImplementedException("Synthetic verification operator cannot create a physical plan");
	}

protected:
	void ResolveTypes() override {
		types = resolved_types;
	}

private:
	string name;
	bool verify_types;
	vector<ColumnBinding> bindings;
	vector<LogicalType> resolved_types;
	vector<TableIndex> table_indexes;
};

static unique_ptr<VerificationExtensionOperator> TypedLeaf(TableIndex table_index, LogicalType type,
                                                           bool verify_types = true) {
	auto binding = ColumnBinding(table_index, ProjectionIndex(0));
	return make_uniq<VerificationExtensionOperator>("typed_verification_leaf", verify_types,
	                                                vector<ColumnBinding> {binding}, vector<LogicalType> {type},
	                                                vector<TableIndex> {table_index});
}

static unique_ptr<LogicalProjection> ReferenceProjection(TableIndex table_index, ColumnBinding binding,
                                                         LogicalType type, unique_ptr<LogicalOperator> child,
                                                         const string &alias = string()) {
	vector<unique_ptr<Expression>> expressions;
	if (alias.empty()) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(type, binding));
	} else {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(Identifier(alias), type, binding));
	}
	auto result = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	result->children.push_back(std::move(child));
	return result;
}

static const Value &GetFact(const CompilerIssue &issue, const string &name) {
	for (auto &fact : issue.facts) {
		if (fact.first == name) {
			return fact.second;
		}
	}
	throw InternalException("Missing compiler issue fact");
}

TEST_CASE("Compiler verification accepts typed extension operators", "[compiler_verification]") {
	SECTION("typed leaf") {
		auto child_index = TableIndex(10);
		auto plan = ReferenceProjection(TableIndex(11), ColumnBinding(child_index, ProjectionIndex(0)),
		                                LogicalType::INTEGER, TypedLeaf(child_index, LogicalType::INTEGER));
		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.IsSuccess());
	}

	SECTION("typed pass-through") {
		auto child_index = TableIndex(20);
		auto binding = ColumnBinding(child_index, ProjectionIndex(0));
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding));
		auto pass_through = make_uniq<VerificationExtensionOperator>(
		    "typed_verification_passthrough", true, vector<ColumnBinding> {binding},
		    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {}, std::move(expressions));
		pass_through->children.push_back(TypedLeaf(child_index, LogicalType::INTEGER));
		auto plan = ReferenceProjection(TableIndex(21), binding, LogicalType::INTEGER, std::move(pass_through));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.IsSuccess());
	}

	SECTION("built-in plan") {
		auto child_index = TableIndex(30);
		auto child = make_uniq<LogicalDummyScan>(child_index);
		auto plan = ReferenceProjection(TableIndex(31), ColumnBinding(child_index, ProjectionIndex(0)),
		                                LogicalType::INTEGER, std::move(child));
		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.IsSuccess());
	}
}

TEST_CASE("Compiler verification reports exact binding and type paths", "[compiler_verification]") {
	SECTION("invalid extension expression binding") {
		auto child_index = TableIndex(40);
		auto child_binding = ColumnBinding(child_index, ProjectionIndex(0));
		auto invalid_binding = ColumnBinding(TableIndex(400), ProjectionIndex(3));
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, invalid_binding));
		auto pass_through = make_uniq<VerificationExtensionOperator>(
		    "typed_verification_invalid_binding", true, vector<ColumnBinding> {child_binding},
		    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {}, std::move(expressions));
		pass_through->children.push_back(TypedLeaf(child_index, LogicalType::INTEGER));
		auto plan = ReferenceProjection(TableIndex(41), child_binding, LogicalType::INTEGER, std::move(pass_through));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.code == CompilerIssueCode::INVALID_BINDING);
		REQUIRE(issue.path == CompilerPath {CompilerPathRoot::LOGICAL_PLAN,
		                                    {{CompilerPathComponentType::OPERATOR_CHILD, 0},
		                                     {CompilerPathComponentType::OPERATOR_EXPRESSION, 0}}});
		REQUIRE(GetFact(issue, "table_index") == Value::UBIGINT(400));
		REQUIRE(GetFact(issue, "column_index") == Value::UBIGINT(3));
	}

	SECTION("parent reference type mismatch") {
		auto child_index = TableIndex(50);
		auto plan = ReferenceProjection(TableIndex(51), ColumnBinding(child_index, ProjectionIndex(0)),
		                                LogicalType::VARCHAR, TypedLeaf(child_index, LogicalType::INTEGER));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.code == CompilerIssueCode::TYPE_MISMATCH);
		REQUIRE(issue.path ==
		        CompilerPath {CompilerPathRoot::LOGICAL_PLAN, {{CompilerPathComponentType::OPERATOR_EXPRESSION, 0}}});
		REQUIRE(issue.construct->type == CompilerConstructType::BINDING_TYPE_MISMATCH);
		REQUIRE(issue.construct->type_mismatch->expected_type == LogicalType::INTEGER);
		REQUIRE(issue.construct->type_mismatch->actual_type == LogicalType::VARCHAR);
	}

	SECTION("nested expression binding") {
		auto child_index = TableIndex(55);
		auto child_binding = ColumnBinding(child_index, ProjectionIndex(0));
		auto invalid_binding = ColumnBinding(TableIndex(550), ProjectionIndex(0));
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundConjunctionExpression>(
		    ExpressionType::CONJUNCTION_AND, make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, invalid_binding),
		    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true))));
		auto pass_through = make_uniq<VerificationExtensionOperator>(
		    "typed_verification_nested_binding", true, vector<ColumnBinding> {child_binding},
		    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {}, std::move(expressions));
		pass_through->children.push_back(TypedLeaf(child_index, LogicalType::INTEGER));
		auto plan = ReferenceProjection(TableIndex(56), child_binding, LogicalType::INTEGER, std::move(pass_through));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		REQUIRE(result.GetIssues()[0].path == CompilerPath {CompilerPathRoot::LOGICAL_PLAN,
		                                                    {{CompilerPathComponentType::OPERATOR_CHILD, 0},
		                                                     {CompilerPathComponentType::OPERATOR_EXPRESSION, 0},
		                                                     {CompilerPathComponentType::EXPRESSION_CHILD, 0}}});
	}
}

TEST_CASE("Compiler verification rejects malformed extension output and duplicate indexes", "[compiler_verification]") {
	SECTION("output arity") {
		auto child_index = TableIndex(60);
		auto malformed = make_uniq<VerificationExtensionOperator>(
		    "typed_verification_malformed", true,
		    vector<ColumnBinding> {ColumnBinding(child_index, ProjectionIndex(0)),
		                           ColumnBinding(child_index, ProjectionIndex(1))},
		    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {child_index});
		auto plan = ReferenceProjection(TableIndex(61), ColumnBinding(child_index, ProjectionIndex(0)),
		                                LogicalType::INTEGER, std::move(malformed));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.code == CompilerIssueCode::MALFORMED_EXTENSION_RESULT);
		REQUIRE(issue.path ==
		        CompilerPath {CompilerPathRoot::LOGICAL_PLAN, {{CompilerPathComponentType::OPERATOR_CHILD, 0}}});
		REQUIRE(issue.construct->identifier == "typed_verification_malformed");
		REQUIRE(GetFact(issue, "binding_count") == Value::UBIGINT(2));
		REQUIRE(GetFact(issue, "type_count") == Value::UBIGINT(1));
	}

	SECTION("duplicate table index") {
		auto duplicate_index = TableIndex(70);
		auto plan = make_uniq<LogicalCrossProduct>(TypedLeaf(duplicate_index, LogicalType::INTEGER),
		                                           TypedLeaf(duplicate_index, LogicalType::INTEGER));

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		auto &issue = result.GetIssues()[0];
		REQUIRE(issue.code == CompilerIssueCode::INTERNAL_INVARIANT);
		REQUIRE(issue.path ==
		        CompilerPath {CompilerPathRoot::LOGICAL_PLAN, {{CompilerPathComponentType::OPERATOR_CHILD, 1}}});
		REQUIRE(GetFact(issue, "invariant") == Value("duplicate_table_index"));
		REQUIRE(GetFact(issue, "table_index") == Value::UBIGINT(70));
	}

	SECTION("duplicate issues are suppressed") {
		auto duplicate_index = TableIndex(75);
		auto plan = make_uniq<VerificationExtensionOperator>(
		    "typed_verification_repeated_index", true,
		    vector<ColumnBinding> {ColumnBinding(duplicate_index, ProjectionIndex(0))},
		    vector<LogicalType> {LogicalType::INTEGER},
		    vector<TableIndex> {duplicate_index, duplicate_index, duplicate_index});

		auto result = ColumnBindingResolver::VerifyAlways(*plan);
		REQUIRE(result.IsValid());
		REQUIRE(result.GetIssues().size() == 1);
		REQUIRE(result.GetIssues()[0].path == CompilerPath {});
	}
}

TEST_CASE("Compiler verification preserves setting and legacy extension behavior", "[compiler_verification]") {
	DuckDB db;
	Connection connection(db);
	auto child_index = TableIndex(80);
	auto invalid_binding = ColumnBinding(TableIndex(800), ProjectionIndex(0));
	auto invalid_plan = ReferenceProjection(TableIndex(81), invalid_binding, LogicalType::INTEGER,
	                                        TypedLeaf(child_index, LogicalType::INTEGER));

	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_column_bindings=false"));
	invalid_plan->types = {LogicalType::VARCHAR};
	REQUIRE_NOTHROW(ColumnBindingResolver::Verify(*connection.context, *invalid_plan));
	REQUIRE(invalid_plan->types == vector<LogicalType> {LogicalType::VARCHAR});
	REQUIRE(ColumnBindingResolver::VerifyAlways(*invalid_plan).HasError());

	REQUIRE_NO_FAIL(connection.Query("SET debug_verify_column_bindings=true"));
	REQUIRE_THROWS_AS(ColumnBindingResolver::Verify(*connection.context, *invalid_plan), InternalException);

	auto valid_plan = ReferenceProjection(TableIndex(82), ColumnBinding(child_index, ProjectionIndex(0)),
	                                      LogicalType::INTEGER, TypedLeaf(child_index, LogicalType::INTEGER));
	REQUIRE_NOTHROW(ColumnBindingResolver::Verify(*connection.context, *valid_plan));

	auto legacy_index = TableIndex(90);
	auto legacy_plan = ReferenceProjection(TableIndex(91), ColumnBinding(legacy_index, ProjectionIndex(0)),
	                                       LogicalType::VARCHAR, TypedLeaf(legacy_index, LogicalType::INTEGER, false));
	auto legacy_result = ColumnBindingResolver::VerifyAlways(*legacy_plan);
	REQUIRE(legacy_result.IsValid());
	REQUIRE(legacy_result.IsSuccess());
}

static unique_ptr<VerificationExtensionOperator> InvalidPassThrough(TableIndex child_index, TableIndex invalid_index,
                                                                    const string &alias) {
	auto child_binding = ColumnBinding(child_index, ProjectionIndex(0));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundColumnRefExpression>(Identifier(alias), LogicalType::INTEGER,
	                                                          ColumnBinding(invalid_index, ProjectionIndex(0))));
	auto result = make_uniq<VerificationExtensionOperator>(
	    "typed_verification_ordering", true, vector<ColumnBinding> {child_binding},
	    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {}, std::move(expressions));
	result->children.push_back(TypedLeaf(child_index, LogicalType::INTEGER));
	return result;
}

static unique_ptr<LogicalOperator> MultipleIssuePlan(const string &left_alias, const string &right_alias) {
	return make_uniq<LogicalCrossProduct>(InvalidPassThrough(TableIndex(100), TableIndex(1000), left_alias),
	                                      InvalidPassThrough(TableIndex(101), TableIndex(1001), right_alias));
}

TEST_CASE("Compiler verification results have deterministic structural identity", "[compiler_verification]") {
	auto first_plan = MultipleIssuePlan("first_left", "first_right");
	auto second_plan = MultipleIssuePlan("second_left", "second_right");
	auto first = ColumnBindingResolver::VerifyAlways(*first_plan);
	auto second = ColumnBindingResolver::VerifyAlways(*second_plan);

	REQUIRE(first.IsValid());
	REQUIRE(second.IsValid());
	REQUIRE(first.GetIssues().size() == 2);
	REQUIRE(first.GetIssues() == second.GetIssues());
	REQUIRE(first.GetIssues()[0].path == CompilerPath {CompilerPathRoot::LOGICAL_PLAN,
	                                                   {{CompilerPathComponentType::OPERATOR_CHILD, 0},
	                                                    {CompilerPathComponentType::OPERATOR_EXPRESSION, 0}}});
	REQUIRE(first.GetIssues()[1].path == CompilerPath {CompilerPathRoot::LOGICAL_PLAN,
	                                                   {{CompilerPathComponentType::OPERATOR_CHILD, 1},
	                                                    {CompilerPathComponentType::OPERATOR_EXPRESSION, 0}}});
	REQUIRE(first.GetIssues()[0].message != second.GetIssues()[0].message);

	auto message_only_change = first.GetIssues()[0];
	message_only_change.message = "Different explanatory text";
	REQUIRE(message_only_change == first.GetIssues()[0]);
}

TEST_CASE("Compiler result records enforce their structural contract", "[compiler_verification]") {
	CompilerPath full_plan_path {CompilerPathRoot::LOGICAL_PLAN,
	                             {{CompilerPathComponentType::OPERATOR_CHILD, 0},
	                              {CompilerPathComponentType::OPERATOR_EXPRESSION, 1},
	                              {CompilerPathComponentType::EXPRESSION_CHILD, 2}}};
	REQUIRE(full_plan_path.IsValid());
	REQUIRE(CompilerPath {CompilerPathRoot::STANDALONE_EXPRESSION, {{CompilerPathComponentType::EXPRESSION_CHILD, 0}}}
	            .IsValid());
	REQUIRE_FALSE(CompilerPath {
	    CompilerPathRoot::LOGICAL_PLAN,
	    {{CompilerPathComponentType::OPERATOR_EXPRESSION, 0}, {CompilerPathComponentType::OPERATOR_CHILD, 0}}}
	                  .IsValid());

	CompilerIssue malformed_extension;
	malformed_extension.code = CompilerIssueCode::MALFORMED_EXTENSION_RESULT;
	malformed_extension.construct = CompilerConstructIdentity::Extension("synthetic_extension");
	REQUIRE(malformed_extension.IsValid());

	CompilerIssue unsupported_extension;
	unsupported_extension.code = CompilerIssueCode::UNSUPPORTED_EXTENSION;
	unsupported_extension.construct = CompilerConstructIdentity::Extension("synthetic_extension");
	REQUIRE_FALSE(unsupported_extension.IsValid());
	unsupported_extension.path = full_plan_path;
	REQUIRE(unsupported_extension.IsValid());

	CompilerIssue invalid_type_mismatch;
	invalid_type_mismatch.code = CompilerIssueCode::TYPE_MISMATCH;
	invalid_type_mismatch.path = full_plan_path;
	invalid_type_mismatch.construct = CompilerConstructIdentity::Expression(ExpressionClass::BOUND_COLUMN_REF);
	REQUIRE_FALSE(invalid_type_mismatch.IsValid());
	invalid_type_mismatch.construct =
	    CompilerConstructIdentity::BindingTypeMismatch(LogicalType::INTEGER, LogicalType::VARCHAR);
	REQUIRE(invalid_type_mismatch.IsValid());
	invalid_type_mismatch.code = static_cast<CompilerIssueCode>(255);
	REQUIRE_FALSE(invalid_type_mismatch.IsValid());
	invalid_type_mismatch.code = CompilerIssueCode::TYPE_MISMATCH;
	invalid_type_mismatch.phase = static_cast<CompilerPhase>(255);
	REQUIRE_FALSE(invalid_type_mismatch.IsValid());
	invalid_type_mismatch.phase = CompilerPhase::VERIFY;

	REQUIRE_FALSE(CompilerResult<VerificationSuccess>::Failure({}).IsValid());
	REQUIRE(CompilerResult<VerificationSuccess>::Failure({invalid_type_mismatch}).IsValid());
}
