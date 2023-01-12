#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

BoundCaseExpression::BoundCaseExpression(LogicalType type)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, std::move(type)) {
}

BoundCaseExpression::BoundCaseExpression(unique_ptr<Expression> when_expr, unique_ptr<Expression> then_expr,
                                         unique_ptr<Expression> else_expr_p)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, then_expr->return_type),
      else_expr(std::move(else_expr_p)) {
	BoundCaseCheck check;
	check.when_expr = std::move(when_expr);
	check.then_expr = std::move(then_expr);
	case_checks.push_back(std::move(check));
}

string BoundCaseExpression::ToString() const {
	return CaseExpression::ToString<BoundCaseExpression, Expression>(*this);
}

bool BoundCaseExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = (BoundCaseExpression &)*other_p;
	if (case_checks.size() != other.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < case_checks.size(); i++) {
		if (!Expression::Equals(case_checks[i].when_expr.get(), other.case_checks[i].when_expr.get())) {
			return false;
		}
		if (!Expression::Equals(case_checks[i].then_expr.get(), other.case_checks[i].then_expr.get())) {
			return false;
		}
	}
	if (!Expression::Equals(else_expr.get(), other.else_expr.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCaseExpression::Copy() {
	auto new_case = make_unique<BoundCaseExpression>(return_type);
	for (auto &check : case_checks) {
		BoundCaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		new_case->case_checks.push_back(std::move(new_check));
	}
	new_case->else_expr = else_expr->Copy();

	new_case->CopyProperties(*this);
	return std::move(new_case);
}

void BoundCaseCheck::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializable(*when_expr);
	writer.WriteSerializable(*then_expr);
	writer.Finalize();
}

BoundCaseCheck BoundCaseCheck::Deserialize(Deserializer &source, PlanDeserializationState &state) {
	FieldReader reader(source);
	auto when_expr = reader.ReadRequiredSerializable<Expression>(state);
	auto then_expr = reader.ReadRequiredSerializable<Expression>(state);
	reader.Finalize();
	BoundCaseCheck result;
	result.when_expr = std::move(when_expr);
	result.then_expr = std::move(then_expr);
	return result;
}

void BoundCaseExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(return_type);
	writer.WriteRegularSerializableList(case_checks);
	writer.WriteSerializable(*else_expr);
}

unique_ptr<Expression> BoundCaseExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto case_checks = reader.ReadRequiredSerializableList<BoundCaseCheck, BoundCaseCheck>(state.gstate);
	auto else_expr = reader.ReadRequiredSerializable<Expression>(state.gstate);

	auto result = make_unique<BoundCaseExpression>(return_type);
	result->else_expr = std::move(else_expr);
	result->case_checks = std::move(case_checks);
	return std::move(result);
}

} // namespace duckdb
