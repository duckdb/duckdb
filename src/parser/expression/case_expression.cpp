#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

SimpleCaseCheck SimpleCaseCheck::Copy() const {
	SimpleCaseCheck result;
	for (auto &test : when) {
		result.when.push_back({test.comparison_type, test.expression->Copy()});
	}
	result.then_expr = then_expr->Copy();
	return result;
}

bool SimpleCaseCheck::Equals(const SimpleCaseCheck &other) const {
	if (when.size() != other.when.size() || !then_expr->Equals(*other.then_expr)) {
		return false;
	}
	for (idx_t i = 0; i < when.size(); i++) {
		if (when[i].comparison_type != other.when[i].comparison_type ||
		    !when[i].expression->Equals(*other.when[i].expression)) {
			return false;
		}
	}
	return true;
}

namespace {

// Nested expressions use the same name-rendering convention as the enclosing SELECT item.
class ColumnNameRenderingScope {
public:
	ColumnNameRenderingScope() : previous(ActiveFlag()) {
		ActiveFlag() = true;
	}
	~ColumnNameRenderingScope() {
		ActiveFlag() = previous;
	}

	static bool IsActive() {
		return ActiveFlag();
	}

private:
	static bool &ActiveFlag() {
		static thread_local bool active = false;
		return active;
	}

	bool previous;
};

string FormatAlternative(ExpressionType type, const ParsedExpression &rhs) {
	auto op = type == ExpressionType::COMPARE_EQUAL ? string() : ExpressionTypeToOperator(type) + " ";
	return op + "(" + rhs.ToString() + ")";
}

} // namespace

Identifier ParsedExpression::GetName() const {
	ColumnNameRenderingScope scope;
	return BaseExpression::GetName();
}

string CaseExpression::ToString() const {
	string result;
	const bool rendering_column_name = ColumnNameRenderingScope::IsActive();
	if (single_comparison && !rendering_column_name) {
		auto &check = case_checks[0];
		auto &test = check.when_expr->Cast<ComparisonExpression>();
		result =
		    "CASE (" + test.Left().ToString() + ") WHEN " + FormatAlternative(test.GetExpressionType(), test.Right());
		result += " THEN (" + check.then_expr->ToString() + ")";
	} else if (case_operand && rendering_column_name) {
		result = "CASE ";
		for (auto &arm : simple_checks) {
			auto condition =
			    StringUtil::Join(arm.when, arm.when.size(), " OR ", [&](const ComparisonExpressionTail &test) {
				    return ComparisonExpression::ToString(test.comparison_type, *case_operand, *test.expression);
			    });
			if (arm.when.size() > 1) {
				condition = "(" + condition + ")";
			}
			result += " WHEN (" + condition + ") THEN (" + arm.then_expr->ToString() + ")";
		}
	} else if (case_operand) {
		result = "CASE (" + case_operand->ToString() + ")";
		for (auto &check : simple_checks) {
			result += " WHEN " +
			          StringUtil::Join(check.when, check.when.size(), ", ", [](const ComparisonExpressionTail &test) {
				          return FormatAlternative(test.comparison_type, *test.expression);
			          });
			result += " THEN (" + check.then_expr->ToString() + ")";
		}
	} else {
		return ToString<CaseExpression, ParsedExpression>(*this);
	}
	return result + " ELSE " + else_expr->ToString() + " END";
}

void CaseExpression::ConvertToSearched() {
	for (auto &arm : simple_checks) {
		vector<unique_ptr<ParsedExpression>> conditions;
		for (auto &alternative : arm.when) {
			conditions.push_back(make_uniq<ComparisonExpression>(alternative.comparison_type, case_operand->Copy(),
			                                                     std::move(alternative.expression)));
		}
		CaseCheck check;
		if (conditions.size() == 1) {
			check.when_expr = std::move(conditions[0]);
		} else {
			check.when_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(conditions));
		}
		check.then_expr = std::move(arm.then_expr);
		case_checks.push_back(std::move(check));
	}
	simple_checks.clear();
	case_operand.reset();
	single_comparison = false;
}

void CaseExpression::LegacySerialize(Serializer &serializer) const {
	auto copy = Copy();
	copy->Cast<CaseExpression>().ConvertToSearched();
	copy->Serialize(serializer);
}

void CaseExpression::VerifyShape() const {
	if (!case_operand) {
		if (case_checks.empty() || !simple_checks.empty() ||
		    (single_comparison && (case_checks.size() != 1 ||
		                           case_checks[0].when_expr->GetExpressionClass() != ExpressionClass::COMPARISON))) {
			throw SerializationException("Invalid CASE checks");
		}
		return;
	}
	if (single_comparison || !case_checks.empty() || simple_checks.empty() ||
	    (simple_checks.size() == 1 && simple_checks[0].when.size() == 1)) {
		throw SerializationException("Invalid simple CASE checks");
	}
	for (auto &check : simple_checks) {
		if (check.when.empty()) {
			throw SerializationException("Missing simple CASE comparison");
		}
		for (auto &test : check.when) {
			if (test.comparison_type < ExpressionType::COMPARE_EQUAL ||
			    test.comparison_type > ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				throw SerializationException("Invalid simple CASE comparison operator");
			}
		}
	}
}

} // namespace duckdb
