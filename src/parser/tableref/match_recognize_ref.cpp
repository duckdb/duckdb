#include "duckdb/parser/tableref/match_recognize_ref.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string MatchRecognizeRef::ToString() const {
	string result = "MATCH_RECOGNIZE( todo )";
	return BaseToString(result, column_name_alias);
}

MatchRecognizeRef::MatchRecognizeRef() : TableRef(TableReferenceType::MATCH_RECOGNIZE) {
}

static bool ExpressionsEqual(const vector<unique_ptr<ParsedExpression>> &left,
                             const vector<unique_ptr<ParsedExpression>> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!left[i]->Equals(*right[i])) {
			return false;
		}
	}
	return true;
}

static vector<unique_ptr<ParsedExpression>> CopyExpressions(const vector<unique_ptr<ParsedExpression>> &expressions) {
	vector<unique_ptr<ParsedExpression>> result;
	result.reserve(expressions.size());
	for (auto &expression : expressions) {
		result.push_back(expression->Copy());
	}
	return result;
}

bool MatchRecognizeRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<MatchRecognizeRef>();
	if (!input->Equals(*other.input)) {
		return false;
	}
	if (config->rows_per_match != other.config->rows_per_match || config->after_match != other.config->after_match ||
	    config->define_auto != other.config->define_auto) {
		return false;
	}
	if (!ExpressionsEqual(config->partition_expressions, other.config->partition_expressions) ||
	    !ExpressionsEqual(config->measures_expression_list, other.config->measures_expression_list) ||
	    !ExpressionsEqual(config->defines_expression_list, other.config->defines_expression_list)) {
		return false;
	}
	if (config->order_by_expressions.size() != other.config->order_by_expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < config->order_by_expressions.size(); i++) {
		auto &mine = config->order_by_expressions[i];
		auto &theirs = other.config->order_by_expressions[i];
		if (mine.type != theirs.type || mine.null_order != theirs.null_order ||
		    !mine.expression->Equals(*theirs.expression)) {
			return false;
		}
	}
	if (config->subsets.size() != other.config->subsets.size()) {
		return false;
	}
	for (idx_t i = 0; i < config->subsets.size(); i++) {
		if (config->subsets[i].name != other.config->subsets[i].name ||
		    config->subsets[i].members != other.config->subsets[i].members) {
			return false;
		}
	}
	if (config->pattern.get() != other.config->pattern.get() &&
	    (!config->pattern || !other.config->pattern || !config->pattern->Equals(*other.config->pattern))) {
		return false;
	}
	if (config->after_match_variable.get() != other.config->after_match_variable.get() &&
	    (!config->after_match_variable || !other.config->after_match_variable ||
	     !config->after_match_variable->Equals(*other.config->after_match_variable))) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> MatchRecognizeRef::Copy() {
	auto copied_config = make_uniq<MatchRecognizeConfig>();
	copied_config->partition_expressions = CopyExpressions(config->partition_expressions);
	for (auto &order : config->order_by_expressions) {
		copied_config->order_by_expressions.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	copied_config->measures_expression_list = CopyExpressions(config->measures_expression_list);
	copied_config->defines_expression_list = CopyExpressions(config->defines_expression_list);
	copied_config->rows_per_match = config->rows_per_match;
	copied_config->after_match = config->after_match;
	if (config->after_match_variable) {
		auto variable = config->after_match_variable->Copy();
		copied_config->after_match_variable =
		    unique_ptr<ConstantExpression>(static_cast<ConstantExpression *>(variable.release()));
	}
	if (config->pattern) {
		copied_config->pattern = config->pattern->Copy();
	}
	copied_config->subsets = config->subsets;
	copied_config->define_auto = config->define_auto;

	auto copy = make_uniq<MatchRecognizeRef>(input->Copy(), std::move(copied_config));
	copy->alias = alias;
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);
	return std::move(copy);
}

} // namespace duckdb
