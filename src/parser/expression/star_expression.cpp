#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/common/exception.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

StarExpression::StarExpression(string relation_name_p)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), relation_name(std::move(relation_name_p)) {
}

string StarExpression::ToString() const {
	if (expr) {
		D_ASSERT(columns);
		return "COLUMNS(" + expr->ToString() + ")";
	}
	string result;
	if (columns) {
		result += "COLUMNS(";
	}
	result += relation_name.empty() ? "*" : relation_name + ".*";
	if (!exclude_list.empty()) {
		result += " EXCLUDE (";
		bool first_entry = true;
		for (auto &entry : exclude_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry;
			first_entry = false;
		}
		result += ")";
	}
	if (!replace_list.empty()) {
		result += " REPLACE (";
		bool first_entry = true;
		for (auto &entry : replace_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.second->ToString();
			result += " AS ";
			result += entry.first;
			first_entry = false;
		}
		result += ")";
	}
	if (columns) {
		result += ")";
	}
	return result;
}

bool StarExpression::Equal(const StarExpression &a, const StarExpression &b) {
	if (a.relation_name != b.relation_name || a.exclude_list != b.exclude_list) {
		return false;
	}
	if (a.columns != b.columns) {
		return false;
	}
	if (a.replace_list.size() != b.replace_list.size()) {
		return false;
	}
	for (auto &entry : a.replace_list) {
		auto other_entry = b.replace_list.find(entry.first);
		if (other_entry == b.replace_list.end()) {
			return false;
		}
		if (!entry.second->Equals(*other_entry->second)) {
			return false;
		}
	}
	if (!ParsedExpression::Equals(a.expr, b.expr)) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> StarExpression::Copy() const {
	auto copy = make_uniq<StarExpression>(relation_name);
	copy->exclude_list = exclude_list;
	for (auto &entry : replace_list) {
		copy->replace_list[entry.first] = entry.second->Copy();
	}
	copy->columns = columns;
	copy->expr = expr ? expr->Copy() : nullptr;
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
