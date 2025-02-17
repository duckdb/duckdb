#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/exception.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "helper.hpp"

namespace duckdb {

StarExpression::StarExpression(string relation_name_p)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), relation_name(std::move(relation_name_p)) {
}

string StarExpression::ToString() const {
	string result;
	if (expr) {
		D_ASSERT(columns);
		result += "COLUMNS(" + expr->ToString() + ")";
		return result;
	}
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
			result += entry.ToString();
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
			result += KeywordHelper::WriteOptionallyQuoted(entry.first);
			first_entry = false;
		}
		result += ")";
	}
	if (!rename_list.empty()) {
		result += " RENAME (";
		bool first_entry = true;
		for (auto &entry : rename_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.first.ToString();
			result += " AS ";
			result += KeywordHelper::WriteOptionallyQuoted(entry.second);
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
	if (a.relation_name != b.relation_name || a.exclude_list != b.exclude_list || a.rename_list != b.rename_list) {
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

bool StarExpression::IsStar(const ParsedExpression &a) {
	if (a.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = a.Cast<StarExpression>();
	return star.columns == false;
}

bool StarExpression::IsColumns(const ParsedExpression &a) {
	if (a.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = a.Cast<StarExpression>();
	return star.columns == true;
}

bool StarExpression::IsColumnsUnpacked(const ParsedExpression &a) {
	if (a.GetExpressionType() != ExpressionType::OPERATOR_UNPACK) {
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
	copy->rename_list = rename_list;
	copy->columns = columns;
	copy->expr = expr ? expr->Copy() : nullptr;
	copy->CopyProperties(*this);
	return std::move(copy);
}

StarExpression::StarExpression(const case_insensitive_set_t &exclude_list_p, qualified_column_set_t qualified_set)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), exclude_list(std::move(qualified_set)) {
	for (auto &entry : exclude_list_p) {
		exclude_list.insert(QualifiedColumnName(entry));
	}
}

case_insensitive_set_t StarExpression::SerializedExcludeList() const {
	// we serialize non-qualified elements in a separate list of only column names for backwards compatibility
	case_insensitive_set_t result;
	for (auto &entry : exclude_list) {
		if (!entry.IsQualified()) {
			result.insert(entry.column);
		}
	}
	return result;
}

qualified_column_set_t StarExpression::SerializedQualifiedExcludeList() const {
	// we serialize only qualified elements in the qualified list for backwards compatibility
	qualified_column_set_t result;
	for (auto &entry : exclude_list) {
		if (entry.IsQualified()) {
			result.insert(entry);
		}
	}
	return result;
}

void StarExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "relation_name", relation_name);
	serializer.WriteProperty<case_insensitive_set_t>(201, "exclude_list", SerializedExcludeList());
	serializer.WritePropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(202, "replace_list",
	                                                                                          replace_list);
	serializer.WritePropertyWithDefault<bool>(203, "columns", columns);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(204, "expr", expr);
	serializer.WritePropertyWithDefault<qualified_column_set_t>(
	    206, "qualified_exclude_list", SerializedQualifiedExcludeList(), qualified_column_set_t());
	serializer.WritePropertyWithDefault<qualified_column_map_t<string>>(207, "rename_list", rename_list,
	                                                                    qualified_column_map_t<string>());
}

unique_ptr<ParsedExpression> StarExpression::Deserialize(Deserializer &deserializer) {
	auto relation_name = deserializer.ReadPropertyWithDefault<string>(200, "relation_name");
	auto exclude_list = deserializer.ReadProperty<case_insensitive_set_t>(201, "exclude_list");
	auto replace_list =
	    deserializer.ReadPropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(202, "replace_list");
	auto columns = deserializer.ReadPropertyWithDefault<bool>(203, "columns");
	auto expr = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(204, "expr");
	auto unpacked = deserializer.ReadPropertyWithExplicitDefault<bool>(205, "unpacked", false);
	auto qualified_exclude_list = deserializer.ReadPropertyWithExplicitDefault<qualified_column_set_t>(
	    206, "qualified_exclude_list", qualified_column_set_t());
	auto result = duckdb::unique_ptr<StarExpression>(new StarExpression(exclude_list, qualified_exclude_list));
	result->relation_name = std::move(relation_name);
	result->replace_list = std::move(replace_list);
	result->columns = columns;
	result->expr = std::move(expr);
	deserializer.ReadPropertyWithExplicitDefault<qualified_column_map_t<string>>(
	    207, "rename_list", result->rename_list, qualified_column_map_t<string>());
	if (unpacked) {
		//! This was previously a member of StarExpression, but not anymore
		//! We wrap it into an OPERATOR_UNPACK instead
		vector<unique_ptr<ParsedExpression>> unpack_children;
		unpack_children.push_back(std::move(result));
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK, std::move(unpack_children));
	}
	return std::move(result);
}

} // namespace duckdb
