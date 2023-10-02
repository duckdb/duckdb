#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformStarExpression(duckdb_libpgquery::PGAStar &star) {
	auto result = make_uniq<StarExpression>(star.relation ? star.relation : string());
	if (star.except_list) {
		for (auto head = star.except_list->head; head; head = head->next) {
			auto value = PGPointerCast<duckdb_libpgquery::PGValue>(head->data.ptr_value);
			D_ASSERT(value->type == duckdb_libpgquery::T_PGString);
			string exclude_entry = value->val.str;
			if (result->exclude_list.find(exclude_entry) != result->exclude_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", exclude_entry);
			}
			result->exclude_list.insert(std::move(exclude_entry));
		}
	}
	if (star.replace_list) {
		for (auto head = star.replace_list->head; head; head = head->next) {
			auto list = PGPointerCast<duckdb_libpgquery::PGList>(head->data.ptr_value);
			D_ASSERT(list->length == 2);
			auto replace_expression =
			    TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(list->head->data.ptr_value));
			auto value = PGPointerCast<duckdb_libpgquery::PGValue>(list->tail->data.ptr_value);
			D_ASSERT(value->type == duckdb_libpgquery::T_PGString);
			string exclude_entry = value->val.str;
			if (result->replace_list.find(exclude_entry) != result->replace_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in REPLACE list", exclude_entry);
			}
			if (result->exclude_list.find(exclude_entry) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCEPT and REPLACE list", exclude_entry);
			}
			result->replace_list.insert(make_pair(std::move(exclude_entry), std::move(replace_expression)));
		}
	}
	if (star.expr) {
		D_ASSERT(star.columns);
		D_ASSERT(result->relation_name.empty());
		D_ASSERT(result->exclude_list.empty());
		D_ASSERT(result->replace_list.empty());
		result->expr = TransformExpression(star.expr);
		if (result->expr->type == ExpressionType::STAR) {
			auto &child_star = result->expr->Cast<StarExpression>();
			result->exclude_list = std::move(child_star.exclude_list);
			result->replace_list = std::move(child_star.replace_list);
			result->expr.reset();
		} else if (result->expr->type == ExpressionType::LAMBDA) {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<StarExpression>());
			children.push_back(std::move(result->expr));
			auto list_filter = make_uniq<FunctionExpression>("list_filter", std::move(children));
			result->expr = std::move(list_filter);
		}
	}
	result->columns = star.columns;
	result->query_location = star.location;
	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformColumnRef(duckdb_libpgquery::PGColumnRef &root) {
	auto fields = root.fields;
	auto head_node = PGPointerCast<duckdb_libpgquery::PGNode>(fields->head->data.ptr_value);
	switch (head_node->type) {
	case duckdb_libpgquery::T_PGString: {
		if (fields->length < 1) {
			throw InternalException("Unexpected field length");
		}
		vector<string> column_names;
		for (auto node = fields->head; node; node = node->next) {
			column_names.emplace_back(PGPointerCast<duckdb_libpgquery::PGValue>(node->data.ptr_value)->val.str);
		}
		auto colref = make_uniq<ColumnRefExpression>(std::move(column_names));
		colref->query_location = root.location;
		return std::move(colref);
	}
	case duckdb_libpgquery::T_PGAStar: {
		return TransformStarExpression(PGCast<duckdb_libpgquery::PGAStar>(*head_node));
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}

} // namespace duckdb
