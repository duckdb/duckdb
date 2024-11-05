#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

QualifiedColumnName TransformQualifiedColumnName(duckdb_libpgquery::PGList &list) {
	QualifiedColumnName result;
	switch (list.length) {
	case 1:
		result.column = const_char_ptr_cast(list.head->data.ptr_value);
		break;
	case 2:
		result.table = const_char_ptr_cast(list.head->data.ptr_value);
		result.column = const_char_ptr_cast(list.head->next->data.ptr_value);
		break;
	case 3:
		result.schema = const_char_ptr_cast(list.head->data.ptr_value);
		result.table = const_char_ptr_cast(list.head->next->data.ptr_value);
		result.column = const_char_ptr_cast(list.head->next->next->data.ptr_value);
		break;
	case 4:
		result.catalog = const_char_ptr_cast(list.head->data.ptr_value);
		result.schema = const_char_ptr_cast(list.head->next->data.ptr_value);
		result.table = const_char_ptr_cast(list.head->next->next->data.ptr_value);
		result.column = const_char_ptr_cast(list.head->next->next->next->data.ptr_value);
		break;
	default:
		throw ParserException("Qualified column name must have between 1 and 4 elements");
	}
	return result;
}

unique_ptr<ParsedExpression> Transformer::TransformStarExpression(duckdb_libpgquery::PGAStar &star) {
	auto result = make_uniq<StarExpression>(star.relation ? star.relation : string());
	if (star.except_list) {
		for (auto head = star.except_list->head; head; head = head->next) {
			auto exclude_column_list = PGPointerCast<duckdb_libpgquery::PGList>(head->data.ptr_value);
			auto exclude_column = TransformQualifiedColumnName(*exclude_column_list);
			// qualified - add to exclude list
			if (result->exclude_list.find(exclude_column) != result->exclude_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", exclude_column.ToString());
			}
			result->exclude_list.insert(std::move(exclude_column));
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
			string replace_entry = value->val.str;
			if (result->replace_list.find(replace_entry) != result->replace_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in REPLACE list", replace_entry);
			}
			if (result->exclude_list.find(QualifiedColumnName(replace_entry)) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and REPLACE list", replace_entry);
			}
			result->replace_list.insert(make_pair(std::move(replace_entry), std::move(replace_expression)));
		}
	}
	if (star.rename_list) {
		for (auto head = star.rename_list->head; head; head = head->next) {
			auto list = PGPointerCast<duckdb_libpgquery::PGList>(head->data.ptr_value);
			D_ASSERT(list->length == 2);
			auto rename_column_list = PGPointerCast<duckdb_libpgquery::PGList>(list->head->data.ptr_value);
			auto rename_column = TransformQualifiedColumnName(*rename_column_list);
			string new_name = char_ptr_cast(list->tail->data.ptr_value);
			if (result->rename_list.find(rename_column) != result->rename_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", rename_column.ToString());
			}
			if (result->exclude_list.find(rename_column) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and RENAME list",
				                      rename_column.ToString());
			}
			if (result->replace_list.find(rename_column.column) != result->replace_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both REPLACE and RENAME list",
				                      rename_column.ToString());
			}
			result->rename_list.insert(make_pair(std::move(rename_column), std::move(new_name)));
		}
	}
	if (star.expr) {
		D_ASSERT(star.columns);
		D_ASSERT(result->relation_name.empty());
		D_ASSERT(result->exclude_list.empty());
		D_ASSERT(result->replace_list.empty());
		result->expr = TransformExpression(star.expr);
		if (StarExpression::IsStar(*result->expr)) {
			auto &child_star = result->expr->Cast<StarExpression>();
			result->relation_name = child_star.relation_name;
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
	result->unpacked = star.unpacked;
	SetQueryLocation(*result, star.location);
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
		SetQueryLocation(*colref, root.location);
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
