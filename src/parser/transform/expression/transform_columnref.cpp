#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformStarExpression(duckdb_libpgquery::PGNode *node, idx_t depth) {
	auto star = (duckdb_libpgquery::PGAStar *)node;
	auto result = make_unique<StarExpression>(star->relation ? star->relation : string());
	if (star->except_list) {
		for (auto head = star->except_list->head; head; head = head->next) {
			auto value = (duckdb_libpgquery::PGValue *)head->data.ptr_value;
			D_ASSERT(value->type == duckdb_libpgquery::T_PGString);
			string exclude_entry = value->val.str;
			if (result->exclude_list.find(exclude_entry) != result->exclude_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", exclude_entry);
			}
			result->exclude_list.insert(move(exclude_entry));
		}
	}
	if (star->replace_list) {
		for (auto head = star->replace_list->head; head; head = head->next) {
			auto list = (duckdb_libpgquery::PGList *)head->data.ptr_value;
			D_ASSERT(list->length == 2);
			auto replace_expression =
			    TransformExpression((duckdb_libpgquery::PGNode *)list->head->data.ptr_value, depth + 1);
			auto value = (char *)list->tail->data.ptr_value;
			string exclude_entry = value;
			if (result->replace_list.find(exclude_entry) != result->replace_list.end()) {
				throw ParserException("Duplicate entry \"%s\" in REPLACE list", exclude_entry);
			}
			if (result->exclude_list.find(exclude_entry) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCEPT and REPLACE list", exclude_entry);
			}
			result->replace_list.insert(make_pair(move(exclude_entry), move(replace_expression)));
		}
	}
	return move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformColumnRef(duckdb_libpgquery::PGColumnRef *root, idx_t depth) {
	auto fields = root->fields;
	auto head_node = (duckdb_libpgquery::PGNode *)fields->head->data.ptr_value;
	switch (head_node->type) {
	case duckdb_libpgquery::T_PGString: {
		if (fields->length < 1) {
			throw InternalException("Unexpected field length");
		}
		string column_name, table_name;
		if (fields->length == 1) {
			column_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(fields->head->data.ptr_value)->val.str);
			auto colref = make_unique<ColumnRefExpression>(column_name, table_name);
			colref->query_location = root->location;
			return move(colref);
		} else if (fields->length == 2) {
			table_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(fields->head->data.ptr_value)->val.str);
			auto col_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(fields->head->next->data.ptr_value);
			switch (col_node->type) {
			case duckdb_libpgquery::T_PGString: {
				column_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(col_node)->val.str);
				auto colref = make_unique<ColumnRefExpression>(column_name, table_name);
				colref->query_location = root->location;
				return move(colref);
			}
			default:
				throw NotImplementedException("ColumnRef not implemented!");
			}
		} else {
			throw NotImplementedException("ColumnRef not implemented!");
		}
	}
	case duckdb_libpgquery::T_PGAStar: {
		return TransformStarExpression(head_node, depth);
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}

} // namespace duckdb
