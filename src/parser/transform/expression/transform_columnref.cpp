#include "common/exception.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformColumnRef(ColumnRef *root) {
	List *fields = root->fields;
	switch ((reinterpret_cast<Node *>(fields->head->data.ptr_value))->type) {
	case T_String: {
		if (fields->length < 1 || fields->length > 2) {
			throw ParserException("Unexpected field length");
		}
		string column_name, table_name;
		if (fields->length == 1) {
			column_name = string(reinterpret_cast<postgres::Value *>(fields->head->data.ptr_value)->val.str);
		} else {
			table_name = string(reinterpret_cast<postgres::Value *>(fields->head->data.ptr_value)->val.str);
			column_name = string(reinterpret_cast<postgres::Value *>(fields->head->next->data.ptr_value)->val.str);
		}
		return make_unique<ColumnRefExpression>(column_name, table_name);
	}
	case T_A_Star: {
		return make_unique<StarExpression>();
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}
