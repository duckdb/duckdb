#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformColumnRef(PGColumnRef *root) {
	auto fields = root->fields;
	switch ((reinterpret_cast<PGNode *>(fields->head->data.ptr_value))->type) {
	case T_PGString: {
		if (fields->length < 1 || fields->length > 2) {
			throw ParserException("Unexpected field length");
		}
		string column_name, table_name;
		if (fields->length == 1) {
			column_name = string(reinterpret_cast<PGValue *>(fields->head->data.ptr_value)->val.str);
		} else {
			table_name = string(reinterpret_cast<PGValue *>(fields->head->data.ptr_value)->val.str);
			column_name = string(reinterpret_cast<PGValue *>(fields->head->next->data.ptr_value)->val.str);
		}
		return make_unique<ColumnRefExpression>(column_name, table_name);
	}
	case T_PGAStar: {
		return make_unique<StarExpression>();
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}
