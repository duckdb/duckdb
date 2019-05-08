#include "parser/constraint.hpp"
#include "parser/constraints/list.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Constraint> Transformer::TransformConstraint(postgres::ListCell *cell) {
	auto constraint = reinterpret_cast<postgres::Constraint *>(cell->data.ptr_value);
	switch (constraint->contype) {
	case postgres::CONSTR_UNIQUE:
	case postgres::CONSTR_PRIMARY: {
		auto type =
		    constraint->contype == postgres::CONSTR_PRIMARY ? ConstraintType::PRIMARY_KEY : ConstraintType::UNIQUE;
		vector<string> columns;
		for (auto kc = constraint->keys->head; kc; kc = kc->next) {
			columns.push_back(string(reinterpret_cast<postgres::Value *>(kc->data.ptr_value)->val.str));
		}
		return make_unique<ParsedConstraint>(type, columns);
	}
	default:
		throw NotImplementedException("Constraint type not handled yet!");
	}
}

unique_ptr<Constraint> Transformer::TransformConstraint(postgres::ListCell *cell, ColumnDefinition &column,
                                                        uint64_t index) {
	auto constraint = reinterpret_cast<postgres::Constraint *>(cell->data.ptr_value);
	assert(constraint);
	switch (constraint->contype) {
	case postgres::CONSTR_NOTNULL:
		return make_unique<NotNullConstraint>(index);
	case postgres::CONSTR_CHECK: {
		auto expression = TransformExpression(constraint->raw_expr);
		if (expression->HasSubquery()) {
			throw ParserException("subqueries prohibited in CHECK constraints");
		}
		if (expression->IsAggregate()) {
			throw ParserException("aggregates prohibited in CHECK constraints");
		}
		return make_unique<CheckConstraint>(TransformExpression(constraint->raw_expr));
	}
	case postgres::CONSTR_PRIMARY:
		return make_unique<ParsedConstraint>(ConstraintType::PRIMARY_KEY, index);
	case postgres::CONSTR_UNIQUE:
		return make_unique<ParsedConstraint>(ConstraintType::UNIQUE, index);
	case postgres::CONSTR_NULL:
		return nullptr;
	case postgres::CONSTR_DEFAULT:
		column.default_value = TransformExpression(constraint->raw_expr);
		return nullptr;
	case postgres::CONSTR_FOREIGN:
	default:
		throw NotImplementedException("Constraint not implemented!");
	}
}
