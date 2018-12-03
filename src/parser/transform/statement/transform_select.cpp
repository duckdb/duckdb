
#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<SelectStatement> Transformer::TransformSelect(Node *node) {
	SelectStmt *stmt = reinterpret_cast<SelectStmt *>(node);
	auto result = make_unique<SelectStatement>();

	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<WithClause *>(stmt->withClause), *result);
	}

	result->select_distinct = stmt->distinctClause != NULL ? true : false;
	result->setop_type = SetopType::NONE;
	result->from_table = TransformFrom(stmt->fromClause);
	TransformGroupBy(stmt->groupClause, result->groupby.groups);
	result->groupby.having = TransformExpression(stmt->havingClause);
	TransformOrderBy(stmt->sortClause, result->orderby);
	result->where_clause = TransformExpression(stmt->whereClause);
	if (stmt->limitCount) {
		result->limit.limit =
		    reinterpret_cast<A_Const *>(stmt->limitCount)->val.val.ival;
		result->limit.offset =
		    !stmt->limitOffset
		        ? 0
		        : reinterpret_cast<A_Const *>(stmt->limitOffset)->val.val.ival;
	}

	switch (stmt->op) {
	case SETOP_NONE: {
		// rest done above
		result->setop_type = SetopType::NONE;
		if (!TransformExpressionList(stmt->targetList, result->select_list)) {
			throw Exception("Failed to transform expression list.");
		}
		break;
	}
	case SETOP_UNION:
	case SETOP_EXCEPT:
	case SETOP_INTERSECT: {
		auto larg = TransformSelect((Node *)stmt->larg);
		auto rarg = TransformSelect((Node *)stmt->rarg);
		if (!larg || !rarg) {
			throw Exception("Failed to transform setop children.");
		}

		result->setop_left = move(larg);
		result->setop_right = move(rarg);
		result->select_distinct = true;

		switch (stmt->op) {
		case SETOP_UNION:
			result->select_distinct = !stmt->all;
			result->setop_type = SetopType::UNION;
			break;
		case SETOP_EXCEPT:
			result->setop_type = SetopType::EXCEPT;
			break;
		case SETOP_INTERSECT:
			result->setop_type = SetopType::INTERSECT;
			break;
		default:
			throw Exception("Unexpected setop type");
		}
		// if we compute the distinct result here, we do not have to do this in
		// the children saves bunch of aggrs
		if (result->select_distinct) {
			result->setop_left->select_distinct = false;
			result->setop_right->select_distinct = false;
		}
		break;
	}

	default:
		throw NotImplementedException("Statement type %d not implemented!",
		                              stmt->op);
	}
	return result;
}