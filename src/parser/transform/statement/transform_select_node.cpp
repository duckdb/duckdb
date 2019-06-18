#include "common/exception.hpp"
#include "parser/query_node/select_node.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<QueryNode> Transformer::TransformSelectNode(postgres::SelectStmt *stmt) {
	unique_ptr<QueryNode> node;
	switch (stmt->op) {
	case SETOP_NONE: {
		node = make_unique<SelectNode>();
		auto result = (SelectNode *)node.get();
		if (stmt->valuesLists) {
			TransformValuesList(stmt->valuesLists, result->values);
			return node;
		}
		// checks distinct clause
		if (stmt->distinctClause != NULL) {
			result->select_distinct = true;
			// checks distinct on clause
			auto target = reinterpret_cast<Node *>(stmt->distinctClause->head->data.ptr_value);
			if (target) {
				//  add the columns defined in the ON clause to the select list
				if (!TransformExpressionList(stmt->distinctClause, result->distinct_on_targets)) {
					throw Exception("Failed to transform expression list from DISTINCT ON.");
				}
			}
		}
		// from table
		result->from_table = TransformFrom(stmt->fromClause);
		// group by
		TransformGroupBy(stmt->groupClause, result->groups);
		result->having = TransformExpression(stmt->havingClause);
		if (result->groups.size() == 0 && result->having) {
			throw ParserException("a GROUP BY clause is required before HAVING");
		}
		// where
		result->where_clause = TransformExpression(stmt->whereClause);
		// select list
		if (!TransformExpressionList(stmt->targetList, result->select_list)) {
			throw Exception("Failed to transform expression list.");
		}
		break;
	}
	case SETOP_UNION:
	case SETOP_EXCEPT:
	case SETOP_INTERSECT: {
		node = make_unique<SetOperationNode>();
		auto result = (SetOperationNode *)node.get();
		result->left = TransformSelectNode(stmt->larg);
		result->right = TransformSelectNode(stmt->rarg);
		if (!result->left || !result->right) {
			throw Exception("Failed to transform setop children.");
		}

		result->select_distinct = true;
		switch (stmt->op) {
		case SETOP_UNION:
			result->select_distinct = !stmt->all;
			result->setop_type = SetOperationType::UNION;
			break;
		case SETOP_EXCEPT:
			result->setop_type = SetOperationType::EXCEPT;
			break;
		case SETOP_INTERSECT:
			result->setop_type = SetOperationType::INTERSECT;
			break;
		default:
			throw Exception("Unexpected setop type");
		}
		// if we compute the distinct result here, we do not have to do this in
		// the children. This saves a bunch of unnecessary DISTINCTs.
		if (result->select_distinct) {
			result->left->select_distinct = false;
			result->right->select_distinct = false;
		}
		break;
	}
	default:
		throw NotImplementedException("Statement type %d not implemented!", stmt->op);
	}
	// transform the common properties
	// both the set operations and the regular select can have an ORDER BY/LIMIT attached to them
	TransformOrderBy(stmt->sortClause, node->orders);
	if (stmt->limitCount) {
		node->limit = TransformExpression(stmt->limitCount);
	}
	if (stmt->limitOffset) {
		node->offset = TransformExpression(stmt->limitOffset);
	}
	return node;
}
