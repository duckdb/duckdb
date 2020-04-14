#include "duckdb/common/exception.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<QueryNode> Transformer::TransformSelectNode(PGSelectStmt *stmt) {
	unique_ptr<QueryNode> node;

	switch (stmt->op) {
	case PG_SETOP_NONE: {
		node = make_unique<SelectNode>();
		auto result = (SelectNode *)node.get();

		if (stmt->windowClause) {
			for (auto window_ele = stmt->windowClause->head; window_ele != NULL; window_ele = window_ele->next) {
				auto window_def = reinterpret_cast<PGWindowDef *>(window_ele->data.ptr_value);
				assert(window_def);
				assert(window_def->name);
				auto window_name = StringUtil::Lower(string(window_def->name));

				auto it = window_clauses.find(window_name);
				if (it != window_clauses.end()) {
					throw ParserException("window \"%s\" is already defined", window_name.c_str());
				}
				window_clauses[window_name] = window_def;
			}
		}

		// do this early so the value lists also have a `FROM`
		if (stmt->valuesLists) {
			// VALUES list, create an ExpressionList
			assert(!stmt->fromClause);
			result->from_table = TransformValuesList(stmt->valuesLists);
			result->select_list.push_back(make_unique<StarExpression>());
		} else {
			result->from_table = TransformFrom(stmt->fromClause);
			if (!stmt->targetList) {
				throw ParserException("SELECT clause without selection list");
			}
			// select list
			if (!TransformExpressionList(stmt->targetList, result->select_list)) {
				throw Exception("Failed to transform expression list.");
			}
		}
		// checks distinct clause
		if (stmt->distinctClause != NULL) {
			auto modifier = make_unique<DistinctModifier>();
			// checks distinct on clause
			auto target = reinterpret_cast<PGNode *>(stmt->distinctClause->head->data.ptr_value);
			if (target) {
				//  add the columns defined in the ON clause to the select list
				if (!TransformExpressionList(stmt->distinctClause, modifier->distinct_on_targets)) {
					throw Exception("Failed to transform expression list from DISTINCT ON.");
				}
			}
			result->modifiers.push_back(move(modifier));
		}
		// from table
		// group by
		TransformGroupBy(stmt->groupClause, result->groups);
		result->having = TransformExpression(stmt->havingClause);
		// where
		result->where_clause = TransformExpression(stmt->whereClause);
		break;
	}
	case PG_SETOP_UNION:
	case PG_SETOP_EXCEPT:
	case PG_SETOP_INTERSECT: {
		node = make_unique<SetOperationNode>();
		auto result = (SetOperationNode *)node.get();
		result->left = TransformSelectNode(stmt->larg);
		result->right = TransformSelectNode(stmt->rarg);
		if (!result->left || !result->right) {
			throw Exception("Failed to transform setop children.");
		}

		bool select_distinct = true;
		switch (stmt->op) {
		case PG_SETOP_UNION:
			select_distinct = !stmt->all;
			result->setop_type = SetOperationType::UNION;
			break;
		case PG_SETOP_EXCEPT:
			result->setop_type = SetOperationType::EXCEPT;
			break;
		case PG_SETOP_INTERSECT:
			result->setop_type = SetOperationType::INTERSECT;
			break;
		default:
			throw Exception("Unexpected setop type");
		}
		if (select_distinct) {
			result->modifiers.push_back(make_unique<DistinctModifier>());
		}
		break;
	}
	default:
		throw NotImplementedException("Statement type %d not implemented!", stmt->op);
	}
	// transform the common properties
	// both the set operations and the regular select can have an ORDER BY/LIMIT attached to them
	vector<OrderByNode> orders;
	TransformOrderBy(stmt->sortClause, orders);
	if (orders.size() > 0) {
		auto order_modifier = make_unique<OrderModifier>();
		order_modifier->orders = move(orders);
		node->modifiers.push_back(move(order_modifier));
	}
	if (stmt->limitCount || stmt->limitOffset) {
		auto limit_modifier = make_unique<LimitModifier>();
		if (stmt->limitCount) {
			limit_modifier->limit = TransformExpression(stmt->limitCount);
		}
		if (stmt->limitOffset) {
			limit_modifier->offset = TransformExpression(stmt->limitOffset);
		}
		node->modifiers.push_back(move(limit_modifier));
	}
	return node;
}
