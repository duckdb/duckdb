#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"

namespace duckdb {

void Transformer::TransformModifiers(duckdb_libpgquery::PGSelectStmt &stmt, QueryNode &node) {
	// transform the common properties
	// both the set operations and the regular select can have an ORDER BY/LIMIT attached to them
	vector<OrderByNode> orders;
	TransformOrderBy(stmt.sortClause, orders);
	if (!orders.empty()) {
		auto order_modifier = make_uniq<OrderModifier>();
		order_modifier->orders = std::move(orders);
		node.modifiers.push_back(std::move(order_modifier));
	}
	if (stmt.limitCount || stmt.limitOffset) {
		if (stmt.limitCount && stmt.limitCount->type == duckdb_libpgquery::T_PGLimitPercent) {
			auto limit_percent_modifier = make_uniq<LimitPercentModifier>();
			auto expr_node = PGPointerCast<duckdb_libpgquery::PGLimitPercent>(stmt.limitCount)->limit_percent;
			limit_percent_modifier->limit = TransformExpression(expr_node);
			if (stmt.limitOffset) {
				limit_percent_modifier->offset = TransformExpression(stmt.limitOffset);
			}
			node.modifiers.push_back(std::move(limit_percent_modifier));
		} else {
			auto limit_modifier = make_uniq<LimitModifier>();
			if (stmt.limitCount) {
				limit_modifier->limit = TransformExpression(stmt.limitCount);
			}
			if (stmt.limitOffset) {
				limit_modifier->offset = TransformExpression(stmt.limitOffset);
			}
			node.modifiers.push_back(std::move(limit_modifier));
		}
	}
}

unique_ptr<QueryNode> Transformer::TransformSelectInternal(duckdb_libpgquery::PGSelectStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGSelectStmt);
	auto stack_checker = StackCheck();

	unique_ptr<QueryNode> node;
	vector<unique_ptr<CTENode>> materialized_ctes;

	switch (stmt.op) {
	case duckdb_libpgquery::PG_SETOP_NONE: {
		node = make_uniq<SelectNode>();
		auto &result = node->Cast<SelectNode>();
		if (stmt.withClause) {
			TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node->cte_map,
			             materialized_ctes);
		}
		if (stmt.windowClause) {
			for (auto window_ele = stmt.windowClause->head; window_ele != nullptr; window_ele = window_ele->next) {
				auto window_def = PGPointerCast<duckdb_libpgquery::PGWindowDef>(window_ele->data.ptr_value);
				D_ASSERT(window_def);
				D_ASSERT(window_def->name);
				string window_name(window_def->name);
				auto it = window_clauses.find(window_name);
				if (it != window_clauses.end()) {
					throw ParserException("window \"%s\" is already defined", window_name);
				}
				window_clauses[window_name] = window_def.get();
			}
		}

		// checks distinct clause
		if (stmt.distinctClause != nullptr) {
			auto modifier = make_uniq<DistinctModifier>();
			// checks distinct on clause
			auto target = PGPointerCast<duckdb_libpgquery::PGNode>(stmt.distinctClause->head->data.ptr_value);
			if (target) {
				//  add the columns defined in the ON clause to the select list
				TransformExpressionList(*stmt.distinctClause, modifier->distinct_on_targets);
			}
			result.modifiers.push_back(std::move(modifier));
		}

		// do this early so the value lists also have a `FROM`
		if (stmt.valuesLists) {
			// VALUES list, create an ExpressionList
			D_ASSERT(!stmt.fromClause);
			result.from_table = TransformValuesList(stmt.valuesLists);
			result.select_list.push_back(make_uniq<StarExpression>());
		} else {
			if (!stmt.targetList) {
				throw ParserException("SELECT clause without selection list");
			}
			// select list
			TransformExpressionList(*stmt.targetList, result.select_list);
			result.from_table = TransformFrom(stmt.fromClause);
		}

		// where
		result.where_clause = TransformExpression(stmt.whereClause);
		// group by
		TransformGroupBy(stmt.groupClause, result);
		// having
		result.having = TransformExpression(stmt.havingClause);
		// qualify
		result.qualify = TransformExpression(stmt.qualifyClause);
		// sample
		result.sample = TransformSampleOptions(stmt.sampleOptions);
		break;
	}
	case duckdb_libpgquery::PG_SETOP_UNION:
	case duckdb_libpgquery::PG_SETOP_EXCEPT:
	case duckdb_libpgquery::PG_SETOP_INTERSECT:
	case duckdb_libpgquery::PG_SETOP_UNION_BY_NAME: {
		node = make_uniq<SetOperationNode>();
		auto &result = node->Cast<SetOperationNode>();
		if (stmt.withClause) {
			TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node->cte_map,
			             materialized_ctes);
		}
		result.left = TransformSelectNode(*stmt.larg);
		result.right = TransformSelectNode(*stmt.rarg);
		if (!result.left || !result.right) {
			throw Exception("Failed to transform setop children.");
		}

		bool select_distinct = true;
		switch (stmt.op) {
		case duckdb_libpgquery::PG_SETOP_UNION:
			select_distinct = !stmt.all;
			result.setop_type = SetOperationType::UNION;
			break;
		case duckdb_libpgquery::PG_SETOP_EXCEPT:
			result.setop_type = SetOperationType::EXCEPT;
			break;
		case duckdb_libpgquery::PG_SETOP_INTERSECT:
			result.setop_type = SetOperationType::INTERSECT;
			break;
		case duckdb_libpgquery::PG_SETOP_UNION_BY_NAME:
			select_distinct = !stmt.all;
			result.setop_type = SetOperationType::UNION_BY_NAME;
			break;
		default:
			throw Exception("Unexpected setop type");
		}
		if (select_distinct) {
			result.modifiers.push_back(make_uniq<DistinctModifier>());
		}
		if (stmt.sampleOptions) {
			throw ParserException("SAMPLE clause is only allowed in regular SELECT statements");
		}
		break;
	}
	default:
		throw NotImplementedException("Statement type %d not implemented!", stmt.op);
	}

	TransformModifiers(stmt, *node);

	// Handle materialized CTEs
	node = Transformer::TransformMaterializedCTE(std::move(node), materialized_ctes);

	return node;
}

} // namespace duckdb
