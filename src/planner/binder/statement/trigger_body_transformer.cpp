#include "duckdb/planner/trigger_body_transformer.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

static bool IsNewRef(const string &name) {
	return StringUtil::CIEquals(name, "new");
}

static void RewriteNewColumnRefs(QueryNode &node, const string &base_cte_name) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(node, [&](unique_ptr<ParsedExpression> &expr) {
		ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(*expr, [&](ColumnRefExpression &col_ref) {
			if (col_ref.column_names.size() >= 2 && IsNewRef(col_ref.column_names[0])) {
				col_ref.column_names[0] = base_cte_name;
			}
		});
	});
}

static void ReplaceNewTableRef(unique_ptr<TableRef> &ref, const string &base_cte_name) {
	if (!ref) {
		return;
	}
	switch (ref->type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref->Cast<BaseTableRef>();
		if (IsNewRef(base.table_name)) {
			base.table_name = base_cte_name;
			base.schema_name = "";
			base.catalog_name = "";
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref->Cast<JoinRef>();
		ReplaceNewTableRef(join.left, base_cte_name);
		ReplaceNewTableRef(join.right, base_cte_name);
		break;
	}
	default:
		break;
	}
}

static void TransformValuesToSelect(SelectNode &select, const string &base_cte_name) {
	// The parser represents VALUES as SelectNode(from=ExpressionListRef, select=[StarExpression])
	auto &expr_list = select.from_table->Cast<ExpressionListRef>();
	// FIXME: We currently support only single-row VALUES
	D_ASSERT(expr_list.values.size() == 1);
	select.select_list = std::move(expr_list.values[0]);
	auto from_ref = make_uniq<BaseTableRef>();
	from_ref->table_name = base_cte_name;
	select.from_table = std::move(from_ref);
}

static bool HasOldRef(QueryNode &node) {
	bool found = false;
	ParsedExpressionIterator::EnumerateQueryNodeChildren(node, [&](unique_ptr<ParsedExpression> &expr) {
		if (found) {
			return;
		}
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(*expr, [&](const ColumnRefExpression &col_ref) {
			if (!col_ref.column_names.empty() && StringUtil::CIEquals(col_ref.column_names[0], "old")) {
				found = true;
			}
		});
	});
	return found;
}

static bool ReadsFromTable(QueryNode &body, const string &table_name) {
	bool found = false;
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    body, [](unique_ptr<ParsedExpression> &) {},
	    [&](TableRef &ref) {
		    if (found || ref.type != TableReferenceType::BASE_TABLE) {
			    return;
		    }
		    if (StringUtil::CIEquals(ref.Cast<BaseTableRef>().table_name, table_name)) {
			    found = true;
		    }
	    });
	return found;
}

static string BulkTransformError(QueryNode &body, TriggerEventType event_type) {
	if (event_type != TriggerEventType::INSERT_EVENT) {
		return "FOR EACH ROW is only supported for AFTER INSERT triggers";
	}
	if (body.type != QueryNodeType::INSERT_QUERY_NODE) {
		return "FOR EACH ROW trigger bodies must be INSERT statements; UPDATE/DELETE bodies are not yet supported";
	}
	if (HasOldRef(body)) {
		return "OLD references in FOR EACH ROW trigger bodies are not yet supported";
	}
	auto &insert = body.Cast<InsertQueryNode>();
	if (insert.select_statement && insert.select_statement->node &&
	    insert.select_statement->node->type != QueryNodeType::SELECT_NODE) {
		return "FOR EACH ROW trigger bodies only support a single SELECT or VALUES clause; UNION, INTERSECT, and "
		       "EXCEPT are not supported";
	}
	auto values_list = insert.GetValuesList();
	if (values_list && values_list->values.size() > 1) {
		return "Multi-row VALUES in FOR EACH ROW trigger bodies are not yet supported";
	}
	// Block INSERT INTO X ... SELECT ... FROM X: bulk reads X at pre-trigger snapshot, not per-row.
	if (ReadsFromTable(body, insert.table)) {
		return "FOR EACH ROW trigger bodies cannot read from the INSERT target table";
	}
	return "";
}

static void TransformForEachRowBodyAsBulk(QueryNode &body, const string &base_cte_name) {
	switch (body.type) {
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &insert = body.Cast<InsertQueryNode>();
		D_ASSERT(insert.select_statement && insert.select_statement->node);
		auto &inner = *insert.select_statement->node;
		D_ASSERT(inner.type == QueryNodeType::SELECT_NODE);
		auto &select = inner.Cast<SelectNode>();

		RewriteNewColumnRefs(inner, base_cte_name);

		if (select.from_table && select.from_table->type == TableReferenceType::EXPRESSION_LIST) {
			TransformValuesToSelect(select, base_cte_name);
			return;
		}
		if (select.from_table) {
			ReplaceNewTableRef(select.from_table, base_cte_name);
		}
		if (!select.from_table) {
			auto from_ref = make_uniq<BaseTableRef>();
			from_ref->table_name = base_cte_name;
			select.from_table = std::move(from_ref);
		}
		return;
	}
	case QueryNodeType::UPDATE_QUERY_NODE:
	case QueryNodeType::DELETE_QUERY_NODE:
		throw NotImplementedException("FOR EACH ROW transformation is not yet implemented for UPDATE/DELETE bodies");
	default:
		throw NotImplementedException("Unsupported FOR EACH ROW trigger body type");
	}
}

void TransformTriggerBody(QueryNode &body, TriggerEventType event_type) {
	auto error = BulkTransformError(body, event_type);
	if (error.empty()) {
		TransformForEachRowBodyAsBulk(body, TRIGGER_BASE_CTE_NAME);
	} else {
		throw NotImplementedException(error);
	}
}

} // namespace duckdb
