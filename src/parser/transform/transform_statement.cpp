
#include "parser/transform.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SelectStatement> TransformSelect(Node *node) {
	SelectStmt *stmt = reinterpret_cast<SelectStmt *>(node);
	switch (stmt->op) {
	case SETOP_NONE: {
		auto result = make_unique<SelectStatement>();
		result->select_distinct = stmt->distinctClause != NULL ? true : false;
		if (!TransformExpressionList(stmt->targetList, result->select_list)) {
			return nullptr;
		}
		result->from_table = TransformFrom(stmt->fromClause);

		// try {
		// 	auto select_list = TargetTransform(root->targetList);
		// 	result->select_list = std::move(*select_list);
		// 	delete select_list;
		// 	result->from_table.reset(FromTransform(root));
		// } catch (ParserException &e) {
		// 	delete (result);
		// 	throw e;
		// }
		// result->group_by.reset(
		//     GroupByTransform(root->groupClause, root->havingClause));
		// result->order.reset(OrderByTransform(root->sortClause));
		// result->where_clause.reset(WhereTransform(root->whereClause));
		// if (root->limitCount != nullptr) {
		// 	int64_t limit =
		// 	    reinterpret_cast<A_Const *>(root->limitCount)->val.val.ival;
		// 	int64_t offset = 0;
		// 	if (root->limitOffset != nullptr)
		// 		offset = reinterpret_cast<A_Const *>(root->limitOffset)
		// 		             ->val.val.ival;
		// 	result->limit.reset(new LimitDescription(limit, offset));
		// }
		return result;
	}
	case SETOP_UNION: {
		auto result = TransformSelect((Node *)stmt->larg);
		if (!result) {
			return nullptr;
		}
		auto right = TransformSelect((Node *)stmt->rarg);
		if (!right) {
			return nullptr;
		}
		result->union_select = move(right);
		return result;
	}
	case SETOP_INTERSECT:
	case SETOP_EXCEPT:
	default:
		throw NotImplementedException("A_Expr not implemented!");
	}
}
