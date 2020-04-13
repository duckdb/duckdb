#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformJoin(PGJoinExpr *root) {
	auto result = make_unique<JoinRef>();
	switch (root->jointype) {
	case PG_JOIN_INNER: {
		result->type = JoinType::INNER;
		break;
	}
	case PG_JOIN_LEFT: {
		result->type = JoinType::LEFT;
		break;
	}
	case PG_JOIN_FULL: {
		result->type = JoinType::OUTER;
		break;
	}
	case PG_JOIN_RIGHT: {
		result->type = JoinType::RIGHT;
		break;
	}
	case PG_JOIN_SEMI: {
		result->type = JoinType::SEMI;
		break;
	}
	default: { throw NotImplementedException("Join type %d not supported yet...\n", root->jointype); }
	}

	// Check the type of left arg and right arg before transform
	result->left = TransformTableRefNode(root->larg);
	result->right = TransformTableRefNode(root->rarg);

	if (root->usingClause && root->usingClause->length > 0) {
		// usingClause is a list of strings
		for (auto node = root->usingClause->head; node != nullptr; node = node->next) {
			auto target = reinterpret_cast<PGNode *>(node->data.ptr_value);
			assert(target->type == T_PGString);
			auto column_name = string(reinterpret_cast<PGValue *>(target)->val.str);
			result->using_columns.push_back(column_name);
		}
		return move(result);
	}

	if (!root->quals && result->using_columns.size() == 0) { // CROSS PRODUCT
		auto cross = make_unique<CrossProductRef>();
		cross->left = move(result->left);
		cross->right = move(result->right);
		return move(cross);
	}

	result->condition = TransformExpression(root->quals);
	return move(result);
}
