#include "parser/tableref/crossproductref.hpp"
#include "parser/tableref/joinref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<TableRef> Transformer::TransformJoin(JoinExpr *root) {
	auto result = make_unique<JoinRef>();
	switch (root->jointype) {
	case JOIN_INNER: {
		result->type = JoinType::INNER;
		break;
	}
	case JOIN_LEFT: {
		result->type = JoinType::LEFT;
		break;
	}
	case JOIN_FULL: {
		result->type = JoinType::OUTER;
		break;
	}
	case JOIN_RIGHT: {
		result->type = JoinType::RIGHT;
		break;
	}
	case JOIN_SEMI: {
		result->type = JoinType::SEMI;
		break;
	}
	default: {
		throw NotImplementedException("Join type %d not supported yet...\n", root->jointype); }
	}

	// Check the type of left arg and right arg before transform
	result->left = TransformTableRefNode(root->larg);
	result->right = TransformTableRefNode(root->rarg);

	if (!root->quals) { // CROSS PRODUCT
		auto cross = make_unique<CrossProductRef>();
		cross->left = move(result->left);
		cross->right = move(result->right);
		return move(cross);
	}

	result->condition = TransformExpression(root->quals);
	return move(result);
}
