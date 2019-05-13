#include "parser/parsed_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

// FIXME: what is the difference between GroupBy and expression list?
bool Transformer::TransformGroupBy(List *group, vector<unique_ptr<ParsedExpression>> &result) {
	if (!group) {
		return false;
	}

	for (auto node = group->head; node != nullptr; node = node->next) {
		Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
		result.push_back(TransformExpression(n));
	}
	return true;
}
