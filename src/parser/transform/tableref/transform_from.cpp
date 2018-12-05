#include "parser/tableref/crossproductref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<TableRef> Transformer::TransformFrom(List *root) {
	if (!root) {
		return nullptr;
	}

	if (root->length > 1) {
		// Cross Product
		auto result = make_unique<CrossProductRef>();
		CrossProductRef *cur_root = result.get();
		for (auto node = root->head; node != nullptr; node = node->next) {
			Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
			unique_ptr<TableRef> next = TransformTableRefNode(n);
			if (!cur_root->left) {
				cur_root->left = move(next);
			} else if (!cur_root->right) {
				cur_root->right = move(next);
			} else {
				auto old_res = move(result);
				result = make_unique<CrossProductRef>();
				result->left = move(old_res);
				result->right = move(next);
				cur_root = result.get();
			}
		}
		return move(result);
	}

	Node *n = reinterpret_cast<Node *>(root->head->data.ptr_value);
	return TransformTableRefNode(n);
}
