#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformFrom(PGList *root) {
	if (!root) {
		return make_unique<EmptyTableRef>();
	}

	if (root->length > 1) {
		// Cross Product
		auto result = make_unique<CrossProductRef>();
		CrossProductRef *cur_root = result.get();
		for (auto node = root->head; node != nullptr; node = node->next) {
			auto n = reinterpret_cast<PGNode *>(node->data.ptr_value);
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

	auto n = reinterpret_cast<PGNode *>(root->head->data.ptr_value);
	return TransformTableRefNode(n);
}
