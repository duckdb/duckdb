#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformFrom(optional_ptr<duckdb_libpgquery::PGList> root) {
	if (!root) {
		return make_uniq<EmptyTableRef>();
	}

	if (root->length > 1) {
		// Cross Product
		auto result = make_uniq<JoinRef>(JoinRefType::CROSS);
		JoinRef *cur_root = result.get();
		idx_t list_size = 0;
		for (auto node = root->head; node != nullptr; node = node->next) {
			auto n = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
			unique_ptr<TableRef> next = TransformTableRefNode(*n);
			if (!cur_root->left) {
				cur_root->left = std::move(next);
			} else if (!cur_root->right) {
				cur_root->right = std::move(next);
			} else {
				auto old_res = std::move(result);
				result = make_uniq<JoinRef>(JoinRefType::CROSS);
				result->left = std::move(old_res);
				result->right = std::move(next);
				cur_root = result.get();
			}
			list_size++;
			StackCheck(list_size);
		}
		return std::move(result);
	}

	auto n = PGPointerCast<duckdb_libpgquery::PGNode>(root->head->data.ptr_value);
	return TransformTableRefNode(*n);
}

} // namespace duckdb
