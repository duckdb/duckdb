#include "duckdb/parser/transformer.hpp"

namespace duckdb {

vector<string> Transformer::TransformStringList(duckdb_libpgquery::PGList *list) {
	vector<string> result;
	if (!list) {
		return result;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		result.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(node->data.ptr_value)->val.str);
	}
	return result;
}

string Transformer::TransformAlias(duckdb_libpgquery::PGAlias *root, vector<string> &column_name_alias) {
	if (!root) {
		return "";
	}
	column_name_alias = TransformStringList(root->colnames);
	return root->aliasname;
}

} // namespace duckdb
