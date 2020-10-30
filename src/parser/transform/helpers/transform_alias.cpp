#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

string Transformer::TransformAlias(PGAlias *root, vector<string> &column_name_alias) {
	if (!root) {
		return "";
	}
	if (root->colnames) {
		for (auto node = root->colnames->head; node != nullptr; node = node->next) {
			column_name_alias.push_back(reinterpret_cast<PGValue *>(node->data.ptr_value)->val.str);
		}
	}
	return root->aliasname;
}

} // namespace duckdb
