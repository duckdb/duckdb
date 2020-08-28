#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

string Transformer::TransformAlias(PGAlias *root) {
	if (!root) {
		return "";
	}
	return root->aliasname;
}

} // namespace duckdb
