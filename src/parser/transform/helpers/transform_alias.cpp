#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

string Transformer::TransformAlias(postgres::PGAlias *root) {
	if (!root) {
		return "";
	}
	return root->aliasname;
}
