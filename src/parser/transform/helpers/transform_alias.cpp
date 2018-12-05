#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

string Transformer::TransformAlias(Alias *root) {
	if (!root) {
		return "";
	}
	return root->aliasname;
}
