#include "common/exception.hpp"
#include "parser/tableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<TableRef> Transformer::TransformTableRefNode(Node *n) {
	switch (n->type) {
	case T_RangeVar:
		return TransformRangeVar(reinterpret_cast<RangeVar *>(n));
	case T_JoinExpr:
		return TransformJoin(reinterpret_cast<JoinExpr *>(n));
	case T_RangeSubselect:
		return TransformRangeSubselect(reinterpret_cast<RangeSubselect *>(n));
	case T_RangeFunction:
		return TransformRangeFunction(reinterpret_cast<RangeFunction *>(n));
	default:
		throw NotImplementedException("From Type %d not supported yet...", n->type);
	}
}
