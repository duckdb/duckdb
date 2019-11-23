#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformTableRefNode(PGNode *n) {
	switch (n->type) {
	case T_PGRangeVar:
		return TransformRangeVar(reinterpret_cast<PGRangeVar *>(n));
	case T_PGJoinExpr:
		return TransformJoin(reinterpret_cast<PGJoinExpr *>(n));
	case T_PGRangeSubselect:
		return TransformRangeSubselect(reinterpret_cast<PGRangeSubselect *>(n));
	case T_PGRangeFunction:
		return TransformRangeFunction(reinterpret_cast<PGRangeFunction *>(n));
	default:
		throw NotImplementedException("From Type %d not supported yet...", n->type);
	}
}
