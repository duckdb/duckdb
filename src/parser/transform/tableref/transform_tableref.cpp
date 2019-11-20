#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformTableRefNode(postgres::PGNode *n) {
	switch (n->type) {
	case postgres::T_PGRangeVar:
		return TransformRangeVar(reinterpret_cast<postgres::PGRangeVar *>(n));
	case postgres::T_PGJoinExpr:
		return TransformJoin(reinterpret_cast<postgres::PGJoinExpr *>(n));
	case postgres::T_PGRangeSubselect:
		return TransformRangeSubselect(reinterpret_cast<postgres::PGRangeSubselect *>(n));
	case postgres::T_PGRangeFunction:
		return TransformRangeFunction(reinterpret_cast<postgres::PGRangeFunction *>(n));
	default:
		throw NotImplementedException("From Type %d not supported yet...", n->type);
	}
}
