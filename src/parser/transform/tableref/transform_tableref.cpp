#include "common/exception.hpp"
#include "parser/tableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformTableRefNode(postgres::Node *n) {
	switch (n->type) {
	case postgres::T_RangeVar:
		return TransformRangeVar(reinterpret_cast<postgres::RangeVar *>(n));
	case postgres::T_JoinExpr:
		return TransformJoin(reinterpret_cast<postgres::JoinExpr *>(n));
	case postgres::T_RangeSubselect:
		return TransformRangeSubselect(reinterpret_cast<postgres::RangeSubselect *>(n));
	case postgres::T_RangeFunction:
		return TransformRangeFunction(reinterpret_cast<postgres::RangeFunction *>(n));
	default:
		throw NotImplementedException("From Type %d not supported yet...", n->type);
	}
}
