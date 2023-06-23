#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformTableRefNode(duckdb_libpgquery::PGNode &n) {
	auto stack_checker = StackCheck();

	switch (n.type) {
	case duckdb_libpgquery::T_PGRangeVar:
		return TransformRangeVar(PGCast<duckdb_libpgquery::PGRangeVar>(n));
	case duckdb_libpgquery::T_PGJoinExpr:
		return TransformJoin(PGCast<duckdb_libpgquery::PGJoinExpr>(n));
	case duckdb_libpgquery::T_PGRangeSubselect:
		return TransformRangeSubselect(PGCast<duckdb_libpgquery::PGRangeSubselect>(n));
	case duckdb_libpgquery::T_PGRangeFunction:
		return TransformRangeFunction(PGCast<duckdb_libpgquery::PGRangeFunction>(n));
	case duckdb_libpgquery::T_PGPivotExpr:
		return TransformPivot(PGCast<duckdb_libpgquery::PGPivotExpr>(n));
	default:
		throw NotImplementedException("From Type %d not supported", n.type);
	}
}

} // namespace duckdb
