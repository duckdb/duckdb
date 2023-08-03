//---------------------------------------------------------------------------
//	@filename:
//		CBinding.h
//
//	@doc:
//		Binding mechanism to extract expression from Memo according to pattern
//---------------------------------------------------------------------------
#ifndef GPOPT_CBinding_H
#define GPOPT_CBinding_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include <memory>
#include <list>

using namespace gpos;
using namespace duckdb;
using namespace std;

namespace gpopt
{
// fwd declaration
class CGroupExpression;
class CGroup;

//---------------------------------------------------------------------------
//	@class:
//		CBinding
//
//	@doc:
//		Binding class used to iteratively generate expressions from the
//		memo so that they match a given pattern
//
//---------------------------------------------------------------------------
class CBinding
{
public:
	// initialize cursors of child expressions
	bool FInitChildCursors(CGroupExpression* pgexpr, Operator* pexprPattern, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr);

	// advance cursors of child expressions
	bool FAdvanceChildCursors(CGroupExpression* pgexpr, Operator* pexprPattern, Operator* pexprLast, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr);

	// move cursor
	list<CGroupExpression*>::iterator PgexprNext(CGroup* pgroup, CGroupExpression* pgexpr) const;

	// expand n-th child of pattern
	Operator* PexprExpandPattern(Operator* pexpr, ULONG ulPos, ULONG arity);

	// get binding for children
	BOOL FExtractChildren(CGroupExpression* pgexpr, Operator* pexprPattern, Operator* pexprLast, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexprChildren);

	// extract binding from a group
	Operator* PexprExtract(CGroup* pgroup, Operator* pexprPattern, Operator* pexprLast);
	
	// extract binding from group expression
	Operator* PexprExtract(CGroupExpression* pgexpr, Operator* pexprPatetrn, Operator* pexprLast);
	
	// build expression
	Operator* PexprFinalize(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprChildren);

public:
	// ctor
	CBinding()
	{
	}
	
	// no copy ctor
	CBinding(const CBinding &) = delete;
	
	// dtor
	~CBinding()
	{
	}
};	// class CBinding
}  // namespace gpopt
#endif