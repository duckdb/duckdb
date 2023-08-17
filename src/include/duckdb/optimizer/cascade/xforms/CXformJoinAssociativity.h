//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinAssociativity.h
//
//	@doc:
//		Join Associativity, A Join B Join C = A Join C Join B
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinAssociativity_H
#define GPOPT_CXformJoinAssociativity_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"
#include "duckdb/planner/joinside.hpp"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformJoinAssociativity
//
//	@doc:
//		Associate the join order
//
//---------------------------------------------------------------------------
class CXformJoinAssociativity : public CXformExploration
{
public:
	// ctor
	explicit CXformJoinAssociativity();
    
    CXformJoinAssociativity(const CXformJoinCommutativity &) = delete;
	
    // dtor
	virtual ~CXformJoinAssociativity()
	{
	}

	// ident accessors
	virtual EXformId ID() const
	{
		return ExfJoinAssociativity;
	}

	// return a string for xform name
	virtual const CHAR* Name() const
	{
		return "CXformJoinAssociativity";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise XformPromise(CExpressionHandle &exprhdl) const;
	
	void CreatePredicates(Operator* join, duckdb::vector<JoinCondition> &upper_join_condition,
                        duckdb::vector<JoinCondition> &lower_join_condition) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt
#endif