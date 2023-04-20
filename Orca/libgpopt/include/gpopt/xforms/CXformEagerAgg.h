//---------------------------------------------------------------------------

//  Greenplum Database
//  Copyright (C) 2018 Pivotal Inc.
//
//  @filename:
//      CXformEagerAgg.h
//
//  @doc:
//      Eagerly push aggregates below join when there is no primary/foreign keys
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformEagerAgg_H
#define GPOPT_CXformEagerAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformEagerAgg
//
//	@doc:
//		Eagerly push aggregates below join
//
//---------------------------------------------------------------------------
class CXformEagerAgg : public CXformExploration
{
public:
	// ctor
	explicit CXformEagerAgg(CMemoryPool *mp);

	// ctor
	explicit CXformEagerAgg(CExpression *exprPattern);

	// dtor
	virtual ~CXformEagerAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfEagerAgg;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformEagerAgg";
	}

	// compatibility function for eager aggregation
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return (CXform::ExfEagerAgg != exfid) &&
			   (CXform::ExfSplitGbAgg != exfid) &&
			   (CXform::ExfSplitDQA != exfid);
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *expr) const;

	// return true if xform should be applied only once
	virtual BOOL
	IsApplyOnce()
	{
		return true;
	};

private:
	// private copy ctor
	CXformEagerAgg(const CXformEagerAgg &);

	// check if transform can be applied
	BOOL CanApplyTransform(CExpression *agg_expr) const;

	// is this aggregate supported for push down?
	BOOL CanPushAggBelowJoin(CExpression *scalar_agg_func_expr) const;

	// generate project lists for the lower and upper aggregates
	// from all the original aggregates
	void PopulateLowerUpperProjectList(
		CMemoryPool *mp,			  // memory pool
		CExpression *orig_proj_list,  // project list of the original aggregate
		CExpression *
			*lower_proj_list,  // output project list of the new lower aggregate
		CExpression *
			*upper_proj_list  // output project list of the new upper aggregate
	) const;

	// generate project element for lower aggregate for a single original aggregate
	void PopulateLowerProjectElement(
		CMemoryPool *mp,  // memory pool
		IMDId *agg_mdid,  // original global aggregate function
		CWStringConst *agg_name, CExpressionArray *agg_arg_array,
		BOOL is_distinct,
		CExpression **
			lower_proj_elem_expr  // output project element of the new lower aggregate
	) const;

	// generate project element for upper aggregate
	void PopulateUpperProjectElement(
		CMemoryPool *mp,  // memory pool
		IMDId *agg_mdid,  // aggregate mdid to create
		CWStringConst *agg_name, CColRef *lower_colref, CColRef *output_colref,
		BOOL is_distinct,
		CExpression **
			upper_proj_elem_expr  // output project element of the new upper aggregate
	) const;
};	// class CXformEagerAgg
}  // namespace gpopt

#endif	// !GPOPT_CXformEagerAgg_H

// EOF
