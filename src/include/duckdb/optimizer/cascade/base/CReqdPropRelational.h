//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropRelational.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropRelational_H
#define GPOPT_CReqdPropRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

// forward declaration
class CExpressionHandle;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CReqdPropRelational
//
//	@doc:
//		Required relational properties container.
//
//---------------------------------------------------------------------------
class CReqdPropRelational : public CReqdProp
{
public:
	// required stat columns
	duckdb::vector<ColumnBinding> m_pcrsStat;

public:
	// default ctor
	CReqdPropRelational();
	
	// private copy ctor
	CReqdPropRelational(const CReqdPropRelational &) = delete;
	
	// ctor
	explicit CReqdPropRelational(duckdb::vector<ColumnBinding> pcrs);

	// dtor
	virtual ~CReqdPropRelational();

	// type of properties
	virtual bool FRelational() const override
	{
		return true;
	}

	// stat columns accessor
	duckdb::vector<ColumnBinding> PcrsStat() const
	{
		return m_pcrsStat;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq) override;

	// return difference from given properties
	CReqdPropRelational* PrprelDifference(CReqdPropRelational* prprel);

	// return true if property container is empty
	bool IsEmpty() const;

	// shorthand for conversion
	static CReqdPropRelational* GetReqdRelationalProps(CReqdProp* prp);
};	// class CReqdPropRelational

}  // namespace gpopt


#endif
