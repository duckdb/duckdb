//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryExistential.cpp
//
//	@doc:
//		Implementation of existential subquery operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarSubqueryExistential.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::CScalarSubqueryExistential
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryExistential::CScalarSubqueryExistential(CMemoryPool *mp)
	: CScalar(mp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::~CScalarSubqueryExistential
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarSubqueryExistential::~CScalarSubqueryExistential()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::MdidType
//
//	@doc:
//		Type of scalar's value
//
//---------------------------------------------------------------------------
IMDId* CScalarSubqueryExistential::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CScalarSubqueryExistential::Matches(COperator *pop) const
{
	GPOS_ASSERT(NULL != pop);

	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::DerivePartitionInfo
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo* CScalarSubqueryExistential::PpartinfoDerive(CMemoryPool* mp, CExpressionHandle &exprhdl) const
{
	CPartInfo *ppartinfoChild = exprhdl.DerivePartitionInfo(0);
	GPOS_ASSERT(NULL != ppartinfoChild);
	ppartinfoChild->AddRef();
	return ppartinfoChild;
}