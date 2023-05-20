//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarCmp.cpp
//
//	@doc:
//		Implementation of scalar comparison operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarCmp.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::CScalarCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCmp::CScalarCmp(CMemoryPool *mp, IMDId *mdid_op, const CWStringConst *pstrOp, IMDType::ECmpType cmp_type)
	: CScalar(mp), m_mdid_op(mdid_op), m_pstrOp(pstrOp), m_comparision_type(cmp_type), m_returns_null_on_null_input(false)
{
	GPOS_ASSERT(mdid_op->IsValid());
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	m_returns_null_on_null_input = CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(md_accessor, m_mdid_op);
	m_fCommutative = CMDAccessorUtils::FCommutativeScalarOp(md_accessor, m_mdid_op);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::GetMDName
//
//	@doc:
//		Comparison operator name
//
//---------------------------------------------------------------------------
const CWStringConst* CScalarCmp::Pstr() const
{
	return m_pstrOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::MdIdOp
//
//	@doc:
//		Comparison operator metadata id
//
//---------------------------------------------------------------------------
IMDId* CScalarCmp::MdIdOp() const
{
	return m_mdid_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG CScalarCmp::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_mdid_op->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CScalarCmp::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);
		// match if operator oid are identical
		return m_mdid_op->Equals(popScCmp->MdIdOp());
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to order of inputs
//
//---------------------------------------------------------------------------
BOOL CScalarCmp::FInputOrderSensitive() const
{
	return !m_fCommutative;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId* CScalarCmp::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult CScalarCmp::Eber(ULongPtrArray *pdrgpulChildren) const
{
	if (m_returns_null_on_null_input)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}
	return EberAny;
}

// get metadata id of the commuted operator
IMDId* CScalarCmp::PmdidCommuteOp(CMDAccessor *md_accessor, COperator *pop)
{
	CScalarCmp *popScalarCmp = dynamic_cast<CScalarCmp *>(pop);
	const IMDScalarOp *pmdScalarCmpOp = md_accessor->RetrieveScOp(popScalarCmp->MdIdOp());
	IMDId *pmdidScalarCmpCommute = pmdScalarCmpOp->GetCommuteOpMdid();
	return pmdidScalarCmpCommute;
}

// get the string representation of a metadata object
CWStringConst* CScalarCmp::Pstr(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid)
{
	mdid->AddRef();
	return GPOS_NEW(mp) CWStringConst(mp, (md_accessor->RetrieveScOp(mdid)->Mdname().GetMDName())->GetBuffer());
}

// get commuted scalar comparision operator
CScalarCmp* CScalarCmp::PopCommutedOp(CMemoryPool *mp, COperator *pop)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *mdid = PmdidCommuteOp(md_accessor, pop);
	if (NULL != mdid && mdid->IsValid())
	{
		return GPOS_NEW(mp) CScalarCmp(mp, mdid, Pstr(mp, md_accessor, mdid), CUtils::ParseCmpType(mdid));
	}
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream & CScalarCmp::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << Pstr()->GetBuffer();
	os << ")";
	return os;
}