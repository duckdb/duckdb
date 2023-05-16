//---------------------------------------------------------------------------
//	@filename:
//		CScalarArrayCmp.cpp
//
//	@doc:
//		Implementation of scalar array comparison operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarArrayCmp.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

const CHAR CScalarArrayCmp::m_rgszCmpType[EarrcmpSentinel][10] = {"Any", "All"};

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::CScalarArrayCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArrayCmp::CScalarArrayCmp(CMemoryPool *mp, IMDId *mdid_op,
								 const CWStringConst *pstrOp,
								 EArrCmpType earrcmpt)
	: CScalar(mp),
	  m_mdid_op(mdid_op),
	  m_pscOp(pstrOp),
	  m_earrccmpt(earrcmpt),
	  m_returns_null_on_null_input(false)
{
	GPOS_ASSERT(mdid_op->IsValid());
	GPOS_ASSERT(EarrcmpSentinel > earrcmpt);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	m_returns_null_on_null_input =
		CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(md_accessor,
														  m_mdid_op);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::GetMDName
//
//	@doc:
//		Comparison operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarArrayCmp::Pstr() const
{
	return m_pscOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::MdIdOp
//
//	@doc:
//		Comparison operator mdid
//
//---------------------------------------------------------------------------
IMDId *
CScalarArrayCmp::MdIdOp() const
{
	return m_mdid_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG
CScalarArrayCmp::HashValue() const
{
	return gpos::CombineHashes(
		gpos::CombineHashes(COperator::HashValue(), m_mdid_op->HashValue()),
		m_earrccmpt);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayCmp::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarArrayCmp *popCmp = CScalarArrayCmp::PopConvert(pop);

		// match if operator oid are identical
		return m_earrccmpt == popCmp->Earrcmpt() &&
			   m_mdid_op->Equals(popCmp->MdIdOp());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarArrayCmp::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarArrayCmp::Eber(ULongPtrArray *pdrgpulChildren) const
{
	if (m_returns_null_on_null_input)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}

	return EberAny;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarArrayCmp::OsPrint(IOstream &os) const
{
	os << SzId() << " " << m_rgszCmpType[m_earrccmpt] << " (";
	os << Pstr()->GetBuffer();
	os << ")";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::PexprExpand
//
//	@doc:
//		Expand array comparison expression into a conjunctive/disjunctive
//		expression
//
//---------------------------------------------------------------------------
CExpression *
CScalarArrayCmp::PexprExpand(CMemoryPool *mp, CExpression *pexprArrayCmp)
{
	GPOS_ASSERT(NULL != pexprArrayCmp);
	GPOS_ASSERT(EopScalarArrayCmp == pexprArrayCmp->Pop()->Eopid());

	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	ULONG array_expansion_threshold =
		optimizer_config->GetHint()->UlArrayExpansionThreshold();
	CExpression *pexprIdent = (*pexprArrayCmp)[0];
	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexprArrayCmp);
	CScalarArrayCmp *popArrayCmp =
		CScalarArrayCmp::PopConvert(pexprArrayCmp->Pop());
	ULONG ulArrayElems = 0;

	if (CUtils::FScalarArray(pexprArray))
	{
		ulArrayElems = CUtils::UlScalarArrayArity(pexprArray);
	}

	// if this condition is true, we know the right child of ArrayCmp is a constant.
	if (0 == ulArrayElems || ulArrayElems > array_expansion_threshold)
	{
		// if right child is not an actual array (e.g., Const of type array), return input
		// expression without expansion
		pexprArrayCmp->AddRef();
		return pexprArrayCmp;
	}

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulArrayElems; ul++)
	{
		CExpression *pexprArrayElem =
			CUtils::PScalarArrayExprChildAt(mp, pexprArray, ul);
		pexprIdent->AddRef();
		const CWStringConst *str_opname = popArrayCmp->Pstr();
		IMDId *mdid_op = popArrayCmp->MdIdOp();
		GPOS_ASSERT(IMDId::IsValid(mdid_op));

		mdid_op->AddRef();

		CExpression *pexprCmp = CUtils::PexprScalarCmp(
			mp, pexprIdent, pexprArrayElem, *str_opname, mdid_op);
		pdrgpexpr->Append(pexprCmp);
	}
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	// deduplicate resulting array
	CExpressionArray *pdrgpexprDeduped = CUtils::PdrgpexprDedup(mp, pdrgpexpr);
	pdrgpexpr->Release();

	EArrCmpType earrcmpt = popArrayCmp->Earrcmpt();
	if (EarrcmpAny == earrcmpt)
	{
		return CPredicateUtils::PexprDisjunction(mp, pdrgpexprDeduped);
	}
	GPOS_ASSERT(EarrcmpAll == earrcmpt);

	return CPredicateUtils::PexprConjunction(mp, pdrgpexprDeduped);
}