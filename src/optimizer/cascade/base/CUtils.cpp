//---------------------------------------------------------------------------
//	@filename:
//		CUtils.cpp
//
//	@doc:
//		Implementation of general utility functions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/io/CFileDescriptor.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionPreprocessor.h"
#include "duckdb/optimizer/cascade/operators/CLogicalCTEConsumer.h"
#include "duckdb/optimizer/cascade/operators/CLogicalCTEProducer.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionRandom.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/base/IDatumInt2.h"
#include "duckdb/optimizer/cascade/base/IDatumInt4.h"
#include "duckdb/optimizer/cascade/base/IDatumInt8.h"
#include "duckdb/optimizer/cascade/base/IDatumOid.h"
#include "duckdb/optimizer/cascade/base/IDatumBool.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/IMDAggregate.h"
#include "duckdb/optimizer/cascade/md/IMDCast.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"
#include "duckdb/optimizer/cascade/md/IMDTypeInt4.h"
#include "duckdb/optimizer/cascade/md/IMDTypeInt8.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;
using namespace gpmd;

#ifdef GPOS_DEBUG

// buffer of 16M characters for print wrapper
const ULONG ulBufferCapacity = 16 * 1024 * 1024;
static WCHAR wszBuffer[ulBufferCapacity];

// global wrapper for debug print of expression
void
PrintExpr(void *pv)
{
	gpopt::CUtils::PrintExpression((gpopt::CExpression *) pv);
}

// debug print expression
void
CUtils::PrintExpression(CExpression *pexpr)
{
	CWStringStatic str(wszBuffer, ulBufferCapacity);
	COstreamString oss(&str);

	if (NULL == pexpr)
	{
		oss << std::endl << "(null)" << std::endl;
	}
	else
	{
		oss << std::endl << pexpr << std::endl << *pexpr << std::endl;
	}

	GPOS_TRACE(str.GetBuffer());
	str.Reset();
}

// global wrapper for debug print of memo
void
PrintMemo(void *pv)
{
	gpopt::CUtils::PrintMemo((gpopt::CMemo *) pv);
}

// debug print Memo structure
void
CUtils::PrintMemo(CMemo *pmemo)
{
	CWStringStatic str(wszBuffer, ulBufferCapacity);
	COstreamString oss(&str);

	if (NULL == pmemo)
	{
		oss << std::endl << "(null)" << std::endl;
	}
	else
	{
		oss << std::endl << pmemo << std::endl;
		(void) pmemo->OsPrint(oss);
		oss << std::endl;
	}

	GPOS_TRACE(str.GetBuffer());
	str.Reset();
}

#endif	// GPOS_DEBUG

// helper function to print a column descriptor array
IOstream &
CUtils::OsPrintDrgPcoldesc(IOstream &os, CColumnDescriptorArray *pdrgpcoldesc,
						   ULONG ulLengthMax)
{
	ULONG length = std::min(pdrgpcoldesc->Size(), ulLengthMax);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->OsPrint(os);

		// separator
		os << (ul == length - 1 ? "" : ", ");
	}

	return os;
}

// generate a ScalarIdent expression
CExpression *
CUtils::PexprScalarIdent(CMemoryPool *mp, const CColRef *colref)
{
	GPOS_ASSERT(NULL != colref);

	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
}

// generate a ScalarProjectElement expression
CExpression *
CUtils::PexprScalarProjectElement(CMemoryPool *mp, CColRef *colref,
								  CExpression *pexpr)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != pexpr);

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectElement(mp, colref), pexpr);
}

// generate a comparison expression over two columns
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   const CColRef *pcrRight, CWStringConst strOp,
					   IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pcrRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		IMDId *left_mdid = pcrLeft->RetrieveType()->MDId();
		IMDId *right_mdid = pcrRight->RetrieveType()->MDId();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			CExpression *pexprScCmp =
				PexprScalarCmp(mp, pcrLeft, pcrRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			ParseCmpType(mdid_op)),
		PexprScalarIdent(mp, pcrLeft), PexprScalarIdent(mp, pcrRight));
}

// Generate a comparison expression between a column and an expression
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   CExpression *pexprRight, CWStringConst strOp,
					   IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pexprRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		IMDId *left_mdid = pcrLeft->RetrieveType()->MDId();
		IMDId *right_mdid = CScalar::PopConvert(pexprRight->Pop())->MdidType();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			CExpression *pexprScCmp =
				PexprScalarCmp(mp, pcrLeft, pexprRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			ParseCmpType(mdid_op)),
		PexprScalarIdent(mp, pcrLeft), pexprRight);
}

// Generate a comparison expression between two columns
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   const CColRef *pcrRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pcrRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CExpression *pexprLeft = PexprScalarIdent(mp, pcrLeft);
	CExpression *pexprRight = PexprScalarIdent(mp, pcrRight);

	return PexprScalarCmp(mp, pexprLeft, pexprRight, cmp_type);
}

// Generate a comparison expression over a column and an expression
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   CExpression *pexprRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pexprRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CExpression *pexprLeft = PexprScalarIdent(mp, pcrLeft);

	return PexprScalarCmp(mp, pexprLeft, pexprRight, cmp_type);
}

// Generate a comparison expression between an expression and a column
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   const CColRef *pcrRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pcrRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CExpression *pexprRight = PexprScalarIdent(mp, pcrRight);

	return PexprScalarCmp(mp, pexprLeft, pexprRight, cmp_type);
}

// Generate a comparison expression over an expression and a column
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   const CColRef *pcrRight, CWStringConst strOp,
					   IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pcrRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		IMDId *left_mdid = CScalar::PopConvert(pexprLeft->Pop())->MdidType();
		IMDId *right_mdid = pcrRight->RetrieveType()->MDId();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			CExpression *pexprScCmp =
				PexprScalarCmp(mp, pexprLeft, pcrRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			cmp_type),
		pexprLeft, PexprScalarIdent(mp, pcrRight));
}

// Generate a comparison expression over two expressions
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   CExpression *pexprRight, CWStringConst strOp,
					   IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		IMDId *left_mdid = CScalar::PopConvert(pexprLeft->Pop())->MdidType();
		IMDId *right_mdid = CScalar::PopConvert(pexprRight->Pop())->MdidType();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			CExpression *pexprScCmp =
				PexprScalarCmp(mp, pexprLeft, pexprRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			cmp_type),
		pexprLeft, pexprRight);
}

// Generate a comparison expression over two expressions
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	GPOS_ASSERT(NULL != mdid_scop);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpression *pexprNewLeft = pexprLeft;
	CExpression *pexprNewRight = pexprRight;

	GPOS_ASSERT(pexprNewLeft != NULL);
	GPOS_ASSERT(pexprNewRight != NULL);

	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, mdid_scop);

	mdid_scop->AddRef();
	const IMDScalarOp *op = md_accessor->RetrieveScOp(mdid_scop);
	const CMDName mdname = op->Mdname();
	CWStringConst strCmpOpName(mdname.GetMDName()->GetBuffer());

	CExpression *pexprResult = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CScalarCmp(mp, mdid_scop,
					   GPOS_NEW(mp) CWStringConst(mp, strCmpOpName.GetBuffer()),
					   op->ParseCmpType()),
		pexprNewLeft, pexprNewRight);

	return pexprResult;
}

// Generate a comparison expression over two expressions
CExpression *
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   CExpression *pexprRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpression *pexprNewLeft = pexprLeft;
	CExpression *pexprNewRight = pexprRight;

	IMDId *op_mdid = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(
		md_accessor, pexprNewLeft, pexprNewRight, cmp_type);
	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, op_mdid);

	GPOS_ASSERT(pexprNewLeft != NULL);
	GPOS_ASSERT(pexprNewRight != NULL);

	op_mdid->AddRef();
	const IMDScalarOp *op = md_accessor->RetrieveScOp(op_mdid);
	const CMDName mdname = op->Mdname();
	CWStringConst strCmpOpName(mdname.GetMDName()->GetBuffer());

	CExpression *pexprResult = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, op_mdid,
			GPOS_NEW(mp) CWStringConst(mp, strCmpOpName.GetBuffer()), cmp_type),
		pexprNewLeft, pexprNewRight);

	return pexprResult;
}

// Generate an equality comparison expression over two columns
CExpression *
CUtils::PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft,
						 const CColRef *pcrRight)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pcrRight);

	return PexprScalarCmp(mp, pcrLeft, pcrRight, IMDType::EcmptEq);
}

// Generate an equality comparison expression over two expressions
CExpression *
CUtils::PexprScalarEqCmp(CMemoryPool *mp, CExpression *pexprLeft,
						 CExpression *pexprRight)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return PexprScalarCmp(mp, pexprLeft, pexprRight, IMDType::EcmptEq);
}

// Generate an equality comparison expression over a column reference and an expression
CExpression *
CUtils::PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft,
						 CExpression *pexprRight)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return PexprScalarCmp(mp, pcrLeft, pexprRight, IMDType::EcmptEq);
}

// Generate an equality comparison expression over an expression and a column reference
CExpression *
CUtils::PexprScalarEqCmp(CMemoryPool *mp, CExpression *pexprLeft,
						 const CColRef *pcrRight)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pcrRight);

	return PexprScalarCmp(mp, pexprLeft, pcrRight, IMDType::EcmptEq);
}

// returns number of children or constants of it is all constants
ULONG CUtils::UlScalarArrayArity(CExpression *pexprArray)
{
	GPOS_ASSERT(FScalarArray(pexprArray));

	ULONG arity = pexprArray->Arity();
	if (0 == arity)
	{
		CScalarArray *popScalarArray = CScalarArray::PopConvert(pexprArray->Pop());
		CScalarConstArray *pdrgPconst = popScalarArray->PdrgPconst();
		arity = pdrgPconst->Size();
	}
	return arity;
}

// returns constant operator of a scalar array expression
CScalarConst *
CUtils::PScalarArrayConstChildAt(CExpression *pexprArray, ULONG ul)
{
	// if the CScalarArray is already collapsed and the consts are stored in the
	// operator itself, we return the constant from the const array.
	if (FScalarArrayCollapsed(pexprArray))
	{
		CScalarArray *popScalarArray =
			CScalarArray::PopConvert(pexprArray->Pop());
		CScalarConstArray *pdrgPconst = popScalarArray->PdrgPconst();
		CScalarConst *pScalarConst = (*pdrgPconst)[ul];
		return pScalarConst;
	}
	// otherwise, we convert the child expression's operator if that's possible.
	else
	{
		return CScalarConst::PopConvert((*pexprArray)[ul]->Pop());
	}
}

// returns constant expression of a scalar array expression
CExpression *
CUtils::PScalarArrayExprChildAt(CMemoryPool *mp, CExpression *pexprArray,
								ULONG ul)
{
	ULONG arity = pexprArray->Arity();
	if (0 == arity)
	{
		CScalarArray *popScalarArray =
			CScalarArray::PopConvert(pexprArray->Pop());
		CScalarConstArray *pdrgPconst = popScalarArray->PdrgPconst();
		CScalarConst *pScalarConst = (*pdrgPconst)[ul];
		pScalarConst->AddRef();
		return GPOS_NEW(mp) CExpression(mp, pScalarConst);
	}
	else
	{
		CExpression *pexprConst = (*pexprArray)[ul];
		pexprConst->AddRef();
		return pexprConst;
	}
}

// returns the scalar array expression child of CScalarArrayComp
CExpression *
CUtils::PexprScalarArrayChild(CExpression *pexprScalarArrayCmp)
{
	CExpression *pexprArray = (*pexprScalarArrayCmp)[1];
	if (FScalarArrayCoerce(pexprArray))
	{
		pexprArray = (*pexprArray)[0];
	}
	return pexprArray;
}

// returns if the scalar array has all constant elements or ScalarIdents
BOOL
CUtils::FScalarConstAndScalarIdentArray(CExpression *pexprArray)
{
	for (ULONG i = 0; i < pexprArray->Arity(); ++i)
	{
		CExpression *pexprChild = (*pexprArray)[i];

		if (!FScalarIdent(pexprChild) && !FScalarConst(pexprChild) &&
			!(CCastUtils::FScalarCast(pexprChild) &&
			  FScalarIdent((*pexprChild)[0])))
		{
			return false;
		}
	}

	return true;
}

// returns if the scalar array has all constant elements or children
BOOL
CUtils::FScalarConstArray(CExpression *pexprArray)
{
	const ULONG arity = pexprArray->Arity();

	BOOL fAllConsts = FScalarArray(pexprArray);
	for (ULONG ul = 0; fAllConsts && ul < arity; ul++)
	{
		fAllConsts = CUtils::FScalarConst((*pexprArray)[ul]);
	}

	return fAllConsts;
}

// returns if the scalar constant array has already been collapased
BOOL
CUtils::FScalarArrayCollapsed(CExpression *pexprArray)
{
	const ULONG ulExprArity = pexprArray->Arity();
	const ULONG ulConstArity = UlScalarArrayArity(pexprArray);

	return ulExprArity == 0 && ulConstArity > 0;
}

// If it's a scalar array of all CScalarConst, collapse it into a single
// expression but keep the constants in the operator.
CExpression *
CUtils::PexprCollapseConstArray(CMemoryPool *mp, CExpression *pexprArray)
{
	GPOS_ASSERT(NULL != pexprArray);

	const ULONG arity = pexprArray->Arity();

	// do not collapse already collapsed array, otherwise we lose the
	// collapsed constants.
	if (FScalarConstArray(pexprArray) && !FScalarArrayCollapsed(pexprArray))
	{
		CScalarConstArray *pdrgPconst = GPOS_NEW(mp) CScalarConstArray(mp);
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CScalarConst *popConst =
				CScalarConst::PopConvert((*pexprArray)[ul]->Pop());
			popConst->AddRef();
			pdrgPconst->Append(popConst);
		}

		CScalarArray *psArray = CScalarArray::PopConvert(pexprArray->Pop());
		IMDId *elem_type_mdid = psArray->PmdidElem();
		IMDId *array_type_mdid = psArray->PmdidArray();
		elem_type_mdid->AddRef();
		array_type_mdid->AddRef();

		CScalarArray *pConstArray =
			GPOS_NEW(mp) CScalarArray(mp, elem_type_mdid, array_type_mdid,
									  psArray->FMultiDimensional(), pdrgPconst);
		return GPOS_NEW(mp) CExpression(mp, pConstArray);
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild =
			PexprCollapseConstArray(mp, (*pexprArray)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pexprArray->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// generate an ArrayCmp expression given an array of CScalarConst's
CExpression *
CUtils::PexprScalarArrayCmp(CMemoryPool *mp,
							CScalarArrayCmp::EArrCmpType earrcmptype,
							IMDType::ECmpType ecmptype,
							CExpressionArray *pexprScalarChildren,
							const CColRef *colref)
{
	GPOS_ASSERT(pexprScalarChildren != NULL);
	GPOS_ASSERT(0 < pexprScalarChildren->Size());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// get column type mdid and mdid of the array type corresponding to that type
	IMDId *pmdidColType = colref->RetrieveType()->MDId();
	IMDId *pmdidArrType = colref->RetrieveType()->GetArrayTypeMdid();
	IMDId *pmdidCmpOp = colref->RetrieveType()->GetMdidForCmpType(ecmptype);

	pmdidColType->AddRef();
	pmdidArrType->AddRef();
	pmdidCmpOp->AddRef();

	const CMDName mdname = md_accessor->RetrieveScOp(pmdidCmpOp)->Mdname();
	CWStringConst strOp(mdname.GetMDName()->GetBuffer());

	CExpression *pexprArray = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CScalarArray(mp, pmdidColType, pmdidArrType,
											  false /*is_multidimenstional*/),
					pexprScalarChildren);

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarArrayCmp(
			mp, pmdidCmpOp, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			earrcmptype),
		CUtils::PexprScalarIdent(mp, colref), pexprArray);
}

// generate a comparison against Zero
CExpression* CUtils::PexprCmpWithZero(CMemoryPool *mp, CExpression *pexprLeft, IMDId *mdid_type_left, IMDType::ECmpType ecmptype)
{
	GPOS_ASSERT(pexprLeft->Pop()->FScalar());
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *pmdtype = md_accessor->RetrieveType(mdid_type_left);
	GPOS_ASSERT(pmdtype->GetDatumType() == IMDType::EtiInt8 && "left expression must be of type int8");
	IMDId *mdid_op = pmdtype->GetMdidForCmpType(ecmptype);
	mdid_op->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(mdid_op)->Mdname();
	CWStringConst strOpName(mdname.GetMDName()->GetBuffer());
	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarCmp(mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOpName.GetBuffer()), ecmptype), pexprLeft, CUtils::PexprScalarConstInt8(mp, 0));
}

// generate an Is Distinct From expression
CExpression* CUtils::PexprIDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CExpression *pexprNewLeft = pexprLeft;
	CExpression *pexprNewRight = pexprRight;
	IMDId *pmdidEqOp = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(md_accessor, pexprNewLeft, pexprNewRight, IMDType::EcmptEq);
	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft, pexprNewRight, pmdidEqOp);
	pmdidEqOp->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(pmdidEqOp)->Mdname();
	CWStringConst strEqOpName(mdname.GetMDName()->GetBuffer());
	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIsDistinctFrom(mp, pmdidEqOp, GPOS_NEW(mp) CWStringConst(mp, strEqOpName.GetBuffer())), pexprNewLeft, pexprNewRight);
}

// generate an Is Distinct From expression
CExpression* CUtils::PexprIDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CExpression *pexprNewLeft = pexprLeft;
	CExpression *pexprNewRight = pexprRight;
	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft, pexprNewRight, mdid_scop);
	mdid_scop->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(mdid_scop)->Mdname();
	CWStringConst strEqOpName(mdname.GetMDName()->GetBuffer());
	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIsDistinctFrom(mp, mdid_scop, GPOS_NEW(mp) CWStringConst(mp, strEqOpName.GetBuffer())), pexprNewLeft, pexprNewRight);
}

// generate an Is NOT Distinct From expression for two columns; the two columns must have the same type
CExpression *
CUtils::PexprINDF(CMemoryPool *mp, const CColRef *pcrLeft,
				  const CColRef *pcrRight)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pcrRight);

	return PexprINDF(mp, PexprScalarIdent(mp, pcrLeft),
					 PexprScalarIdent(mp, pcrRight));
}

// Generate an Is NOT Distinct From expression for scalar expressions;
// the two expressions must have the same type
CExpression *
CUtils::PexprINDF(CMemoryPool *mp, CExpression *pexprLeft,
				  CExpression *pexprRight)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return PexprNegate(mp, PexprIDF(mp, pexprLeft, pexprRight));
}

CExpression *
CUtils::PexprINDF(CMemoryPool *mp, CExpression *pexprLeft,
				  CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return PexprNegate(mp, PexprIDF(mp, pexprLeft, pexprRight, mdid_scop));
}

// Generate an Is Null expression
CExpression* CUtils::PexprIsNull(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarNullTest(mp), pexpr);
}

// Generate an Is Not Null expression
CExpression* CUtils::PexprIsNotNull(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	return PexprNegate(mp, PexprIsNull(mp, pexpr));
}

// Generate an Is Not False expression
CExpression *
CUtils::PexprIsNotFalse(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	return PexprIDF(mp, pexpr, PexprScalarConstBool(mp, false /*value*/));
}

// Find if a scalar expression uses a nullable column from the
// output of a logical expression
BOOL
CUtils::FUsesNullableCol(CMemoryPool *mp, CExpression *pexprScalar,
						 CExpression *pexprLogical)
{
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(pexprLogical->Pop()->FLogical());

	CColRefSet *pcrsNotNull = pexprLogical->DeriveNotNullColumns();
	CColRefSet *pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
	pcrsUsed->Include(pexprScalar->DeriveUsedColumns());
	pcrsUsed->Intersection(pexprLogical->DeriveOutputColumns());

	BOOL fUsesNullableCol = !pcrsNotNull->ContainsAll(pcrsUsed);
	pcrsUsed->Release();

	return fUsesNullableCol;
}

// Extract the partition key at the given level from the given array of partition keys
CColRef *
CUtils::PcrExtractPartKey(CColRef2dArray *pdrgpdrgpcr, ULONG ulLevel)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	GPOS_ASSERT(ulLevel < pdrgpdrgpcr->Size());

	CColRefArray *pdrgpcrPartKey = (*pdrgpdrgpcr)[ulLevel];
	GPOS_ASSERT(1 == pdrgpcrPartKey->Size() &&
				"Composite keys not currently supported");

	return (*pdrgpcrPartKey)[0];
}

// check for existence of subqueries
BOOL
CUtils::FHasSubquery(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	return pexpr->DeriveHasSubquery();
}

// check for existence of CTE anchor
BOOL
CUtils::FHasCTEAnchor(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopLogicalCTEAnchor == pexpr->Pop()->Eopid())
	{
		return true;
	}

	for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (FHasCTEAnchor(pexprChild))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@class:
//		CUtils::FHasSubqueryOrApply
//
//	@doc:
//		Check existence of subqueries or Apply operators in deep expression tree
//
//---------------------------------------------------------------------------
BOOL
CUtils::FHasSubqueryOrApply(CExpression *pexpr, BOOL fCheckRoot)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (fCheckRoot)
	{
		COperator *pop = pexpr->Pop();
		if (FApply(pop) && !FCorrelatedApply(pop))
		{
			return true;
		}

		if (pop->FScalar() && pexpr->DeriveHasSubquery())
		{
			return true;
		}
	}

	const ULONG arity = pexpr->Arity();
	BOOL fSubqueryOrApply = false;
	for (ULONG ul = 0; !fSubqueryOrApply && ul < arity; ul++)
	{
		fSubqueryOrApply = FHasSubqueryOrApply((*pexpr)[ul]);
	}

	return fSubqueryOrApply;
}

//---------------------------------------------------------------------------
//	@class:
//		CUtils::FHasCorrelatedApply
//
//	@doc:
//		Check existence of Correlated Apply operators in deep expression tree
//
//---------------------------------------------------------------------------
BOOL
CUtils::FHasCorrelatedApply(CExpression *pexpr, BOOL fCheckRoot)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (fCheckRoot && FCorrelatedApply(pexpr->Pop()))
	{
		return true;
	}

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasCorrelatedApply((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// check for existence of outer references
BOOL
CUtils::HasOuterRefs(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	return 0 < pexpr->DeriveOuterReferences()->Size();
}

// check if a given operator is a logical join
BOOL
CUtils::FLogicalJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CLogicalJoin *popJoin = NULL;
	if (pop->FLogical())
	{
		// attempt casting to logical join,
		// dynamic cast returns NULL if operator is not join
		popJoin = dynamic_cast<CLogicalJoin *>(pop);
	}

	return (NULL != popJoin);
}

// check if a given operator is a logical set operation
BOOL
CUtils::FLogicalSetOp(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CLogicalSetOp *popSetOp = NULL;
	if (pop->FLogical())
	{
		// attempt casting to logical SetOp,
		// dynamic cast returns NULL if operator is not SetOp
		popSetOp = dynamic_cast<CLogicalSetOp *>(pop);
	}

	return (NULL != popSetOp);
}

// check if a given operator is a logical unary operator
BOOL
CUtils::FLogicalUnary(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CLogicalUnary *popUnary = NULL;
	if (pop->FLogical())
	{
		// attempt casting to logical unary,
		// dynamic cast returns NULL if operator is not unary
		popUnary = dynamic_cast<CLogicalUnary *>(pop);
	}

	return (NULL != popUnary);
}

// check if a given operator is a hash join
BOOL
CUtils::FHashJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalHashJoin *popHJN = NULL;
	if (pop->FPhysical())
	{
		popHJN = dynamic_cast<CPhysicalHashJoin *>(pop);
	}

	return (NULL != popHJN);
}

// check if a given operator is a correlated nested loops join
BOOL CUtils::FCorrelatedNLJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	BOOL fCorrelatedNLJ = false;
	if (FNLJoin(pop))
	{
		fCorrelatedNLJ = dynamic_cast<CPhysicalNLJoin *>(pop)->FCorrelated();
	}

	return fCorrelatedNLJ;
}

// check if a given operator is a nested loops join
BOOL CUtils::FNLJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalNLJoin *popNLJ = NULL;
	if (pop->FPhysical())
	{
		popNLJ = dynamic_cast<CPhysicalNLJoin *>(pop);
	}

	return (NULL != popNLJ);
}

// check if a given operator is a logical join
BOOL
CUtils::FPhysicalJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	return FHashJoin(pop) || FNLJoin(pop);
}

// check if a given operator is a physical outer join
BOOL
CUtils::FPhysicalOuterJoin(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	return COperator::EopPhysicalLeftOuterNLJoin == pop->Eopid() ||
		   COperator::EopPhysicalLeftOuterIndexNLJoin == pop->Eopid() ||
		   COperator::EopPhysicalLeftOuterHashJoin == pop->Eopid() ||
		   COperator::EopPhysicalCorrelatedLeftOuterNLJoin == pop->Eopid();
}

// check if a given operator is a physical agg
BOOL
CUtils::FPhysicalScan(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalScan *popScan = NULL;
	if (pop->FPhysical())
	{
		// attempt casting to physical scan,
		// dynamic cast returns NULL if operator is not a scan
		popScan = dynamic_cast<CPhysicalScan *>(pop);
	}

	return (NULL != popScan);
}

// check if a given operator is a physical agg
BOOL
CUtils::FPhysicalAgg(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalAgg *popAgg = NULL;
	if (pop->FPhysical())
	{
		// attempt casting to physical agg,
		// dynamic cast returns NULL if operator is not an agg
		popAgg = dynamic_cast<CPhysicalAgg *>(pop);
	}

	return (NULL != popAgg);
}

// check if a given operator is a physical motion
BOOL
CUtils::FPhysicalMotion(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalMotion *popMotion = NULL;
	if (pop->FPhysical())
	{
		// attempt casting to physical motion,
		// dynamic cast returns NULL if operator is not a motion
		popMotion = dynamic_cast<CPhysicalMotion *>(pop);
	}

	return (NULL != popMotion);
}

// check if a given operator is an FEnforcer
BOOL CUtils::FEnforcer(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return COperator::EopPhysicalSort == op_id ||
		   COperator::EopPhysicalSpool == op_id ||
		   COperator::EopPhysicalPartitionSelector == op_id ||
		   FPhysicalMotion(pop);
}

// check if a given operator is a subquery
BOOL
CUtils::FSubquery(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubquery == op_id ||
							  COperator::EopScalarSubqueryExists == op_id ||
							  COperator::EopScalarSubqueryNotExists == op_id ||
							  COperator::EopScalarSubqueryAll == op_id ||
							  COperator::EopScalarSubqueryAny == op_id);
}

// check if a given operator is existential subquery
BOOL
CUtils::FExistentialSubquery(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubqueryExists == op_id ||
							  COperator::EopScalarSubqueryNotExists == op_id);
}

// check if a given operator is quantified subquery
BOOL
CUtils::FQuantifiedSubquery(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubqueryAll == op_id ||
							  COperator::EopScalarSubqueryAny == op_id);
}

// check if given expression is a Project on ConstTable with one
// scalar subquery in Project List
BOOL CUtils::FProjectConstTableWithOneScalarSubq(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	if (COperator::EopLogicalProject == pexpr->Pop()->Eopid() && COperator::EopLogicalConstTableGet == (*pexpr)[0]->Pop()->Eopid())
	{
		CExpression *pexprScalar = (*pexpr)[1];
		GPOS_ASSERT(COperator::EopScalarProjectList == pexprScalar->Pop()->Eopid());
		if (1 == pexprScalar->Arity() && FProjElemWithScalarSubq((*pexprScalar)[0]))
		{
			return true;
		}
	}
	return false;
}

BOOL CUtils::FApply(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	CLogicalApply *popApply = NULL;
	if (pop->FLogical())
	{
		// attempt casting to logical apply,
		// dynamic cast returns NULL if operator is not an Apply operator
		popApply = dynamic_cast<CLogicalApply *>(pop);
	}

	return (NULL != popApply);
}

// check if a given operator is a correlated Apply
BOOL CUtils::FCorrelatedApply(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	BOOL fCorrelatedApply = false;
	if (FApply(pop))
	{
		fCorrelatedApply = CLogicalApply::PopConvert(pop)->FCorrelated();
	}
	return fCorrelatedApply;
}

// check if a given operator is left semi apply
BOOL CUtils::FLeftSemiApply(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	BOOL fLeftSemiApply = false;
	if (FApply(pop))
	{
		fLeftSemiApply = CLogicalApply::PopConvert(pop)->FLeftSemiApply();
	}
	return fLeftSemiApply;
}

// check if a given operator is left anti semi apply
BOOL CUtils::FLeftAntiSemiApply(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	BOOL fLeftAntiSemiApply = false;
	if (FApply(pop))
	{
		fLeftAntiSemiApply =
			CLogicalApply::PopConvert(pop)->FLeftAntiSemiApply();
	}

	return fLeftAntiSemiApply;
}

// check if given expression is a Project Element with scalar subquery
BOOL CUtils::FProjElemWithScalarSubq(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	return (COperator::EopScalarProjectElement == pexpr->Pop()->Eopid() && COperator::EopScalarSubquery == (*pexpr)[0]->Pop()->Eopid());
}

// check if given expression is a scalar subquery with a ConstTableGet as the only child
BOOL CUtils::FScalarSubqWithConstTblGet(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	if (COperator::EopScalarSubquery == pexpr->Pop()->Eopid() && COperator::EopLogicalConstTableGet == (*pexpr)[0]->Pop()->Eopid() && 1 == pexpr->Arity())
	{
		return true;
	}
	return false;
}

// check if a limit expression has 0 offset
BOOL CUtils::FHasZeroOffset(CExpression *pexpr)
{
	GPOS_ASSERT(COperator::EopLogicalLimit == pexpr->Pop()->Eopid() || COperator::EopPhysicalLimit == pexpr->Pop()->Eopid());
	return FScalarConstIntZero((*pexpr)[1]);
}

// check if an expression is a 0 integer
BOOL
CUtils::FScalarConstIntZero(CExpression *pexprOffset)
{
	if (COperator::EopScalarConst != pexprOffset->Pop()->Eopid())
	{
		return false;
	}

	CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprOffset->Pop());
	IDatum *datum = popScalarConst->GetDatum();

	switch (datum->GetDatumType())
	{
		case IMDType::EtiInt2:
			return (0 == dynamic_cast<IDatumInt2 *>(datum)->Value());
		case IMDType::EtiInt4:
			return (0 == dynamic_cast<IDatumInt4 *>(datum)->Value());
		case IMDType::EtiInt8:
			return (0 == dynamic_cast<IDatumInt8 *>(datum)->Value());
		default:
			return false;
	}
}

// deduplicate an array of expressions
CExpressionArray *
CUtils::PdrgpexprDedup(CMemoryPool *mp, CExpressionArray *pdrgpexpr)
{
	const ULONG size = pdrgpexpr->Size();
	CExpressionArray *pdrgpexprDedup = GPOS_NEW(mp) CExpressionArray(mp);
	ExprHashSet *phsexpr = GPOS_NEW(mp) ExprHashSet(mp);

	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		pexpr->AddRef();
		if (phsexpr->Insert(pexpr))
		{
			pexpr->AddRef();
			pdrgpexprDedup->Append(pexpr);
		}
		else
		{
			pexpr->Release();
		}
	}

	phsexpr->Release();
	return pdrgpexprDedup;
}

// deep equality of expression arrays
BOOL
CUtils::Equals(const CExpressionArray *pdrgpexprLeft,
			   const CExpressionArray *pdrgpexprRight)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL arrays are equal
	if (NULL == pdrgpexprLeft || NULL == pdrgpexprRight)
	{
		return NULL == pdrgpexprLeft && NULL == pdrgpexprRight;
	}

	// start with pointers comparison
	if (pdrgpexprLeft == pdrgpexprRight)
	{
		return true;
	}

	const ULONG length = pdrgpexprLeft->Size();
	BOOL fEqual = (length == pdrgpexprRight->Size());

	for (ULONG ul = 0; ul < length && fEqual; ul++)
	{
		const CExpression *pexprLeft = (*pdrgpexprLeft)[ul];
		const CExpression *pexprRight = (*pdrgpexprRight)[ul];
		fEqual = Equals(pexprLeft, pexprRight);
	}

	return fEqual;
}

// deep equality of expression trees
BOOL
CUtils::Equals(const CExpression *pexprLeft, const CExpression *pexprRight)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL expressions are equal
	if (NULL == pexprLeft || NULL == pexprRight)
	{
		return NULL == pexprLeft && NULL == pexprRight;
	}

	// start with pointers comparison
	if (pexprLeft == pexprRight)
	{
		return true;
	}

	// compare number of children and root operators
	if (pexprLeft->Arity() != pexprRight->Arity() ||
		!pexprLeft->Pop()->Matches(pexprRight->Pop()))
	{
		return false;
	}

	if (0 < pexprLeft->Arity() && pexprLeft->Pop()->FInputOrderSensitive())
	{
		return FMatchChildrenOrdered(pexprLeft, pexprRight);
	}

	return FMatchChildrenUnordered(pexprLeft, pexprRight);
}

// check if two expressions have the same children in any order
BOOL
CUtils::FMatchChildrenUnordered(const CExpression *pexprLeft,
								const CExpression *pexprRight)
{
	BOOL fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	GPOS_ASSERT(pexprRight->Arity() == arity);

	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		CExpression *pexpr = (*pexprLeft)[ul];
		fEqual = (UlOccurrences(pexpr, pexprLeft->PdrgPexpr()) ==
				  UlOccurrences(pexpr, pexprRight->PdrgPexpr()));
	}

	return fEqual;
}

// check if two expressions have the same children in the same order
BOOL
CUtils::FMatchChildrenOrdered(const CExpression *pexprLeft,
							  const CExpression *pexprRight)
{
	BOOL fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	GPOS_ASSERT(pexprRight->Arity() == arity);

	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		// child must be at the same position in the other expression
		fEqual = Equals((*pexprLeft)[ul], (*pexprRight)[ul]);
	}

	return fEqual;
}

// return the number of occurrences of the given expression in the given array of expressions
ULONG
CUtils::UlOccurrences(const CExpression *pexpr, CExpressionArray *pdrgpexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	ULONG count = 0;

	const ULONG size = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		if (Equals(pexpr, (*pdrgpexpr)[ul]))
		{
			count++;
		}
	}

	return count;
}

// compare expression against an array of expressions
BOOL
CUtils::FEqualAny(const CExpression *pexpr, const CExpressionArray *pdrgpexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	const ULONG size = pdrgpexpr->Size();
	BOOL fEqual = false;
	for (ULONG ul = 0; !fEqual && ul < size; ul++)
	{
		fEqual = Equals(pexpr, (*pdrgpexpr)[ul]);
	}

	return fEqual;
}

// check if first expression array contains all expressions in second array
BOOL
CUtils::Contains(const CExpressionArray *pdrgpexprFst,
				 const CExpressionArray *pdrgpexprSnd)
{
	GPOS_ASSERT(NULL != pdrgpexprFst);
	GPOS_ASSERT(NULL != pdrgpexprSnd);

	if (pdrgpexprFst == pdrgpexprSnd)
	{
		return true;
	}

	if (pdrgpexprFst->Size() < pdrgpexprSnd->Size())
	{
		return false;
	}

	const ULONG size = pdrgpexprSnd->Size();
	BOOL fContains = true;
	for (ULONG ul = 0; fContains && ul < size; ul++)
	{
		fContains = FEqualAny((*pdrgpexprSnd)[ul], pdrgpexprFst);
	}

	return fContains;
}

// generate a Not expression on top of the given expression
CExpression *
CUtils::PexprNegate(CMemoryPool *mp, CExpression *pexpr)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexpr->Append(pexpr);

	return PexprScalarBoolOp(mp, CScalarBoolOp::EboolopNot, pdrgpexpr);
}

// generate a ScalarOp expression over a column and an expression
CExpression *
CUtils::PexprScalarOp(CMemoryPool *mp, const CColRef *pcrLeft,
					  CExpression *pexprRight, const CWStringConst strOp,
					  IMDId *mdid_op, IMDId *return_type_mdid)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CScalarOp(mp, mdid_op, return_type_mdid,
					  GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer())),
		PexprScalarIdent(mp, pcrLeft), pexprRight);
}

// generate a ScalarBoolOp expression
CExpression *
CUtils::PexprScalarBoolOp(CMemoryPool *mp, CScalarBoolOp::EBoolOperator eboolop,
						  CExpressionArray *pdrgpexpr)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarBoolOp(mp, eboolop), pdrgpexpr);
}

// generate a boolean scalar constant expression
CExpression *
CUtils::PexprScalarConstBool(CMemoryPool *mp, BOOL fval, BOOL is_null)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// create a BOOL datum
	const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
	IDatumBool *datum = pmdtypebool->CreateBoolDatum(mp, fval, is_null);

	CExpression *pexpr = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) datum));

	return pexpr;
}

// generate an int4 scalar constant expression
CExpression* CUtils::PexprScalarConstInt4(CMemoryPool *mp, INT val)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	// create a int4 datum
	const IMDTypeInt4 *pmdtypeint4 = md_accessor->PtMDType<IMDTypeInt4>();
	IDatumInt4 *datum = pmdtypeint4->CreateInt4Datum(mp, val, false);
	CExpression *pexpr = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) datum));
	return pexpr;
}

// generate an int8 scalar constant expression
CExpression* CUtils::PexprScalarConstInt8(CMemoryPool *mp, LINT val, BOOL is_null)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	// create a int8 datum
	const IMDTypeInt8 *pmdtypeint8 = md_accessor->PtMDType<IMDTypeInt8>();
	IDatumInt8 *datum = pmdtypeint8->CreateInt8Datum(mp, val, is_null);
	CExpression *pexpr = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) datum));
	return pexpr;
}

// generate a NULL constant of a given type
CExpression* CUtils::PexprScalarConstNull(CMemoryPool *mp, const IMDType *typ, INT type_modifier)
{
	IDatum *datum = NULL;
	IMDId *mdid = typ->MDId();
	mdid->AddRef();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	switch (typ->GetDatumType())
	{
		case IMDType::EtiInt4:
		{
			const IMDTypeInt4 *pmdtypeint4 =
				md_accessor->PtMDType<IMDTypeInt4>();
			datum = pmdtypeint4->CreateInt4Datum(mp, 0, true);
		}
		break;

		case IMDType::EtiBool:
		{
			const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
			datum = pmdtypebool->CreateBoolDatum(mp, false, true);
		}
		break;

		default:
			// shouldn't come here
			GPOS_RTL_ASSERT(!"Invalid operator type");
	}

	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, datum));
}

// get column reference defined by project element
CColRef *
CUtils::PcrFromProjElem(CExpression *pexprPrEl)
{
	return (CScalarProjectElement::PopConvert(pexprPrEl->Pop()))->Pcr();
}

// generate an aggregate function operator
CScalarAggFunc *
CUtils::PopAggFunc(
	CMemoryPool *mp, IMDId *pmdidAggFunc, const CWStringConst *pstrAggFunc,
	BOOL is_distinct, EAggfuncStage eaggfuncstage, BOOL fSplit,
	IMDId *
		pmdidResolvedReturnType	 // return type to be used if original return type is ambiguous
)
{
	GPOS_ASSERT(NULL != pmdidAggFunc);
	GPOS_ASSERT(NULL != pstrAggFunc);
	GPOS_ASSERT_IMP(NULL != pmdidResolvedReturnType,
					pmdidResolvedReturnType->IsValid());

	return GPOS_NEW(mp)
		CScalarAggFunc(mp, pmdidAggFunc, pmdidResolvedReturnType, pstrAggFunc,
					   is_distinct, eaggfuncstage, fSplit);
}

// generate an aggregate function
CExpression *
CUtils::PexprAggFunc(CMemoryPool *mp, IMDId *pmdidAggFunc,
					 const CWStringConst *pstrAggFunc, const CColRef *colref,
					 BOOL is_distinct, EAggfuncStage eaggfuncstage, BOOL fSplit)
{
	GPOS_ASSERT(NULL != pstrAggFunc);
	GPOS_ASSERT(NULL != colref);

	// generate aggregate function
	CScalarAggFunc *popScAggFunc = PopAggFunc(
		mp, pmdidAggFunc, pstrAggFunc, is_distinct, eaggfuncstage, fSplit);

	// generate function arguments
	CExpression *pexprScalarIdent = PexprScalarIdent(mp, colref);
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexpr->Append(pexprScalarIdent);

	return GPOS_NEW(mp) CExpression(mp, popScAggFunc, pdrgpexpr);
}


// generate a count(*) expression
CExpression *
CUtils::PexprCountStar(CMemoryPool *mp)
{
	// TODO,  04/26/2012, create count(*) expressions in a system-independent
	// way using MDAccessor

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_COUNT_STAR);
	CWStringConst *str = GPOS_NEW(mp) CWStringConst(GPOS_WSZ_LIT("count"));

	CScalarAggFunc *popScAggFunc =
		PopAggFunc(mp, mdid, str, false /*is_distinct*/,
				   EaggfuncstageGlobal /*eaggfuncstage*/, false /*fSplit*/);

	CExpression *pexprCountStar =
		GPOS_NEW(mp) CExpression(mp, popScAggFunc, pdrgpexpr);

	return pexprCountStar;
}

// generate a GbAgg with count(*) function over the given expression
CExpression *
CUtils::PexprCountStar(CMemoryPool *mp, CExpression *pexprLogical)
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a count(*) expression
	CExpression *pexprCountStar = PexprCountStar(mp);

	// generate a computed column with count(*) type
	CScalarAggFunc *popScalarAggFunc =
		CScalarAggFunc::PopConvert(pexprCountStar->Pop());
	IMDId *mdid_type = popScalarAggFunc->MdidType();
	INT type_modifier = popScalarAggFunc->TypeModifier();
	const IMDType *pmdtype = md_accessor->RetrieveType(mdid_type);
	CColRef *pcrComputed = col_factory->PcrCreate(pmdtype, type_modifier);
	CExpression *pexprPrjElem =
		PexprScalarProjectElement(mp, pcrComputed, pexprCountStar);
	CExpression *pexprPrjList = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pexprPrjElem);

	// generate a Gb expression
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, colref_array, pexprLogical,
								   pexprPrjList);
}

// generate a GbAgg with count(*) and sum(col) over the given expression
CExpression *
CUtils::PexprCountStarAndSum(CMemoryPool *mp, const CColRef *colref,
							 CExpression *pexprLogical)
{
	GPOS_ASSERT(pexprLogical->DeriveOutputColumns()->FMember(colref));

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a count(*) expression
	CExpression *pexprCountStar = PexprCountStar(mp);

	// generate a computed column with count(*) type
	CScalarAggFunc *popScalarAggFunc =
		CScalarAggFunc::PopConvert(pexprCountStar->Pop());
	IMDId *mdid_type = popScalarAggFunc->MdidType();
	INT type_modifier = popScalarAggFunc->TypeModifier();
	const IMDType *pmdtype = md_accessor->RetrieveType(mdid_type);
	CColRef *pcrComputed = col_factory->PcrCreate(pmdtype, type_modifier);
	CExpression *pexprPrjElemCount =
		PexprScalarProjectElement(mp, pcrComputed, pexprCountStar);

	// generate sum(col) expression
	CExpression *pexprSum = PexprSum(mp, colref);
	CScalarAggFunc *popScalarSumFunc =
		CScalarAggFunc::PopConvert(pexprSum->Pop());
	const IMDType *pmdtypeSum =
		md_accessor->RetrieveType(popScalarSumFunc->MdidType());
	CColRef *pcrSum =
		col_factory->PcrCreate(pmdtypeSum, popScalarSumFunc->TypeModifier());
	CExpression *pexprPrjElemSum =
		PexprScalarProjectElement(mp, pcrSum, pexprSum);
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pexprPrjElemCount, pexprPrjElemSum);

	// generate a Gb expression
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, colref_array, pexprLogical,
								   pexprPrjList);
}

// return True if passed expression is a Project Element defined on count(*)/count(Any) agg
BOOL
CUtils::FCountAggProjElem(
	CExpression *pexprPrjElem,
	CColRef **ppcrCount	 // output: count(*)/count(Any) column
)
{
	GPOS_ASSERT(NULL != pexprPrjElem);
	GPOS_ASSERT(NULL != ppcrCount);

	COperator *pop = pexprPrjElem->Pop();
	if (COperator::EopScalarProjectElement != pop->Eopid())
	{
		return false;
	}

	if (COperator::EopScalarAggFunc == (*pexprPrjElem)[0]->Pop()->Eopid())
	{
		CScalarAggFunc *popAggFunc =
			CScalarAggFunc::PopConvert((*pexprPrjElem)[0]->Pop());
		if (popAggFunc->FCountStar() || popAggFunc->FCountAny())
		{
			*ppcrCount = CScalarProjectElement::PopConvert(pop)->Pcr();
			return true;
		}
	}

	return FHasCountAgg((*pexprPrjElem)[0], ppcrCount);
}

// check if the given expression has a count(*)/count(Any) agg, return the top-most found count column
BOOL
CUtils::FHasCountAgg(CExpression *pexpr,
					 CColRef **ppcrCount  // output: count(*)/count(Any) column
)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != ppcrCount);

	if (COperator::EopScalarProjectElement == pexpr->Pop()->Eopid())
	{
		// base case, count(*)/count(Any) must appear below a project element
		return FCountAggProjElem(pexpr, ppcrCount);
	}

	// recursively process children
	BOOL fHasCountAgg = false;
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; !fHasCountAgg && ul < arity; ul++)
	{
		fHasCountAgg = FHasCountAgg((*pexpr)[ul], ppcrCount);
	}

	return fHasCountAgg;
}


static BOOL
FCountAggMatchingColumn(CExpression *pexprPrjElem, const CColRef *colref)
{
	CColRef *pcrCount = NULL;
	return CUtils::FCountAggProjElem(pexprPrjElem, &pcrCount) &&
		   colref == pcrCount;
}


BOOL
CUtils::FHasCountAggMatchingColumn(const CExpression *pexpr,
								   const CColRef *colref,
								   const CLogicalGbAgg **ppgbAgg)
{
	COperator *pop = pexpr->Pop();
	// base case, we have a logical agg operator
	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		const CExpression *const pexprProjectList = (*pexpr)[1];
		GPOS_ASSERT(COperator::EopScalarProjectList ==
					pexprProjectList->Pop()->Eopid());
		const ULONG arity = pexprProjectList->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *const pexprPrjElem = (*pexprProjectList)[ul];
			if (FCountAggMatchingColumn(pexprPrjElem, colref))
			{
				const CLogicalGbAgg *pgbAgg = CLogicalGbAgg::PopConvert(pop);
				*ppgbAgg = pgbAgg;
				return true;
			}
		}
	}
	// recurse
	else
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			const CExpression *pexprChild = (*pexpr)[ul];
			if (FHasCountAggMatchingColumn(pexprChild, colref, ppgbAgg))
			{
				return true;
			}
		}
	}
	return false;
}

// generate a sum(col) expression
CExpression *
CUtils::PexprSum(CMemoryPool *mp, const CColRef *colref)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return PexprAgg(mp, md_accessor, IMDType::EaggSum, colref,
					false /*is_distinct*/);
}

// generate a GbAgg with sum(col) expressions for all columns in the passed array
CExpression *
CUtils::PexprGbAggSum(CMemoryPool *mp, CExpression *pexprLogical,
					  CColRefArray *pdrgpcrSum)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpcrSum->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*pdrgpcrSum)[ul];
		CExpression *pexprSum = PexprSum(mp, colref);
		CScalarAggFunc *popScalarAggFunc =
			CScalarAggFunc::PopConvert(pexprSum->Pop());
		const IMDType *pmdtypeSum =
			md_accessor->RetrieveType(popScalarAggFunc->MdidType());
		CColRef *pcrSum = col_factory->PcrCreate(
			pmdtypeSum, popScalarAggFunc->TypeModifier());
		CExpression *pexprPrjElemSum =
			PexprScalarProjectElement(mp, pcrSum, pexprSum);
		pdrgpexpr->Append(pexprPrjElemSum);
	}

	CExpression *pexprPrjList = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexpr);

	// generate a Gb expression
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, colref_array, pexprLogical,
								   pexprPrjList);
}

// generate a count(<distinct> col) expression
CExpression *
CUtils::PexprCount(CMemoryPool *mp, const CColRef *colref, BOOL is_distinct)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return PexprAgg(mp, md_accessor, IMDType::EaggCount, colref, is_distinct);
}

// generate a min(col) expression
CExpression *
CUtils::PexprMin(CMemoryPool *mp, CMDAccessor *md_accessor,
				 const CColRef *colref)
{
	return PexprAgg(mp, md_accessor, IMDType::EaggMin, colref,
					false /*is_distinct*/);
}

// generate an aggregate expression of the specified type
CExpression* CUtils::PexprAgg(CMemoryPool *mp, CMDAccessor *md_accessor, IMDType::EAggType agg_type, const CColRef *colref, BOOL is_distinct)
{
	GPOS_ASSERT(IMDType::EaggGeneric > agg_type);
	GPOS_ASSERT(colref->RetrieveType()->GetMdidForAggType(agg_type)->IsValid());

	const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(colref->RetrieveType()->GetMdidForAggType(agg_type));

	IMDId *agg_mdid = pmdagg->MDId();
	agg_mdid->AddRef();
	CWStringConst *str = GPOS_NEW(mp) CWStringConst(mp, pmdagg->Mdname().GetMDName()->GetBuffer());
	return PexprAggFunc(mp, agg_mdid, str, colref, is_distinct, EaggfuncstageGlobal /*fGlobal*/, false /*fSplit*/);
}

// generate a select expression
CExpression *
CUtils::PexprLogicalSelect(CMemoryPool *mp, CExpression *pexpr,
						   CExpression *pexprPredicate)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pexprPredicate);

	CTableDescriptor *ptabdesc = NULL;
	if (pexpr->Pop()->Eopid() == CLogical::EopLogicalSelect ||
		pexpr->Pop()->Eopid() == CLogical::EopLogicalGet ||
		pexpr->Pop()->Eopid() == CLogical::EopLogicalDynamicGet)
	{
		ptabdesc = pexpr->DeriveTableDescriptor();
		// there are some cases where we don't populate LogicalSelect currently
		GPOS_ASSERT_IMP(pexpr->Pop()->Eopid() != CLogical::EopLogicalSelect,
						NULL != ptabdesc);
	}
	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSelect(mp, ptabdesc), pexpr, pexprPredicate);
}

// if predicate is True return logical expression, otherwise return a new select node
CExpression *
CUtils::PexprSafeSelect(CMemoryPool *mp, CExpression *pexprLogical,
						CExpression *pexprPred)
{
	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(NULL != pexprPred);

	if (FScalarConstTrue(pexprPred))
	{
		// caller must have add-refed the predicate before coming here
		pexprPred->Release();
		return pexprLogical;
	}

	return PexprLogicalSelect(mp, pexprLogical, pexprPred);
}

// generate a select expression, if child is another Select expression
// collapse both Selects into one expression
CExpression *
CUtils::PexprCollapseSelect(CMemoryPool *mp, CExpression *pexpr,
							CExpression *pexprPredicate)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pexprPredicate);

	if (COperator::EopLogicalSelect == pexpr->Pop()->Eopid() &&
		2 ==
			pexpr
				->Arity()  // perform collapsing only when we have a full Select tree
	)
	{
		// collapse Selects into one expression
		(*pexpr)[0]->AddRef();
		CExpression *pexprChild = (*pexpr)[0];
		CExpression *pexprScalar =
			CPredicateUtils::PexprConjunction(mp, pexprPredicate, (*pexpr)[1]);

		return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
										pexprChild, pexprScalar);
	}

	pexpr->AddRef();
	pexprPredicate->AddRef();

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), pexpr, pexprPredicate);
}

// generate a project expression
CExpression *
CUtils::PexprLogicalProject(CMemoryPool *mp, CExpression *pexpr,
							CExpression *pexprPrjList, BOOL fNewComputedCol)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());

	if (fNewComputedCol)
	{
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		const ULONG arity = pexprPrjList->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprPrEl = (*pexprPrjList)[ul];
			col_factory->AddComputedToUsedColsMap(pexprPrEl);
		}
	}
	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalProject(mp), pexpr, pexprPrjList);
}

// generate a sequence project expression
CExpression *
CUtils::PexprLogicalSequenceProject(CMemoryPool *mp, CDistributionSpec *pds,
									COrderSpecArray *pdrgpos,
									CWindowFrameArray *pdrgpwf,
									CExpression *pexpr,
									CExpression *pexprPrjList)
{
	GPOS_ASSERT(NULL != pds);
	GPOS_ASSERT(NULL != pdrgpos);
	GPOS_ASSERT(NULL != pdrgpwf);
	GPOS_ASSERT(pdrgpwf->Size() == pdrgpos->Size());
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSequenceProject(mp, pds, pdrgpos, pdrgpwf),
		pexpr, pexprPrjList);
}

// construct a projection of NULL constants using the given column
// names and types on top of the given expression
CExpression* CUtils::PexprLogicalProjectNulls(CMemoryPool *mp, CColRefArray *colref_array, CExpression *pexpr, UlongToColRefMap *colref_mapping)
{
	IDatumArray *pdrgpdatum = GPOS_NEW(mp) IDatumArray(mp);
	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		const IMDType *pmdtype = colref->RetrieveType();
		IDatum *datum = pmdtype->DatumNull();
		datum->AddRef();
		pdrgpdatum->Append(datum);
	}
	CExpression *pexprProjList = PexprScalarProjListConst(mp, colref_array, pdrgpdatum, colref_mapping);
	pdrgpdatum->Release();

	return PexprLogicalProject(mp, pexpr, pexprProjList, false /*fNewComputedCol*/);
}

// construct a project list using the column names and types of the given
// array of column references, and the given datums
CExpression *
CUtils::PexprScalarProjListConst(CMemoryPool *mp, CColRefArray *colref_array,
								 IDatumArray *pdrgpdatum,
								 UlongToColRefMap *colref_mapping)
{
	GPOS_ASSERT(colref_array->Size() == pdrgpdatum->Size());

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CScalarProjectList *pscprl = GPOS_NEW(mp) CScalarProjectList(mp);

	const ULONG arity = colref_array->Size();
	if (0 == arity)
	{
		return GPOS_NEW(mp) CExpression(mp, pscprl);
	}

	CExpressionArray *pdrgpexprProjElems = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRef *colref = (*colref_array)[ul];

		IDatum *datum = (*pdrgpdatum)[ul];
		datum->AddRef();
		CScalarConst *popScConst = GPOS_NEW(mp) CScalarConst(mp, datum);
		CExpression *pexprScConst = GPOS_NEW(mp) CExpression(mp, popScConst);

		CColRef *new_colref = col_factory->PcrCreate(
			colref->RetrieveType(), colref->TypeModifier(), colref->Name());
		if (NULL != colref_mapping)
		{
#ifdef GPOS_DEBUG
			BOOL fInserted =
#endif
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(colref->Id()),
									   new_colref);
			GPOS_ASSERT(fInserted);
		}

		CScalarProjectElement *popScPrEl =
			GPOS_NEW(mp) CScalarProjectElement(mp, new_colref);
		CExpression *pexprScPrEl =
			GPOS_NEW(mp) CExpression(mp, popScPrEl, pexprScConst);

		pdrgpexprProjElems->Append(pexprScPrEl);
	}

	return GPOS_NEW(mp) CExpression(mp, pscprl, pdrgpexprProjElems);
}

// generate a project expression with one additional project element
CExpression *
CUtils::PexprAddProjection(CMemoryPool *mp, CExpression *pexpr,
						   CExpression *pexprProjected)
{
	GPOS_ASSERT(pexpr->Pop()->FLogical());
	GPOS_ASSERT(pexprProjected->Pop()->FScalar());

	CExpressionArray *pdrgpexprProjected = GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexprProjected->Append(pexprProjected);

	CExpression *pexprProjection =
		PexprAddProjection(mp, pexpr, pdrgpexprProjected);
	pdrgpexprProjected->Release();

	return pexprProjection;
}

// generate a project expression with one or more additional project elements
CExpression *
CUtils::PexprAddProjection(CMemoryPool *mp, CExpression *pexpr,
						   CExpressionArray *pdrgpexprProjected,
						   BOOL fNewComputedCol)
{
	GPOS_ASSERT(pexpr->Pop()->FLogical());
	GPOS_ASSERT(NULL != pdrgpexprProjected);

	if (0 == pdrgpexprProjected->Size())
	{
		return pexpr;
	}

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpressionArray *pdrgpexprPrjElem = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulProjected = pdrgpexprProjected->Size();
	for (ULONG ul = 0; ul < ulProjected; ul++)
	{
		CExpression *pexprProjected = (*pdrgpexprProjected)[ul];
		GPOS_ASSERT(pexprProjected->Pop()->FScalar());

		// generate a computed column with scalar expression type
		CScalar *popScalar = CScalar::PopConvert(pexprProjected->Pop());
		const IMDType *pmdtype =
			md_accessor->RetrieveType(popScalar->MdidType());
		CColRef *colref =
			col_factory->PcrCreate(pmdtype, popScalar->TypeModifier());

		pexprProjected->AddRef();
		pdrgpexprPrjElem->Append(
			PexprScalarProjectElement(mp, colref, pexprProjected));
	}

	CExpression *pexprPrjList = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprPrjElem);

	return PexprLogicalProject(mp, pexpr, pexprPrjList, fNewComputedCol);
}

// generate an aggregate expression
CExpression *
CUtils::PexprLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array,
						  CExpression *pexprRelational, CExpression *pexprPrL,
						  COperator::EGbAggType egbaggtype)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != pexprRelational);
	GPOS_ASSERT(NULL != pexprPrL);

	// create a new logical group by operator
	CLogicalGbAgg *pop =
		GPOS_NEW(mp) CLogicalGbAgg(mp, colref_array, egbaggtype);

	return GPOS_NEW(mp) CExpression(mp, pop, pexprRelational, pexprPrL);
}

// generate a global aggregate expression
CExpression *
CUtils::PexprLogicalGbAggGlobal(CMemoryPool *mp, CColRefArray *colref_array,
								CExpression *pexprRelational,
								CExpression *pexprProjList)
{
	return PexprLogicalGbAgg(mp, colref_array, pexprRelational, pexprProjList,
							 COperator::EgbaggtypeGlobal);
}

// check if given project list has a global aggregate function
BOOL
CUtils::FHasGlobalAggFunc(const CExpression *pexprAggProjList)
{
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprAggProjList->Pop()->Eopid());
	BOOL fGlobal = false;

	const ULONG arity = pexprAggProjList->Arity();

	for (ULONG ul = 0; ul < arity && !fGlobal; ul++)
	{
		CExpression *pexprPrEl = (*pexprAggProjList)[ul];
		GPOS_ASSERT(COperator::EopScalarProjectElement ==
					pexprPrEl->Pop()->Eopid());
		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
		fGlobal = popScAggFunc->FGlobal();
	}

	return fGlobal;
}

// return the comparison type for the given mdid
IMDType::ECmpType
CUtils::ParseCmpType(IMDId *mdid)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);
	return md_scalar_op->ParseCmpType();
}

// return the comparison type for the given mdid
IMDType::ECmpType
CUtils::ParseCmpType(CMDAccessor *md_accessor, IMDId *mdid)
{
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);
	return md_scalar_op->ParseCmpType();
}

// check if the expression is a scalar boolean const
BOOL
CUtils::FScalarConstBool(CExpression *pexpr, BOOL value)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarConst == pop->Eopid())
	{
		CScalarConst *popScalarConst = CScalarConst::PopConvert(pop);
		if (IMDType::EtiBool == popScalarConst->GetDatum()->GetDatumType())
		{
			IDatumBool *datum =
				dynamic_cast<IDatumBool *>(popScalarConst->GetDatum());
			return !datum->IsNull() && datum->GetValue() == value;
		}
	}

	return false;
}

BOOL
CUtils::FScalarConstBoolNull(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarConst == pop->Eopid())
	{
		CScalarConst *popScalarConst = CScalarConst::PopConvert(pop);
		if (IMDType::EtiBool == popScalarConst->GetDatum()->GetDatumType())
		{
			return popScalarConst->GetDatum()->IsNull();
		}
	}

	return false;
}

// checks to see if the expression is a scalar const TRUE
BOOL
CUtils::FScalarConstTrue(CExpression *pexpr)
{
	return FScalarConstBool(pexpr, true /*value*/);
}

// checks to see if the expression is a scalar const FALSE
BOOL
CUtils::FScalarConstFalse(CExpression *pexpr)
{
	return FScalarConstBool(pexpr, false /*value*/);
}

//	create an array of expression's output columns including a key for grouping
CColRefArray *
CUtils::PdrgpcrGroupingKey(
	CMemoryPool *mp, CExpression *pexpr,
	CColRefArray **
		ppdrgpcrKey	 // output: array of key columns contained in the returned grouping columns
)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != ppdrgpcrKey);

	CKeyCollection *pkc = pexpr->DeriveKeyCollection();
	GPOS_ASSERT(NULL != pkc);

	CColRefSet *pcrsOutput = pexpr->DeriveOutputColumns();
	CColRefSet *pcrsUsedOuter = GPOS_NEW(mp) CColRefSet(mp);

	// remove any columns that are not referenced in the query from pcrsOuterOutput
	// filter out system columns since they may introduce columns with undefined sort/hash operators
	CColRefSetIter it(*pcrsOutput);
	while (it.Advance())
	{
		CColRef *pcr = it.Pcr();

		if (CColRef::EUsed == pcr->GetUsage() && !pcr->FSystemCol())
		{
			pcrsUsedOuter->Include(pcr);
		}
	}

	// prefer extracting a hashable key since Agg operator may redistribute child on grouping columns
	CColRefArray *pdrgpcrKey = pkc->PdrgpcrHashableKey(mp);
	if (NULL == pdrgpcrKey)
	{
		// if no hashable key, extract any key
		pdrgpcrKey = pkc->PdrgpcrKey(mp);
	}
	GPOS_ASSERT(NULL != pdrgpcrKey);

	CColRefSet *pcrsKey = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrKey);
	pcrsUsedOuter->Union(pcrsKey);

	pcrsKey->Release();
	CColRefArray *colref_array = pcrsUsedOuter->Pdrgpcr(mp);
	pcrsUsedOuter->Release();

	// set output key array
	*ppdrgpcrKey = pdrgpcrKey;

	return colref_array;
}

// add an equivalence class to the array. If the new equiv class contains
// columns from separate equiv classes, then these are merged. Returns a new
// array of equivalence classes
CColRefSetArray *
CUtils::AddEquivClassToArray(CMemoryPool *mp, const CColRefSet *pcrsNew,
							 const CColRefSetArray *pdrgpcrs)
{
	CColRefSetArray *pdrgpcrsNew = GPOS_NEW(mp) CColRefSetArray(mp);
	CColRefSet *pcrsCopy = GPOS_NEW(mp) CColRefSet(mp, *pcrsNew);

	const ULONG length = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		if (pcrsCopy->IsDisjoint(pcrs))
		{
			pcrs->AddRef();
			pdrgpcrsNew->Append(pcrs);
		}
		else
		{
			pcrsCopy->Include(pcrs);
		}
	}

	pdrgpcrsNew->Append(pcrsCopy);

	return pdrgpcrsNew;
}

// merge 2 arrays of equivalence classes
CColRefSetArray *
CUtils::PdrgpcrsMergeEquivClasses(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst,
								  CColRefSetArray *pdrgpcrsSnd)
{
	pdrgpcrsFst->AddRef();
	CColRefSetArray *pdrgpcrsMerged = pdrgpcrsFst;

	const ULONG length = pdrgpcrsSnd->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrsSnd)[ul];

		CColRefSetArray *pdrgpcrs =
			AddEquivClassToArray(mp, pcrs, pdrgpcrsMerged);
		pdrgpcrsMerged->Release();
		pdrgpcrsMerged = pdrgpcrs;
	}

	return pdrgpcrsMerged;
}

// intersect 2 arrays of equivalence classes. This is accomplished by using
// a hashmap to find intersects between two arrays of colomun referance sets
CColRefSetArray *
CUtils::PdrgpcrsIntersectEquivClasses(CMemoryPool *mp,
									  CColRefSetArray *pdrgpcrsFst,
									  CColRefSetArray *pdrgpcrsSnd)
{
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(mp, pdrgpcrsFst));
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(mp, pdrgpcrsSnd));

	CColRefSetArray *pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	const ULONG ulLenFst = pdrgpcrsFst->Size();
	const ULONG ulLenSnd = pdrgpcrsSnd->Size();

	// nothing to intersect, so return empty array
	if (ulLenSnd == 0 || ulLenFst == 0)
	{
		return pdrgpcrs;
	}

	ColRefToColRefSetMap *phmcscrs = GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	ColRefToColRefMap *phmcscrDone = GPOS_NEW(mp) ColRefToColRefMap(mp);

	// populate a hashmap in this loop
	for (ULONG ulFst = 0; ulFst < ulLenFst; ulFst++)
	{
		CColRefSet *pcrsFst = (*pdrgpcrsFst)[ulFst];

		// because the equivalence classes are disjoint, a single column will only be a member
		// of one equivalence class. therefore, we create a hash map keyed on one column
		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();
			phmcscrs->Insert(colref, pcrsFst);
		}
	}

	// probe the hashmap with the equivalence classes
	for (ULONG ulSnd = 0; ulSnd < ulLenSnd; ulSnd++)
	{
		CColRefSet *pcrsSnd = (*pdrgpcrsSnd)[ulSnd];

		// iterate on column references in the equivalence class
		CColRefSetIter crsi(*pcrsSnd);
		while (crsi.Advance())
		{
			// lookup a column in the hashmap
			CColRef *colref = crsi.Pcr();
			CColRefSet *pcrs = phmcscrs->Find(colref);
			CColRef *pcrDone = phmcscrDone->Find(colref);

			// continue if we don't find this column or if that column
			// is already processed and outputed as an intersection of two
			// column referance sets
			if (NULL != pcrs && pcrDone == NULL)
			{
				CColRefSet *pcrsNew = GPOS_NEW(mp) CColRefSet(mp);
				pcrsNew->Include(pcrsSnd);
				pcrsNew->Intersection(pcrs);
				pdrgpcrs->Append(pcrsNew);

				CColRefSetIter hmpcrs(*pcrsNew);
				// now that we have created an intersection, any additional matches to these
				// columns would create a duplicate intersect, so we add those columns to
				// the DONE hash map
				while (hmpcrs.Advance())
				{
					CColRef *pcrProcessed = hmpcrs.Pcr();
					phmcscrDone->Insert(pcrProcessed, pcrProcessed);
				}
			}
		}
	}
	phmcscrDone->Release();
	phmcscrs->Release();

	return pdrgpcrs;
}

// return a copy of equivalence classes from all children
CColRefSetArray *
CUtils::PdrgpcrsCopyChildEquivClasses(CMemoryPool *mp,
									  CExpressionHandle &exprhdl)
{
	CColRefSetArray *pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CColRefSetArray *pdrgpcrsChild =
				exprhdl.DerivePropertyConstraint(ul)->PdrgpcrsEquivClasses();

			CColRefSetArray *pdrgpcrsChildCopy =
				GPOS_NEW(mp) CColRefSetArray(mp);
			const ULONG size = pdrgpcrsChild->Size();
			for (ULONG ulInner = 0; ulInner < size; ulInner++)
			{
				CColRefSet *pcrs =
					GPOS_NEW(mp) CColRefSet(mp, *(*pdrgpcrsChild)[ulInner]);
				pdrgpcrsChildCopy->Append(pcrs);
			}

			CColRefSetArray *pdrgpcrsMerged =
				PdrgpcrsMergeEquivClasses(mp, pdrgpcrs, pdrgpcrsChildCopy);
			pdrgpcrsChildCopy->Release();
			pdrgpcrs->Release();
			pdrgpcrs = pdrgpcrsMerged;
		}
	}

	return pdrgpcrs;
}

// return a copy of the given array of columns, excluding the columns in the given colrefset
CColRefArray *
CUtils::PdrgpcrExcludeColumns(CMemoryPool *mp, CColRefArray *pdrgpcrOriginal,
							  CColRefSet *pcrsExcluded)
{
	GPOS_ASSERT(NULL != pdrgpcrOriginal);
	GPOS_ASSERT(NULL != pcrsExcluded);

	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG num_cols = pdrgpcrOriginal->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*pdrgpcrOriginal)[ul];
		if (!pcrsExcluded->FMember(colref))
		{
			colref_array->Append(colref);
		}
	}

	return colref_array;
}

// helper function to print a colref array
IOstream &
CUtils::OsPrintDrgPcr(IOstream &os, const CColRefArray *colref_array,
					  ULONG ulLenMax)
{
	ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < std::min(length, ulLenMax); ul++)
	{
		(*colref_array)[ul]->OsPrint(os);
		if (ul < length - 1)
		{
			os << ", ";
		}
	}

	if (ulLenMax < length)
	{
		os << "...";
	}

	return os;
}

// check if given expression is a scalar comparison
BOOL
CUtils::FScalarCmp(CExpression *pexpr)
{
	return (COperator::EopScalarCmp == pexpr->Pop()->Eopid());
}

// check if given expression is a scalar array comparison
BOOL
CUtils::FScalarArrayCmp(CExpression *pexpr)
{
	return (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid());
}

// check if given expression has any one stage agg nodes
BOOL
CUtils::FHasOneStagePhysicalAgg(const CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (FPhysicalAgg(pexpr->Pop()) &&
		!CPhysicalAgg::PopConvert(pexpr->Pop())->FMultiStage())
	{
		return true;
	}

	// recursively check children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasOneStagePhysicalAgg((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// check if given operator exists in the given list
BOOL
CUtils::FOpExists(const COperator *pop, const COperator::EOperatorId *peopid,
				  ULONG ulOps)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != peopid);

	COperator::EOperatorId op_id = pop->Eopid();
	for (ULONG ul = 0; ul < ulOps; ul++)
	{
		if (op_id == peopid[ul])
		{
			return true;
		}
	}

	return false;
}

// check if given expression has any operator in the given list
BOOL
CUtils::FHasOp(const CExpression *pexpr, const COperator::EOperatorId *peopid,
			   ULONG ulOps)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != peopid);

	if (FOpExists(pexpr->Pop(), peopid, ulOps))
	{
		return true;
	}

	// recursively check children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasOp((*pexpr)[ul], peopid, ulOps))
		{
			return true;
		}
	}

	return false;
}

// return number of inlinable CTEs in the given expression
ULONG
CUtils::UlInlinableCTEs(CExpression *pexpr, ULONG ulDepth)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pop);
		CExpression *pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		GPOS_ASSERT(NULL != pexprProducer);
		return ulDepth + UlInlinableCTEs(pexprProducer, ulDepth + 1);
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildCTEs = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildCTEs += UlInlinableCTEs((*pexpr)[ul], ulDepth);
	}

	return ulChildCTEs;
}

// return number of joins in the given expression
ULONG
CUtils::UlJoins(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	ULONG ulJoins = 0;
	COperator *pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pop);
		CExpression *pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		return UlJoins(pexprProducer);
	}

	if (FLogicalJoin(pop) || FPhysicalJoin(pop))
	{
		ulJoins = 1;
		if (COperator::EopLogicalNAryJoin == pop->Eopid())
		{
			// N-Ary join is equivalent to a cascade of (Arity - 2) binary joins
			ulJoins = pexpr->Arity() - 2;
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildJoins = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildJoins += UlJoins((*pexpr)[ul]);
	}

	return ulJoins + ulChildJoins;
}

// return number of subqueries in the given expression
ULONG
CUtils::UlSubqueries(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	ULONG ulSubqueries = 0;
	COperator *pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pop);
		CExpression *pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		return UlSubqueries(pexprProducer);
	}

	if (FSubquery(pop))
	{
		ulSubqueries = 1;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildSubqueries = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildSubqueries += UlSubqueries((*pexpr)[ul]);
	}

	return ulSubqueries + ulChildSubqueries;
}

// check if given expression is a scalar boolean operator
BOOL
CUtils::FScalarBoolOp(CExpression *pexpr)
{
	return (COperator::EopScalarBoolOp == pexpr->Pop()->Eopid());
}

// is the given expression a scalar bool op of the passed type?
BOOL
CUtils::FScalarBoolOp(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	return pop->FScalar() && COperator::EopScalarBoolOp == pop->Eopid() &&
		   eboolop == CScalarBoolOp::PopConvert(pop)->Eboolop();
}

// check if given expression is a scalar null test
BOOL
CUtils::FScalarNullTest(CExpression *pexpr)
{
	return (COperator::EopScalarNullTest == pexpr->Pop()->Eopid());
}

// check if given expression is a NOT NULL predicate
BOOL
CUtils::FScalarNotNull(CExpression *pexpr)
{
	return FScalarBoolOp(pexpr, CScalarBoolOp::EboolopNot) &&
		   FScalarNullTest((*pexpr)[0]);
}

// check if given expression is a scalar identifier
BOOL
CUtils::FScalarIdent(CExpression *pexpr)
{
	return (COperator::EopScalarIdent == pexpr->Pop()->Eopid());
}

BOOL
CUtils::FScalarIdentIgnoreCast(CExpression *pexpr)
{
	return (COperator::EopScalarIdent == pexpr->Pop()->Eopid() ||
			CScalarIdent::FCastedScId(pexpr));
}

// check if given expression is a scalar boolean identifier
BOOL
CUtils::FScalarIdentBoolType(CExpression *pexpr)
{
	return FScalarIdent(pexpr) &&
		   IMDType::EtiBool == CScalarIdent::PopConvert(pexpr->Pop())
								   ->Pcr()
								   ->RetrieveType()
								   ->GetDatumType();
}

// check if given expression is a scalar array
BOOL
CUtils::FScalarArray(CExpression *pexpr)
{
	return (COperator::EopScalarArray == pexpr->Pop()->Eopid());
}

// check if given expression is a scalar array coerce
BOOL
CUtils::FScalarArrayCoerce(CExpression *pexpr)
{
	return (COperator::EopScalarArrayCoerceExpr == pexpr->Pop()->Eopid());
}

// is the given expression a scalar identifier with the given column reference
BOOL
CUtils::FScalarIdent(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != colref);

	return COperator::EopScalarIdent == pexpr->Pop()->Eopid() &&
		   CScalarIdent::PopConvert(pexpr->Pop())->Pcr() == colref;
}

// check if the expression is a scalar const
BOOL
CUtils::FScalarConst(CExpression *pexpr)
{
	return (COperator::EopScalarConst == pexpr->Pop()->Eopid());
}

// check if the expression is variable-free
BOOL
CUtils::FVarFreeExpr(CExpression *pexpr)
{
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (FScalarConst(pexpr))
	{
		return true;
	}

	if (pexpr->DeriveHasSubquery())
	{
		return false;
	}

	GPOS_ASSERT(0 == pexpr->DeriveDefinedColumns()->Size());
	CColRefSet *pcrsUsed = pexpr->DeriveUsedColumns();

	// no variables in expression
	return 0 == pcrsUsed->Size();
}

// check if the expression is a scalar predicate, i.e. bool op, comparison, or null test
BOOL
CUtils::FPredicate(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();
	return pop->FScalar() &&
		   (FScalarCmp(pexpr) || CPredicateUtils::FIDF(pexpr) ||
			FScalarArrayCmp(pexpr) || FScalarBoolOp(pexpr) ||
			FScalarNullTest(pexpr) ||
			CLogical::EopScalarNAryJoinPredList == pop->Eopid());
}

// checks that the given type has all the comparisons: Eq, NEq, L, LEq, G, GEq.
BOOL
CUtils::FHasAllDefaultComparisons(const IMDType *pmdtype)
{
	GPOS_ASSERT(NULL != pmdtype);

	return IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptNEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptL)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptLEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptG)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptGEq));
}

// determine whether a type is supported for use in contradiction detection.
// The assumption is that we only compare data of the same type.
BOOL
CUtils::FConstrainableType(IMDId *mdid_type)
{
	if (FIntType(mdid_type))
	{
		return true;
	}
	if (!GPOS_FTRACE(EopttraceEnableConstantExpressionEvaluation))
	{
		return false;
	}
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *pmdtype = md_accessor->RetrieveType(mdid_type);

	return FHasAllDefaultComparisons(pmdtype);
}

// determine whether a type is an integer type
BOOL
CUtils::FIntType(IMDId *mdid_type)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDType::ETypeInfo type_info =
		md_accessor->RetrieveType(mdid_type)->GetDatumType();

	return (IMDType::EtiInt2 == type_info || IMDType::EtiInt4 == type_info ||
			IMDType::EtiInt8 == type_info);
}

// check if a binary operator uses only columns produced by its children
BOOL
CUtils::FUsesChildColsOnly(CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	CColRefSet *pcrsUsed = exprhdl.DeriveUsedColumns(2);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(exprhdl.DeriveOutputColumns(0 /*child_index*/));
	pcrs->Include(exprhdl.DeriveOutputColumns(1 /*child_index*/));
	BOOL fUsesChildCols = pcrs->ContainsAll(pcrsUsed);
	pcrs->Release();

	return fUsesChildCols;
}

// check if inner child of a binary operator uses columns not produced by outer child
BOOL
CUtils::FInnerUsesExternalCols(CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences(1 /*child_index*/);
	if (0 == outer_refs->Size())
	{
		return false;
	}
	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns(0 /*child_index*/);

	return !pcrsOutput->ContainsAll(outer_refs);
}

// check if inner child of a binary operator uses only columns not produced by outer child
BOOL
CUtils::FInnerUsesExternalColsOnly(CExpressionHandle &exprhdl)
{
	return FInnerUsesExternalCols(exprhdl) &&
		   exprhdl.DeriveOuterReferences(1)->IsDisjoint(
			   exprhdl.DeriveOutputColumns(0));
}

// check if given columns have available comparison operators
BOOL
CUtils::FComparisonPossible(CColRefArray *colref_array,
							IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		const IMDType *pmdtype = colref->RetrieveType();
		if (!IMDId::IsValid(pmdtype->GetMdidForCmpType(cmp_type)))
		{
			return false;
		}
	}

	return true;
}

// counts the number of times a certain operator appears
ULONG
CUtils::UlCountOperator(const CExpression *pexpr, COperator::EOperatorId op_id)
{
	ULONG ulOpCnt = 0;
	if (op_id == pexpr->Pop()->Eopid())
	{
		ulOpCnt += 1;
	}

	const ULONG arity = pexpr->Arity();
	for (ULONG ulChild = 0; ulChild < arity; ulChild++)
	{
		ulOpCnt += UlCountOperator((*pexpr)[ulChild], op_id);
	}
	return ulOpCnt;
}

// return the max subset of redistributable columns for the given columns
CColRefArray *
CUtils::PdrgpcrRedistributableSubset(CMemoryPool *mp,
									 CColRefArray *colref_array)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	CColRefArray *pdrgpcrRedist = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		const IMDType *pmdtype = colref->RetrieveType();
		if (pmdtype->IsRedistributable())
		{
			pdrgpcrRedist->Append(colref);
		}
	}

	return pdrgpcrRedist;
}

// check if hashing is possible for the given columns
BOOL
CUtils::IsHashable(CColRefArray *colref_array)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		const IMDType *pmdtype = colref->RetrieveType();
		if (!pmdtype->IsHashable())
		{
			return false;
		}
	}

	return true;
}

// check if given operator is a logical DML operator
BOOL
CUtils::FLogicalDML(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return COperator::EopLogicalDML == op_id ||
		   COperator::EopLogicalInsert == op_id ||
		   COperator::EopLogicalDelete == op_id ||
		   COperator::EopLogicalUpdate == op_id;
}

// return regular string from wide-character string
CHAR *
CUtils::CreateMultiByteCharStringFromWCString(CMemoryPool *mp, WCHAR *wsz)
{
	ULONG ulMaxLength = GPOS_WSZ_LENGTH(wsz) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *sz = GPOS_NEW_ARRAY(mp, CHAR, ulMaxLength);
	clib::Wcstombs(sz, wsz, ulMaxLength);
	sz[ulMaxLength - 1] = '\0';

	return sz;
}

// construct an array of colids from the given array of column references
ULongPtrArray *
CUtils::Pdrgpul(CMemoryPool *mp, const CColRefArray *colref_array)
{
	ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ULONG *pul = GPOS_NEW(mp) ULONG(colref->Id());
		pdrgpul->Append(pul);
	}

	return pdrgpul;
}

// generate a timestamp-based filename in the provided buffer.
void
CUtils::GenerateFileName(CHAR *buf, const CHAR *szPrefix, const CHAR *szExt,
						 ULONG length, ULONG ulSessionId, ULONG ulCmdId)
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	CWStringDynamic *filename_template = GPOS_NEW(mp) CWStringDynamic(mp);
	COstreamString oss(filename_template);
	oss << szPrefix << "_%04d%02d%02d_%02d%02d%02d_%d_%d." << szExt;

	const WCHAR *wszFileNameTemplate = filename_template->GetBuffer();
	GPOS_ASSERT(length >= GPOS_FILE_NAME_BUF_SIZE);

	TIMEVAL tv;
	TIME tm;

	// get local time
	syslib::GetTimeOfDay(&tv, NULL /*timezone*/);
#ifdef GPOS_DEBUG
	TIME *ptm =
#endif	// GPOS_DEBUG
		clib::Localtime_r(&tv.tv_sec, &tm);

	GPOS_ASSERT(NULL != ptm && "Failed to get local time");

	WCHAR wszBuf[GPOS_FILE_NAME_BUF_SIZE];
	CWStringStatic str(wszBuf, GPOS_ARRAY_SIZE(wszBuf));

	str.AppendFormat(wszFileNameTemplate, tm.tm_year + 1900, tm.tm_mon + 1,
					 tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, ulSessionId,
					 ulCmdId);

	INT iResult = (INT) clib::Wcstombs(buf, wszBuf, length);

	GPOS_RTL_ASSERT((INT) 0 < iResult && iResult <= (INT) str.Length());

	buf[length - 1] = '\0';

	GPOS_DELETE(filename_template);
}

// return the mapping of the given colref based on the given hashmap
CColRef *
CUtils::PcrRemap(const CColRef *colref, UlongToColRefMap *colref_mapping,
				 BOOL
#ifdef GPOS_DEBUG
					 must_exist
#endif	//GPOS_DEBUG
)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != colref_mapping);

	ULONG id = colref->Id();
	CColRef *pcrMapped = colref_mapping->Find(&id);

	if (NULL != pcrMapped)
	{
		GPOS_ASSERT(colref != pcrMapped);
		return pcrMapped;
	}

	GPOS_ASSERT(!must_exist && "Column does not exist in hashmap");
	return const_cast<CColRef *>(colref);
}

// create a new colrefset corresponding to the given colrefset and based on the given mapping
CColRefSet *
CUtils::PcrsRemap(CMemoryPool *mp, CColRefSet *pcrs,
				  UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != colref_mapping);

	CColRefSet *pcrsMapped = GPOS_NEW(mp) CColRefSet(mp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();
		CColRef *pcrMapped = PcrRemap(colref, colref_mapping, must_exist);
		pcrsMapped->Include(pcrMapped);
	}

	return pcrsMapped;
}

// create an array of column references corresponding to the given array
// and based on the given mapping
CColRefArray *
CUtils::PdrgpcrRemap(CMemoryPool *mp, CColRefArray *colref_array,
					 UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != colref_mapping);

	CColRefArray *pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CColRef *pcrMapped = PcrRemap(colref, colref_mapping, must_exist);
		pdrgpcrNew->Append(pcrMapped);
	}

	return pdrgpcrNew;
}

// ceate an array of column references corresponding to the given array
// and based on the given mapping. Create new colrefs if necessary
CColRefArray *
CUtils::PdrgpcrRemapAndCreate(CMemoryPool *mp, CColRefArray *colref_array,
							  UlongToColRefMap *colref_mapping)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != colref_mapping);

	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CColRefArray *pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ULONG id = colref->Id();
		CColRef *pcrMapped = colref_mapping->Find(&id);
		if (NULL == pcrMapped)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			pcrMapped = col_factory->PcrCopy(colref);

#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrMapped);
			GPOS_ASSERT(result);
		}

		pdrgpcrNew->Append(pcrMapped);
	}

	return pdrgpcrNew;
}

// create an array of column arrays corresponding to the given array
// and based on the given mapping
CColRef2dArray *
CUtils::PdrgpdrgpcrRemap(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcr,
						 UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	GPOS_ASSERT(NULL != colref_mapping);

	CColRef2dArray *pdrgpdrgpcrNew = GPOS_NEW(mp) CColRef2dArray(mp);

	const ULONG arity = pdrgpdrgpcr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRefArray *colref_array =
			PdrgpcrRemap(mp, (*pdrgpdrgpcr)[ul], colref_mapping, must_exist);
		pdrgpdrgpcrNew->Append(colref_array);
	}

	return pdrgpdrgpcrNew;
}

// remap given array of expressions with provided column mappings
CExpressionArray *
CUtils::PdrgpexprRemap(CMemoryPool *mp, CExpressionArray *pdrgpexpr,
					   UlongToColRefMap *colref_mapping)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != colref_mapping);

	CExpressionArray *pdrgpexprNew = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		pdrgpexprNew->Append(pexpr->PexprCopyWithRemappedColumns(
			mp, colref_mapping, true /*must_exist*/));
	}

	return pdrgpexprNew;
}

// create col ID->ColRef mapping using the given ColRef arrays
UlongToColRefMap *
CUtils::PhmulcrMapping(CMemoryPool *mp, CColRefArray *pdrgpcrFrom,
					   CColRefArray *pdrgpcrTo)
{
	GPOS_ASSERT(NULL != pdrgpcrFrom);
	GPOS_ASSERT(NULL != pdrgpcrTo);

	UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
	AddColumnMapping(mp, colref_mapping, pdrgpcrFrom, pdrgpcrTo);

	return colref_mapping;
}

// add col ID->ColRef mappings to the given hashmap based on the given ColRef arrays
void
CUtils::AddColumnMapping(CMemoryPool *mp, UlongToColRefMap *colref_mapping,
						 CColRefArray *pdrgpcrFrom, CColRefArray *pdrgpcrTo)
{
	GPOS_ASSERT(NULL != colref_mapping);
	GPOS_ASSERT(NULL != pdrgpcrFrom);
	GPOS_ASSERT(NULL != pdrgpcrTo);

	const ULONG ulColumns = pdrgpcrFrom->Size();
	GPOS_ASSERT(ulColumns == pdrgpcrTo->Size());

	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		CColRef *pcrFrom = (*pdrgpcrFrom)[ul];
		ULONG ulFromId = pcrFrom->Id();
		CColRef *pcrTo = (*pdrgpcrTo)[ul];
		GPOS_ASSERT(pcrFrom != pcrTo);

#ifdef GPOS_DEBUG
		BOOL result = false;
#endif	// GPOS_DEBUG
		CColRef *pcrExist = colref_mapping->Find(&ulFromId);
		if (NULL == pcrExist)
		{
#ifdef GPOS_DEBUG
			result =
#endif	// GPOS_DEBUG
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(ulFromId), pcrTo);
			GPOS_ASSERT(result);
		}
		else
		{
#ifdef GPOS_DEBUG
			result =
#endif	// GPOS_DEBUG
				colref_mapping->Replace(&ulFromId, pcrTo);
		}
		GPOS_ASSERT(result);
	}
}

// create a copy of the array of column references
CColRefArray *
CUtils::PdrgpcrExactCopy(CMemoryPool *mp, CColRefArray *colref_array)
{
	CColRefArray *pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG num_cols = colref_array->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		pdrgpcrNew->Append(colref);
	}

	return pdrgpcrNew;
}

// Create an array of new column references with the same names and types
// as the given column references. If the passed map is not null, mappings
// from old to copied variables are added to it.
CColRefArray *
CUtils::PdrgpcrCopy(CMemoryPool *mp, CColRefArray *colref_array,
					BOOL fAllComputed, UlongToColRefMap *colref_mapping)
{
	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CColRefArray *pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG num_cols = colref_array->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CColRef *new_colref = NULL;
		if (fAllComputed)
		{
			new_colref = col_factory->PcrCreate(colref);
		}
		else
		{
			new_colref = col_factory->PcrCopy(colref);
		}

		pdrgpcrNew->Append(new_colref);
		if (NULL != colref_mapping)
		{
#ifdef GPOS_DEBUG
			BOOL fInserted =
#endif
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(colref->Id()),
									   new_colref);
			GPOS_ASSERT(fInserted);
		}
	}

	return pdrgpcrNew;
}

// equality check between two arrays of column refs. Inputs can be NULL
BOOL
CUtils::Equals(CColRefArray *pdrgpcrFst, CColRefArray *pdrgpcrSnd)
{
	if (NULL == pdrgpcrFst || NULL == pdrgpcrSnd)
	{
		return (NULL == pdrgpcrFst && NULL == pdrgpcrSnd);
	}

	return pdrgpcrFst->Equals(pdrgpcrSnd);
}

// compute hash value for an array of column references
ULONG
CUtils::UlHashColArray(const CColRefArray *colref_array, const ULONG ulMaxCols)
{
	ULONG ulHash = 0;

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length && ul < ulMaxCols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(colref));
	}

	return ulHash;
}

// Return the set of column reference from the CTE Producer corresponding to the
// subset of input columns from the CTE Consumer
CColRefSet *
CUtils::PcrsCTEProducerColumns(CMemoryPool *mp, CColRefSet *pcrsInput,
							   CLogicalCTEConsumer *popCTEConsumer)
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
			popCTEConsumer->UlCTEId());
	GPOS_ASSERT(NULL != pexprProducer);
	CLogicalCTEProducer *popProducer =
		CLogicalCTEProducer::PopConvert(pexprProducer->Pop());

	CColRefArray *pdrgpcrInput = pcrsInput->Pdrgpcr(mp);
	UlongToColRefMap *colref_mapping =
		PhmulcrMapping(mp, popCTEConsumer->Pdrgpcr(), popProducer->Pdrgpcr());
	CColRefArray *pdrgpcrOutput =
		PdrgpcrRemap(mp, pdrgpcrInput, colref_mapping, true /*must_exist*/);

	CColRefSet *pcrsCTEProducer = GPOS_NEW(mp) CColRefSet(mp);
	pcrsCTEProducer->Include(pdrgpcrOutput);

	colref_mapping->Release();
	pdrgpcrInput->Release();
	pdrgpcrOutput->Release();

	return pcrsCTEProducer;
}

// Construct the join condition (AND-tree) of INDF condition
// from the array of input column reference arrays
CExpression *
CUtils::PexprConjINDFCond(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcrInput)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);
	GPOS_ASSERT(2 == pdrgpdrgpcrInput->Size());

	// assemble the new scalar condition
	CExpression *pexprScCond = NULL;
	const ULONG length = (*pdrgpdrgpcrInput)[0]->Size();
	GPOS_ASSERT(0 != length);
	GPOS_ASSERT(length == (*pdrgpdrgpcrInput)[1]->Size());

	CExpressionArray *pdrgpexprInput =
		GPOS_NEW(mp) CExpressionArray(mp, length);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *pcrLeft = (*(*pdrgpdrgpcrInput)[0])[ul];
		CColRef *pcrRight = (*(*pdrgpdrgpcrInput)[1])[ul];
		GPOS_ASSERT(pcrLeft != pcrRight);

		pdrgpexprInput->Append(CUtils::PexprNegate(
			mp, CUtils::PexprIDF(mp, CUtils::PexprScalarIdent(mp, pcrLeft),
								 CUtils::PexprScalarIdent(mp, pcrRight))));
	}

	pexprScCond = CPredicateUtils::PexprConjunction(mp, pdrgpexprInput);

	return pexprScCond;
}

// return index of the set containing given column; if column is not found, return gpos::ulong_max
ULONG
CUtils::UlPcrIndexContainingSet(CColRefSetArray *pdrgpcrs,
								const CColRef *colref)
{
	GPOS_ASSERT(NULL != pdrgpcrs);

	const ULONG size = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		if (pcrs->FMember(colref))
		{
			return ul;
		}
	}

	return gpos::ulong_max;
}

// cast the input expression to the destination mdid
CExpression *
CUtils::PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor, CExpression *pexpr,
				  IMDId *mdid_dest)
{
	GPOS_ASSERT(NULL != mdid_dest);
	IMDId *mdid_src = CScalar::PopConvert(pexpr->Pop())->MdidType();
	GPOS_ASSERT(
		CMDAccessorUtils::FCastExists(md_accessor, mdid_src, mdid_dest));

	const IMDCast *pmdcast = md_accessor->Pmdcast(mdid_src, mdid_dest);

	mdid_dest->AddRef();
	pmdcast->GetCastFuncMdId()->AddRef();
	CExpression *pexprCast;

	CScalarCast *popCast = GPOS_NEW(mp) CScalarCast(mp, mdid_dest, pmdcast->GetCastFuncMdId(), pmdcast->IsBinaryCoercible());
	pexprCast = GPOS_NEW(mp) CExpression(mp, popCast, pexpr);

	return pexprCast;
}

// check whether a colref array contains repeated items
BOOL
CUtils::FHasDuplicates(const CColRefArray *colref_array)
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		if (pcrs->FMember(colref))
		{
			pcrs->Release();
			return true;
		}

		pcrs->Include(colref);
	}

	pcrs->Release();
	return false;
}

// construct an array of scalar ident expressions from the given array of column references
CExpressionArray *
CUtils::PdrgpexprScalarIdents(CMemoryPool *mp, CColRefArray *colref_array)
{
	GPOS_ASSERT(NULL != colref_array);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = colref_array->Size();

	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CExpression *pexpr = PexprScalarIdent(mp, colref);
		pdrgpexpr->Append(pexpr);
	}

	return pdrgpexpr;
}

// return used columns by expressions in the given array
CColRefSet *
CUtils::PcrsExtractColumns(CMemoryPool *mp, const CExpressionArray *pdrgpexpr)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	const ULONG length = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		pcrs->Include(pexpr->DeriveUsedColumns());
	}

	return pcrs;
}

// Create a hashmap of constraints corresponding to a bool const on the given partkeys
// true - unbounded intervals with nulls
// false - empty intervals with no nulls
UlongToConstraintMap *
CUtils::PhmulcnstrBoolConstOnPartKeys(CMemoryPool *mp,
									  CColRef2dArray *pdrgpdrgpcrPartKey,
									  BOOL value)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcrPartKey);
	UlongToConstraintMap *phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);

	const ULONG ulLevels = pdrgpdrgpcrPartKey->Size();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *pcrPartKey = PcrExtractPartKey(pdrgpdrgpcrPartKey, ul);
		CConstraint *pcnstr = NULL;
		if (value)
		{
			// unbounded constraint
			pcnstr = CConstraintInterval::PciUnbounded(mp, pcrPartKey,
													   true /*is_null*/);
		}
		else
		{
			// empty constraint (contradiction)
			pcnstr = GPOS_NEW(mp) CConstraintInterval(
				mp, pcrPartKey, GPOS_NEW(mp) CRangeArray(mp),
				false /*is_null*/);
		}

		if (NULL != pcnstr)
		{
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ul), pcnstr);
			GPOS_ASSERT(result);
		}
	}

	return phmulcnstr;
}

// returns a new bitset of the given length, where all the bits are set
CBitSet *
CUtils::PbsAllSet(CMemoryPool *mp, ULONG size)
{
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, size);
	for (ULONG ul = 0; ul < size; ul++)
	{
		pbs->ExchangeSet(ul);
	}

	return pbs;
}

// returns a new bitset, setting the bits in the given array
CBitSet *
CUtils::Pbs(CMemoryPool *mp, ULongPtrArray *pdrgpul)
{
	GPOS_ASSERT(NULL != pdrgpul);
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp);

	const ULONG length = pdrgpul->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG *pul = (*pdrgpul)[ul];
		pbs->ExchangeSet(*pul);
	}

	return pbs;
}

// extract part constraint from metadata
CPartConstraint *
CUtils::PpartcnstrFromMDPartCnstr(CMemoryPool *mp, CMDAccessor *md_accessor,
								  CColRef2dArray *pdrgpdrgpcrPartKey,
								  const IMDPartConstraint *mdpart_constraint,
								  CColRefArray *pdrgpcrOutput,
								  BOOL fDummyConstraint)
{
	if (fDummyConstraint)
	{
		return GPOS_NEW(mp) CPartConstraint(true /*fUninterpreted*/);
	}

	GPOS_ASSERT(NULL != pdrgpdrgpcrPartKey);

	const ULONG ulLevels = pdrgpdrgpcrPartKey->Size();

	CExpression *pexprPartCnstr = mdpart_constraint->GetPartConstraintExpr(
		mp, md_accessor, pdrgpcrOutput);

	UlongToConstraintMap *phmulcnstr = NULL;
	if (CUtils::FScalarConstTrue(pexprPartCnstr))
	{
		phmulcnstr = PhmulcnstrBoolConstOnPartKeys(mp, pdrgpdrgpcrPartKey,
												   true /*value*/);
	}
	else if (CUtils::FScalarConstFalse(pexprPartCnstr))
	{
		// contradiction
		phmulcnstr = PhmulcnstrBoolConstOnPartKeys(mp, pdrgpdrgpcrPartKey,
												   false /*value*/);
	}
	else
	{
		CColRefSetArray *pdrgpcrs = NULL;
		CConstraint *pcnstr =
			CConstraint::PcnstrFromScalarExpr(mp, pexprPartCnstr, &pdrgpcrs);
		CRefCount::SafeRelease(pdrgpcrs);

		phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);
		for (ULONG ul = 0; ul < ulLevels && NULL != pcnstr; ul++)
		{
			CColRef *pcrPartKey = PcrExtractPartKey(pdrgpdrgpcrPartKey, ul);
			CConstraint *pcnstrLevel = pcnstr->Pcnstr(mp, pcrPartKey);
			if (NULL == pcnstrLevel)
			{
				pcnstrLevel = CConstraintInterval::PciUnbounded(
					mp, pcrPartKey, true /*is_null*/);
			}

			if (NULL != pcnstrLevel)
			{
#ifdef GPOS_DEBUG
				BOOL result =
#endif	// GPOS_DEBUG
					phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ul), pcnstrLevel);
				GPOS_ASSERT(result);
			}
		}
		CRefCount::SafeRelease(pcnstr);
	}

	pexprPartCnstr->Release();

	CBitSet *pbsDefaultParts =
		Pbs(mp, mdpart_constraint->GetDefaultPartsArray());
	pdrgpdrgpcrPartKey->AddRef();

	return GPOS_NEW(mp) CPartConstraint(
		mp, phmulcnstr, pbsDefaultParts,
		mdpart_constraint->IsConstraintUnbounded(), pdrgpdrgpcrPartKey);
}

// Helper to create a dummy constant table expression;
// the table has one boolean column with value True and one row
CExpression *
CUtils::PexprLogicalCTGDummy(CMemoryPool *mp)
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a bool column
	const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
	CColRef *colref =
		col_factory->PcrCreate(pmdtypebool, default_type_modifier);
	CColRefArray *pdrgpcrCTG = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrCTG->Append(colref);

	// generate a bool datum
	IDatumArray *pdrgpdatum = GPOS_NEW(mp) IDatumArray(mp);
	IDatumBool *datum =
		pmdtypebool->CreateBoolDatum(mp, false /*value*/, false /*is_null*/);
	pdrgpdatum->Append(datum);
	IDatum2dArray *pdrgpdrgpdatum = GPOS_NEW(mp) IDatum2dArray(mp);
	pdrgpdrgpdatum->Append(pdrgpdatum);

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalConstTableGet(mp, pdrgpcrCTG, pdrgpdrgpdatum));
}

// map a column from source array to destination array based on position
CColRef *
CUtils::PcrMap(CColRef *pcrSource, CColRefArray *pdrgpcrSource,
			   CColRefArray *pdrgpcrTarget)
{
	const ULONG num_cols = pdrgpcrSource->Size();
	GPOS_ASSERT(num_cols == pdrgpcrTarget->Size());

	CColRef *pcrTarget = NULL;
	for (ULONG ul = 0; NULL == pcrTarget && ul < num_cols; ul++)
	{
		if ((*pdrgpcrSource)[ul] == pcrSource)
		{
			pcrTarget = (*pdrgpcrTarget)[ul];
		}
	}

	return pcrTarget;
}

// check if the given operator is a motion and the derived relational
// properties contain a consumer which is not in the required part consumers
BOOL
CUtils::FMotionOverUnresolvedPartConsumers(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CPartIndexMap *ppimReqd)
{
	GPOS_ASSERT(NULL != ppimReqd);

	if (!FPhysicalMotion(exprhdl.Pop()))
	{
		return false;
	}

	CPartIndexMap *ppimDrvd = exprhdl.Pdpplan(0 /*child_index*/)->Ppim();
	ULongPtrArray *pdrgpulScanIds =
		ppimDrvd->PdrgpulScanIds(mp, true /*fConsumersOnly*/);
	BOOL fHasUnresolvedConsumers = false;

	const ULONG ulConsumers = pdrgpulScanIds->Size();
	if (0 < ulConsumers && !ppimReqd->FContainsUnresolved())
	{
		fHasUnresolvedConsumers = true;
	}

	for (ULONG ul = 0; !fHasUnresolvedConsumers && ul < ulConsumers; ul++)
	{
		ULONG *pulScanId = (*pdrgpulScanIds)[ul];
		if (!ppimReqd->Contains(*pulScanId))
		{
			// there is an unresolved consumer which is not included in the
			// requirements and will therefore be resolved elsewhere
			fHasUnresolvedConsumers = true;
		}
	}

	pdrgpulScanIds->Release();

	return fHasUnresolvedConsumers;
}

// Check if duplicate values can be generated when executing the given Motion expression,
// duplicates occur if Motion's input has replicated/universal distribution,
// which means that we have exactly the same copy of input on each host,
BOOL
CUtils::FDuplicateHazardMotion(CExpression *pexprMotion)
{
	GPOS_ASSERT(NULL != pexprMotion);
	GPOS_ASSERT(CUtils::FPhysicalMotion(pexprMotion->Pop()));

	CExpression *pexprChild = (*pexprMotion)[0];
	CDrvdPropPlan *pdpplanChild =
		CDrvdPropPlan::Pdpplan(pexprChild->PdpDerive());
	CDistributionSpec *pdsChild = pdpplanChild->Pds();
	CDistributionSpec::EDistributionType edtChild = pdsChild->Edt();

	BOOL fReplicatedInput = CDistributionSpec::EdtReplicated == edtChild ||
							CDistributionSpec::EdtUniversal == edtChild;

	return fReplicatedInput;
}

// Collapse the top two project nodes like this, if unable return NULL;
//
// clang-format off
//	+--CLogicalProject                                            <-- pexpr
//		|--CLogicalProject                                        <-- pexprRel
//		|  |--CLogicalGet "t" ("t"), Columns: ["a" (0), "b" (1)]  <-- pexprChildRel
//		|  +--CScalarProjectList                                  <-- pexprChildScalar
//		|     +--CScalarProjectElement "c" (9)
//		|        +--CScalarFunc ()
//		|           |--CScalarIdent "a" (0)
//		|           +--CScalarConst ()
//		+--CScalarProjectList                                     <-- pexprScalar
//		   +--CScalarProjectElement "d" (10)                      <-- pexprPrE
//			  +--CScalarFunc ()
//				 |--CScalarIdent "b" (1)
//				 +--CScalarConst ()
// clang-format on
CExpression *
CUtils::PexprCollapseProjects(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (pexpr->Pop()->Eopid() != COperator::EopLogicalProject)
	{
		// not a project node
		return NULL;
	}

	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	if (pexprRel->Pop()->Eopid() != COperator::EopLogicalProject)
	{
		// not a project node
		return NULL;
	}

	CExpression *pexprChildRel = (*pexprRel)[0];
	CExpression *pexprChildScalar = (*pexprRel)[1];

	CColRefSet *pcrsDefinedChild =
		GPOS_NEW(mp) CColRefSet(mp, *pexprChildScalar->DeriveDefinedColumns());

	// array of project elements for the new child project node
	CExpressionArray *pdrgpexprPrElChild = GPOS_NEW(mp) CExpressionArray(mp);

	// array of project elements that have set returning scalar functions that can be collapsed
	CExpressionArray *pdrgpexprSetReturnFunc =
		GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulCollapsableSetReturnFunc = 0;

	BOOL fChildProjElHasSetReturn =
		pexprChildScalar->DeriveHasNonScalarFunction();

	// iterate over the parent project elements and see if we can add it to the child's project node
	CExpressionArray *pdrgpexprPrEl = GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulLenPr = pexprScalar->Arity();
	for (ULONG ul1 = 0; ul1 < ulLenPr; ul1++)
	{
		CExpression *pexprPrE = (*pexprScalar)[ul1];

		CColRefSet *pcrsUsed =
			GPOS_NEW(mp) CColRefSet(mp, *pexprPrE->DeriveUsedColumns());

		pexprPrE->AddRef();

		BOOL fHasSetReturn = pexprPrE->DeriveHasNonScalarFunction();

		pcrsUsed->Intersection(pcrsDefinedChild);
		ULONG ulIntersect = pcrsUsed->Size();

		if (fHasSetReturn)
		{
			pdrgpexprSetReturnFunc->Append(pexprPrE);

			if (0 == ulIntersect)
			{
				// there are no columns from the relational child that this project element uses
				ulCollapsableSetReturnFunc++;
			}
		}
		else if (0 == ulIntersect)
		{
			pdrgpexprPrElChild->Append(pexprPrE);
		}
		else
		{
			pdrgpexprPrEl->Append(pexprPrE);
		}

		pcrsUsed->Release();
	}

	const ULONG ulTotalSetRetFunc = pdrgpexprSetReturnFunc->Size();

	if (!fChildProjElHasSetReturn &&
		ulCollapsableSetReturnFunc == ulTotalSetRetFunc)
	{
		// there are set returning functions and
		// all of the set returning functions are collapsabile
		AppendArrayExpr(pdrgpexprSetReturnFunc, pdrgpexprPrElChild);
	}
	else
	{
		// We come here when either
		// 1. None of the parent's project element use a set retuning function
		// 2. Both parent's and relation child's project list has atleast one project element using set
		// returning functions. In this case we should not collapsed into one project to ensure for correctness.
		// 3. In the parent's project list there exists a project element with set returning functions that
		// cannot be collapsed. If the parent's project list has more than one project element with
		// set returning functions we should either collapse all of them or none of them for correctness.

		AppendArrayExpr(pdrgpexprSetReturnFunc, pdrgpexprPrEl);
	}

	// clean up
	pcrsDefinedChild->Release();
	pdrgpexprSetReturnFunc->Release();

	// add all project elements of the origin child project node
	ULONG ulLenChild = pexprChildScalar->Arity();
	for (ULONG ul = 0; ul < ulLenChild; ul++)
	{
		CExpression *pexprPrE = (*pexprChildScalar)[ul];
		pexprPrE->AddRef();
		pdrgpexprPrElChild->Append(pexprPrE);
	}

	if (ulLenPr == pdrgpexprPrEl->Size())
	{
		// no candidate project element found for collapsing
		pdrgpexprPrElChild->Release();
		pdrgpexprPrEl->Release();

		return NULL;
	}

	pexprChildRel->AddRef();
	CExpression *pexprProject = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalProject(mp), pexprChildRel,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElChild));

	if (0 == pdrgpexprPrEl->Size())
	{
		// no residual project elements
		pdrgpexprPrEl->Release();

		return pexprProject;
	}

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalProject(mp), pexprProject,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrEl));
}

// append expressions in the source array to destination array
void
CUtils::AppendArrayExpr(CExpressionArray *pdrgpexprSrc,
						CExpressionArray *pdrgpexprDest)
{
	GPOS_ASSERT(NULL != pdrgpexprSrc);
	GPOS_ASSERT(NULL != pdrgpexprDest);

	ULONG length = pdrgpexprSrc->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexprPrE = (*pdrgpexprSrc)[ul];
		pexprPrE->AddRef();
		pdrgpexprDest->Append(pexprPrE);
	}
}

// compares two datums. Takes pointer pointer to a datums.
INT
CUtils::IDatumCmp(const void *val1, const void *val2)
{
	const IDatum *dat1 = *(IDatum **) (val1);
	const IDatum *dat2 = *(IDatum **) (val2);

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();

	if (pcomp->Equals(dat1, dat2))
	{
		return 0;
	}
	else if (pcomp->IsLessThan(dat1, dat2))
	{
		return -1;
	}

	return 1;
}

// compares two points. Takes pointer pointer to a CPoint.
INT
CUtils::CPointCmp(const void *val1, const void *val2)
{
	const CPoint *p1 = *(CPoint **) (val1);
	const CPoint *p2 = *(CPoint **) (val2);

	if (p1->Equals(p2))
	{
		return 0;
	}
	else if (p1->IsLessThan(p2))
	{
		return -1;
	}

	GPOS_ASSERT(p1->IsGreaterThan(p2));
	return 1;
}

// check if the equivalance classes are disjoint
BOOL
CUtils::FEquivalanceClassesDisjoint(CMemoryPool *mp,
									const CColRefSetArray *pdrgpcrs)
{
	const ULONG length = pdrgpcrs->Size();

	// nothing to check
	if (length == 0)
	{
		return true;
	}

	ColRefToColRefSetMap *phmcscrs = GPOS_NEW(mp) ColRefToColRefSetMap(mp);

	// populate a hashmap in this loop
	for (ULONG ulFst = 0; ulFst < length; ulFst++)
	{
		CColRefSet *pcrsFst = (*pdrgpcrs)[ulFst];

		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();

			// if there is already an entry for the column referance it means the column is
			// part of another set which shows the equivalance classes are not disjoint
			if (!phmcscrs->Insert(colref, pcrsFst))
			{
				phmcscrs->Release();
				pcrsFst->Release();
				return false;
			}
		}
	}

	phmcscrs->Release();
	return true;
}

// check if the equivalance classes are same
BOOL
CUtils::FEquivalanceClassesEqual(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst,
								 CColRefSetArray *pdrgpcrsSnd)
{
	const ULONG ulLenFrst = pdrgpcrsFst->Size();
	const ULONG ulLenSecond = pdrgpcrsSnd->Size();

	if (ulLenFrst != ulLenSecond)
		return false;

	ColRefToColRefSetMap *phmcscrs = GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	for (ULONG ulFst = 0; ulFst < ulLenFrst; ulFst++)
	{
		CColRefSet *pcrsFst = (*pdrgpcrsFst)[ulFst];
		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();
			phmcscrs->Insert(colref, pcrsFst);
		}
	}

	for (ULONG ulSnd = 0; ulSnd < ulLenSecond; ulSnd++)
	{
		CColRefSet *pcrsSnd = (*pdrgpcrsSnd)[ulSnd];
		CColRef *colref = pcrsSnd->PcrAny();
		CColRefSet *pcrs = phmcscrs->Find(colref);
		if (!pcrsSnd->Equals(pcrs))
		{
			phmcscrs->Release();
			return false;
		}
	}
	phmcscrs->Release();
	return true;
}

// This function provides a check for a plan with CTE, if both
// CTEProducer and CTEConsumer are executed on the same locality.
// If it is not the case, the plan is bogus and cannot be executed
// by the executor, therefore it throws an exception causing fallback
// to planner.
//
// The overall algorithm for detecting CTE producer and consumer
// inconsistency employs a HashMap while preorder traversing the tree.
// Preorder traversal will guarantee that we visit the producer before
// we visit the consumer. In this regard, when we see a CTE producer,
// we add its CTE id as a key and its execution locality as a value to
// the HashMap.
// And when we encounter the matching CTE consumer while we traverse the
// tree, we check if the locality matches by looking up the CTE id from
// the HashMap. If we see a non-matching locality, we report the
// anamoly.
//
// We change the locality and push it down the tree whenever we detect
// a motion and the motion type enforces a locality change. We pass the
// locality type by value instead of referance to avoid locality changes
// affect parent and sibling localities.
void
CUtils::ValidateCTEProducerConsumerLocality(
	CMemoryPool *mp, CExpression *pexpr, EExecLocalityType eelt,
	UlongToUlongMap *
		phmulul	 // Hash Map containing the CTE Producer id and its execution locality
)
{
	COperator *pop = pexpr->Pop();
	if (COperator::EopPhysicalCTEProducer == pop->Eopid())
	{
		// record the location (either master or segment or singleton)
		// where the CTE producer is being executed
		ULONG ulCTEID = CPhysicalCTEProducer::PopConvert(pop)->UlCTEId();
		phmulul->Insert(GPOS_NEW(mp) ULONG(ulCTEID), GPOS_NEW(mp) ULONG(eelt));
	}
	else if (COperator::EopPhysicalCTEConsumer == pop->Eopid())
	{
		ULONG ulCTEID = CPhysicalCTEConsumer::PopConvert(pop)->UlCTEId();
		ULONG *pulLocProducer = phmulul->Find(&ulCTEID);

		// check if the CTEConsumer is being executed in the same location
		// as the CTE Producer
		if (NULL == pulLocProducer || *pulLocProducer != (ULONG) eelt)
		{
			phmulul->Release();
			GPOS_RAISE(gpopt::ExmaGPOPT,
					   gpopt::ExmiCTEProducerConsumerMisAligned, ulCTEID);
		}
	}
	// In case of a Gather motion, the execution locality is set to segments
	// since the child of Gather motion executes on segments
	else if (COperator::EopPhysicalMotionGather == pop->Eopid())
	{
		eelt = EeltSegments;
	}
	else if (COperator::EopPhysicalMotionHashDistribute == pop->Eopid() ||
			 COperator::EopPhysicalMotionRandom == pop->Eopid() ||
			 COperator::EopPhysicalMotionBroadcast == pop->Eopid())
	{
		// For any of these physical motions, the outer child's execution needs to be
		// tracked for depending upon the distribution spec
		CDrvdPropPlan *pdpplanChild =
			CDrvdPropPlan::Pdpplan((*pexpr)[0]->PdpDerive());
		CDistributionSpec *pdsChild = pdpplanChild->Pds();

		eelt = CUtils::ExecLocalityType(pdsChild);
	}

	const ULONG length = pexpr->Arity();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];

		if (!pexprChild->Pop()->FScalar())
		{
			ValidateCTEProducerConsumerLocality(mp, pexprChild, eelt, phmulul);
		}
	}
}

// get execution locality type
CUtils::EExecLocalityType
CUtils::ExecLocalityType(CDistributionSpec *pds)
{
	EExecLocalityType eelt;
	if (CDistributionSpec::EdtSingleton == pds->Edt() ||
		CDistributionSpec::EdtStrictSingleton == pds->Edt())
	{
		CDistributionSpecSingleton *pdss =
			CDistributionSpecSingleton::PdssConvert(pds);
		if (pdss->FOnMaster())
		{
			eelt = EeltMaster;
		}
		else
		{
			eelt = EeltSingleton;
		}
	}
	else
	{
		eelt = EeltSegments;
	}
	return eelt;
}

// generate a limit expression on top of the given relational child with the given offset and limit count
CExpression* CUtils::PexprLimit(CMemoryPool *mp, CExpression *pexpr, ULONG ulOffSet, ULONG count)
{
	GPOS_ASSERT(pexpr);

	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	CLogicalLimit *popLimit = GPOS_NEW(mp)
		CLogicalLimit(mp, pos, true /* fGlobal */, true /* fHasCount */,
					  false /*fTopLimitUnderDML*/);
	CExpression *pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, ulOffSet);
	CExpression *pexprLimitCount = CUtils::PexprScalarConstInt8(mp, count);

	return GPOS_NEW(mp) CExpression(mp, popLimit, pexpr, pexprLimitOffset, pexprLimitCount);
}

// generate part oid
BOOL
CUtils::FGeneratePartOid(IMDId *mdid)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(mdid);
	BOOL fInsertSortOnParquet =
		(!GPOS_FTRACE(EopttraceDisableSortForDMLOnParquet) &&
		 pmdrel->RetrieveRelStorageType() ==
			 IMDRelation::ErelstorageAppendOnlyParquet);

	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	BOOL fInsertSortOnRows =
		(pmdrel->RetrieveRelStorageType() ==
		 IMDRelation::ErelstorageAppendOnlyRows) &&
		(optimizer_config->GetHint()->UlMinNumOfPartsToRequireSortOnInsert() <=
		 pmdrel->PartitionCount());

	return fInsertSortOnParquet || fInsertSortOnRows;
}

// check if a given operator is a ANY subquery
BOOL
CUtils::FAnySubquery(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);

	BOOL fInSubquery = false;
	if (COperator::EopScalarSubqueryAny == pop->Eopid())
	{
		fInSubquery = true;
	}


	return fInSubquery;
}

// returns the expression under the Nth project element of a CLogicalProject
CExpression *
CUtils::PNthProjectElementExpr(CExpression *pexpr, ULONG ul)
{
	GPOS_ASSERT(pexpr->Pop()->Eopid() == COperator::EopLogicalProject);

	// Logical Project's first child is relational child and the second
	// child is the project list. We initially get the project list and then
	// the nth element in the project list and finally expression under that
	// element.
	return (*(*(*pexpr)[1])[ul])[0];
}

// check if the Project list has an inner reference assuming project list has one projecet element
BOOL
CUtils::FInnerRefInProjectList(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalProject == pexpr->Pop()->Eopid());

	// extract output columns of the relational child
	CColRefSet *pcrsOuterOutput = (*pexpr)[0]->DeriveOutputColumns();

	// Project List with one project element
	CExpression *pexprInner = (*pexpr)[1];
	GPOS_ASSERT(1 == pexprInner->Arity());
	BOOL fExprHasAnyCrFromCrs =
		CUtils::FExprHasAnyCrFromCrs(pexprInner, pcrsOuterOutput);

	return fExprHasAnyCrFromCrs;
}

// Check if expression tree has a col being referenced in the CColRefSet passed as input
BOOL
CUtils::FExprHasAnyCrFromCrs(CExpression *pexpr, CColRefSet *pcrs)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pcrs);
	CColRef *colref = NULL;

	COperator::EOperatorId op_id = pexpr->Pop()->Eopid();
	switch (op_id)
	{
		case COperator::EopScalarProjectElement:
		{
			// check project elements
			CScalarProjectElement *popScalarProjectElement =
				CScalarProjectElement::PopConvert(pexpr->Pop());
			colref = (CColRef *) popScalarProjectElement->Pcr();
			if (pcrs->FMember(colref))
				return true;
			break;
		}
		case COperator::EopScalarIdent:
		{
			// Check scalarIdents
			CScalarIdent *popScalarOp = CScalarIdent::PopConvert(pexpr->Pop());
			colref = (CColRef *) popScalarOp->Pcr();
			if (pcrs->FMember(colref))
				return true;
			break;
		}
		default:
			break;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FExprHasAnyCrFromCrs((*pexpr)[ul], pcrs))
			return true;
	}

	return false;
}

// returns true if expression contains aggregate window function
BOOL
CUtils::FHasAggWindowFunc(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopScalarWindowFunc == pexpr->Pop()->Eopid())
	{
		CScalarWindowFunc *popScWindowFunc =
			CScalarWindowFunc::PopConvert(pexpr->Pop());
		return popScWindowFunc->FAgg();
	}

	// process children
	BOOL fHasAggWindowFunc = false;
	for (ULONG ul = 0; !fHasAggWindowFunc && ul < pexpr->Arity(); ul++)
	{
		fHasAggWindowFunc = FHasAggWindowFunc((*pexpr)[ul]);
	}

	return fHasAggWindowFunc;
}

BOOL
CUtils::FCrossJoin(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fCrossJoin = false;
	if (pexpr->Pop()->Eopid() == COperator::EopLogicalInnerJoin)
	{
		GPOS_ASSERT(3 == pexpr->Arity());
		CExpression *pexprPred = (*pexpr)[2];
		if (CUtils::FScalarConstTrue(pexprPred))
			fCrossJoin = true;
	}
	return fCrossJoin;
}

// Determine whether a scalar expression consists only of a scalar id and NDV-preserving
// functions plus casts. If so, return the corresponding CColRef.
BOOL
CUtils::IsExprNDVPreserving(CExpression *pexpr,
							const CColRef **underlying_colref)
{
	CExpression *curr_expr = pexpr;

	*underlying_colref = NULL;

	// go down the expression tree, visiting the child containing a scalar ident until
	// we found the ident or until we found a non-NDV-preserving function (at which point there
	// is no more need to check)
	while (1)
	{
		COperator *pop = curr_expr->Pop();
		ULONG child_with_scalar_ident = 0;

		switch (pop->Eopid())
		{
			case COperator::EopScalarIdent:
			{
				// we reached the bottom of the expression, return the ColRef
				CScalarIdent *cr = CScalarIdent::PopConvert(pop);

				*underlying_colref = cr->Pcr();
				GPOS_ASSERT(1 == pexpr->DeriveUsedColumns()->Size());
				return true;
			}

			case COperator::EopScalarCast:
				// skip over casts
				// Note: We might in the future investigate whether there are some casts
				// that reduce NDVs by too much. Most, if not all, casts that have that potential are
				// converted to functions, though. Examples: timestamp -> date, double precision -> int.
				break;

			case COperator::EopScalarCoalesce:
			{
				// coalesce(col, const1, ... constn) is treated as an NDV-preserving function
				for (ULONG c = 1; c < curr_expr->Arity(); c++)
				{
					if (0 < (*curr_expr)[c]->DeriveUsedColumns()->Size())
					{
						// this coalesce has a ColRef in the second or later arguments, assume for
						// now that this doesn't preserve NDVs (we could add logic to support this case later)
						return false;
					}
				}
				break;
			}
			case COperator::EopScalarFunc:
			{
				// check whether the function is NDV-preserving
				CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
				CScalarFunc *sf = CScalarFunc::PopConvert(pop);
				const IMDFunction *pmdfunc =
					md_accessor->RetrieveFunc(sf->FuncMdId());

				if (!pmdfunc->IsNDVPreserving() || 1 != curr_expr->Arity())
				{
					return false;
				}
				break;
			}

			case COperator::EopScalarOp:
			{
				CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
				CScalarOp *so = CScalarOp::PopConvert(pop);
				const IMDScalarOp *pmdscop = md_accessor->RetrieveScOp(so->MdIdOp());

				if (!pmdscop->IsNDVPreserving() || 2 != curr_expr->Arity())
				{
					return false;
				}

				// col <op> const is NDV-preserving, and so is const <op> col
				if (0 == (*curr_expr)[1]->DeriveUsedColumns()->Size())
				{
					// col <op> const
					child_with_scalar_ident = 0;
				}
				else if (0 == (*curr_expr)[0]->DeriveUsedColumns()->Size())
				{
					// const <op> col
					child_with_scalar_ident = 1;
				}
				else
				{
					// give up for now, both children reference a column,
					// e.g. col1 <op> col2
					return false;
				}
				break;
			}

			default:
				// anything else we see is considered non-NDV-preserving
				return false;
		}

		curr_expr = (*curr_expr)[child_with_scalar_ident];
	}
}


// search the given array of predicates for predicates with equality or IS NOT
// DISTINCT FROM operators that has one side equal to the given expression, if
// found, return the other side of equality, otherwise return NULL
CExpression *
CUtils::PexprMatchEqualityOrINDF(
	CExpression *pexprToMatch,
	CExpressionArray *pdrgpexpr	 // array of predicates to inspect
)
{
	GPOS_ASSERT(NULL != pexprToMatch);
	GPOS_ASSERT(NULL != pdrgpexpr);

	CExpression *pexprMatching = NULL;
	const ULONG ulSize = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		CExpression *pexprPredOuter, *pexprPredInner;


		if (CPredicateUtils::IsEqualityOp(pexprPred))
		{
			pexprPredOuter = (*pexprPred)[0];
			pexprPredInner = (*pexprPred)[1];
		}
		else if (CPredicateUtils::FINDF(pexprPred))
		{
			pexprPredOuter = (*(*pexprPred)[0])[0];
			pexprPredInner = (*(*pexprPred)[0])[1];
		}
		else
		{
			continue;
		}

		IMDId *pmdidTypeOuter =
			CScalar::PopConvert(pexprPredOuter->Pop())->MdidType();
		IMDId *pmdidTypeInner =
			CScalar::PopConvert(pexprPredInner->Pop())->MdidType();
		if (!pmdidTypeOuter->Equals(pmdidTypeInner))
		{
			// only consider equality of identical types
			continue;
		}

		pexprToMatch =
			CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprToMatch);
		if (CUtils::Equals(
				CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredOuter),
				pexprToMatch))
		{
			pexprMatching = pexprPredInner;
			break;
		}

		if (CUtils::Equals(
				CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredInner),
				pexprToMatch))
		{
			pexprMatching = pexprPredOuter;
			break;
		}
	}

	if (NULL != pexprMatching)
		return CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprMatching);
	return pexprMatching;
}

// from the input join expression, remove the inferred predicates
// and return the new join expression without inferred predicate
CExpression *
CUtils::MakeJoinWithoutInferredPreds(CMemoryPool *mp, CExpression *join_expr)
{
	GPOS_ASSERT(COperator::EopLogicalInnerJoin == join_expr->Pop()->Eopid());

	CExpressionHandle expression_handle(mp);
	expression_handle.Attach(join_expr);
	CExpression *scalar_expr = expression_handle.PexprScalarExactChild(
		join_expr->Arity() - 1, true /*error_on_null_return*/);
	CExpression *scalar_expr_without_inferred_pred =
		CPredicateUtils::PexprRemoveImpliedConjuncts(mp, scalar_expr,
													 expression_handle);

	// create a new join expression using the scalar expr without inferred predicate
	CExpression *left_child_expr = (*join_expr)[0];
	CExpression *right_child_expr = (*join_expr)[1];
	left_child_expr->AddRef();
	right_child_expr->AddRef();
	COperator *join_op = join_expr->Pop();
	join_op->AddRef();
	return GPOS_NEW(mp)
		CExpression(mp, join_op, left_child_expr, right_child_expr,
					scalar_expr_without_inferred_pred);
}

// check if the input expr array contains the expr
BOOL
CUtils::Contains(const CExpressionArray *exprs, CExpression *expr_to_match)
{
	if (NULL == exprs)
	{
		return false;
	}

	BOOL contains = false;
	for (ULONG ul = 0; ul < exprs->Size() && !contains; ul++)
	{
		CExpression *expr = (*exprs)[ul];
		contains = CUtils::Equals(expr, expr_to_match);
	}
	return contains;
}

BOOL
CUtils::Equals(const CExpressionArrays *exprs_arr,
			   const CExpressionArrays *other_exprs_arr)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL arrays are equal
	if (NULL == exprs_arr || NULL == other_exprs_arr)
	{
		return NULL == exprs_arr && NULL == other_exprs_arr;
	}

	// do pointer comparision
	if (exprs_arr == other_exprs_arr)
	{
		return true;
	}

	// if the size is not equal, the two arrays are not equal
	if (exprs_arr->Size() != other_exprs_arr->Size())
	{
		return false;
	}

	// if all the elements are equal, then both the arrays are equal
	BOOL equal = true;
	for (ULONG id = 0; id < exprs_arr->Size() && equal; id++)
	{
		equal = CUtils::Equals((*exprs_arr)[id], (*other_exprs_arr)[id]);
	}
	return equal;
}

// operators from which the inferred predicates can be removed
// NB: currently, only inner join is included, but we can add more later.
BOOL
CUtils::CanRemoveInferredPredicates(COperator::EOperatorId op_id)
{
	return op_id == COperator::EopLogicalInnerJoin;
}

CExpressionArrays *
CUtils::GetCombinedExpressionArrays(CMemoryPool *mp,
									CExpressionArrays *exprs_array,
									CExpressionArrays *exprs_array_other)
{
	CExpressionArrays *result_exprs = GPOS_NEW(mp) CExpressionArrays(mp);
	AddExprs(result_exprs, exprs_array);
	AddExprs(result_exprs, exprs_array_other);

	return result_exprs;
}

void
CUtils::AddExprs(CExpressionArrays *results_exprs,
				 CExpressionArrays *input_exprs)
{
	GPOS_ASSERT(NULL != results_exprs);
	GPOS_ASSERT(NULL != input_exprs);
	for (ULONG ul = 0; ul < input_exprs->Size(); ul++)
	{
		CExpressionArray *exprs = (*input_exprs)[ul];
		exprs->AddRef();
		results_exprs->Append(exprs);
	}
	GPOS_ASSERT(results_exprs->Size() >= input_exprs->Size());
}