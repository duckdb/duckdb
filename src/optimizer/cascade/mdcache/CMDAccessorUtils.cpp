//---------------------------------------------------------------------------
//	@filename:
//		CMDAccessorUtils.cpp
//
//	@doc:
//		Implementation of the utility function associated with the metadata
//		accessor
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CErrorHandlerStandard.h"
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/task/CAutoTraceFlag.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/IMDAggregate.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"
#include "duckdb/optimizer/cascade/md/IMDScCmp.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"

using namespace gpmd;
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::PstrWindowFuncName
//
//	@doc:
//		Return the name of the window operation
//
//---------------------------------------------------------------------------
const CWStringConst *
CMDAccessorUtils::PstrWindowFuncName(CMDAccessor *md_accessor, IMDId *mdid_func)
{
	if (md_accessor->FAggWindowFunc(mdid_func))
	{
		const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(mdid_func);

		return pmdagg->Mdname().GetMDName();
	}

	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(mdid_func);

	return pmdfunc->Mdname().GetMDName();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::PmdidWindowReturnType
//
//	@doc:
//		Return the return type of the window function
//
//---------------------------------------------------------------------------
IMDId *
CMDAccessorUtils::PmdidWindowReturnType(CMDAccessor *md_accessor,
										IMDId *mdid_func)
{
	if (md_accessor->FAggWindowFunc(mdid_func))
	{
		const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(mdid_func);
		return pmdagg->GetResultTypeMdid();
	}

	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(mdid_func);
	return pmdfunc->GetResultTypeMdid();
}

// Does a scalar comparison object between given types exists
BOOL
CMDAccessorUtils::FCmpExists(CMDAccessor *md_accessor, IMDId *left_mdid,
							 IMDId *right_mdid, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	GPOS_TRY
	{
		(void) GetScCmpMdid(md_accessor, left_mdid, right_mdid, cmp_type);

		return true;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(
			GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;

		return false;
	}
	GPOS_CATCH_END;
}

// Get the mdid of the scalar comparison between the given types.
// Throws an exception if no such comparison exists! Use
// CMDAccessorUtils::FCmpExists() to check that before calling this function.
IMDId *
CMDAccessorUtils::GetScCmpMdid(CMDAccessor *md_accessor, IMDId *left_mdid,
							   IMDId *right_mdid, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	IMDId *sc_cmp_mdid;

	// if the left & right are the same, first check the MDType
	if (left_mdid->Equals(right_mdid))
	{
		const IMDType *pmdtypeLeft = md_accessor->RetrieveType(left_mdid);
		sc_cmp_mdid = pmdtypeLeft->GetMdidForCmpType(cmp_type);

		if (IMDId::IsValid(sc_cmp_mdid))
		{
			return sc_cmp_mdid;
		}
	}

	// then check for an explicit operator
	sc_cmp_mdid =
		md_accessor->Pmdsccmp(left_mdid, right_mdid, cmp_type)->MdIdOp();

	// either sc_cmp_mdid is valid or an exception was raised during lookup
	GPOS_ASSERT(IMDId::IsValid(sc_cmp_mdid));

	return sc_cmp_mdid;
}


// check is a comparison between given types or a comparison after casting one
// side to an another exists
BOOL
CMDAccessorUtils::FCmpOrCastedCmpExists(IMDId *left_mdid, IMDId *right_mdid,
										IMDType::ECmpType cmp_type)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	GPOS_TRY
	{
		(void) CMDAccessorUtils::GetScCmpMdIdConsiderCasts(
			md_accessor, left_mdid, right_mdid, cmp_type);

		return true;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(
			GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;

		return false;
	}
	GPOS_CATCH_END;
}

// return the scalar comparison operator id between the two types
// Throws an exception if no such comparison exists! Use
// CMDAccessorUtils::FCmpOrCastedCmpExists() to check that before calling this
// function.
IMDId *
CMDAccessorUtils::GetScCmpMdIdConsiderCasts(CMDAccessor *md_accessor,
											IMDId *left_mdid, IMDId *right_mdid,
											IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != right_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	// left op right
	if (CMDAccessorUtils::FCmpExists(md_accessor, left_mdid, right_mdid,
									 cmp_type))
	{
		return CMDAccessorUtils::GetScCmpMdid(md_accessor, left_mdid,
											  right_mdid, cmp_type);
	}

	// left op cast(right)
	if (CMDAccessorUtils::FCmpExists(md_accessor, left_mdid, left_mdid,
									 cmp_type) &&
		CMDAccessorUtils::FCastExists(md_accessor, right_mdid, left_mdid))
	{
		return CMDAccessorUtils::GetScCmpMdid(md_accessor, left_mdid, left_mdid,
											  cmp_type);
	}

	// cast(left) op right
	if (CMDAccessorUtils::FCmpExists(md_accessor, right_mdid, right_mdid,
									 cmp_type) &&
		CMDAccessorUtils::FCastExists(md_accessor, left_mdid, right_mdid))
	{
		return CMDAccessorUtils::GetScCmpMdid(md_accessor, right_mdid,
											  right_mdid, cmp_type);
	}

	// call to raise an error on non-comparable data types
	return CMDAccessorUtils::GetScCmpMdid(md_accessor, left_mdid, right_mdid,
										  cmp_type);
}

IMDId *
CMDAccessorUtils::GetScCmpMdIdConsiderCasts(CMDAccessor *md_accessor,
											CExpression *pexprLeft,
											CExpression *pexprRight,
											IMDType::ECmpType cmp_type)
{
	IMDId *left_mdid = CScalar::PopConvert(pexprLeft->Pop())->MdidType();
	IMDId *right_mdid = CScalar::PopConvert(pexprRight->Pop())->MdidType();

	return CMDAccessorUtils::GetScCmpMdIdConsiderCasts(md_accessor, left_mdid,
													   right_mdid, cmp_type);
}

void
CMDAccessorUtils::ApplyCastsForScCmp(CMemoryPool *mp, CMDAccessor *md_accessor,
									 CExpression *&pexprLeft,
									 CExpression *&pexprRight, IMDId *op_mdid)
{
	IMDId *left_mdid = CScalar::PopConvert(pexprLeft->Pop())->MdidType();
	IMDId *right_mdid = CScalar::PopConvert(pexprRight->Pop())->MdidType();

	const IMDScalarOp *op = md_accessor->RetrieveScOp(op_mdid);
	IMDId *op_left_mdid = op->GetLeftMdid();
	IMDId *op_right_mdid = op->GetRightMdid();

	// Determine if casts need to be added (following is a description of what to
	// do on the left side, for the right side it's analogous):

	// GetScCmpMdIdConsiderCasts() has determined that
	// a) there is a comparison operator with matching input types (left_mdid equals
	//    op_left_mdid); or
	//
	// b) there is a common type t (either left_mdid or right_mdid) such that we
	//    can cast both left and right side to t, if needed, and that type t has a
	//    valid comparison operator.  Note that the input types of that operator
	//    may or may not be equal to t.  Note that we assume in this case that
	//    casts from t to the operators's input types "op_left_mdid" and
	//    "op_right_mdid" exist, otherwise this cannot be used to compare to
	//    values of type t (proposition A). Unfortunately, when we reach here, we
	//    don't know whether t is "left_mdid" or "right_mdid".
	//
	//    Case b) may require one or two casts. If a direct cast from "left_mdid"
	//    to "op_left_mdid" exists, we can use that directly. Otherwise,
	//    GetScCmpMdIdConsiderCasts() must have picked "right_mdid" as the common
	//    type t and it will have established that a cast from "left_mdid" to
	//    "right_mdid" must exist (proposition B). We know that we can cast from
	//    "left_mdid" to "right_mdid" and then, using the assumption above, from
	//    "right_mdid" to "op_left_mdid".

	if (!op_left_mdid->Equals(left_mdid))
	{
		// We are in case b).
		if (CMDAccessorUtils::FCastExists(md_accessor, left_mdid, op_left_mdid))
		{
			// The simple case, a direct cast exists
			pexprLeft =
				CUtils::PexprCast(mp, md_accessor, pexprLeft, op_left_mdid);
		}
		else
		{
			// validate proposition B
			GPOS_ASSERT(CMDAccessorUtils::FCastExists(md_accessor, left_mdid,
													  right_mdid));
			// Create the two casts described above, first from left to right type
			pexprLeft =
				CUtils::PexprCast(mp, md_accessor, pexprLeft, right_mdid);
			if (!right_mdid->Equals(op_left_mdid))
			{
				// validate proposition A
				GPOS_ASSERT(CMDAccessorUtils::FCastExists(
					md_accessor, right_mdid, op_left_mdid));
				// and then from right type to the type the comparison operator expects
				pexprLeft =
					CUtils::PexprCast(mp, md_accessor, pexprLeft, op_left_mdid);
			}
		}
	}

	if (!op_right_mdid->Equals(right_mdid))
	{
		// We are in case b).
		if (CMDAccessorUtils::FCastExists(md_accessor, right_mdid,
										  op_right_mdid))
		{
			// The simple case, a direct cast exists
			pexprRight =
				CUtils::PexprCast(mp, md_accessor, pexprRight, op_right_mdid);
		}
		else
		{
			// validate proposition B
			GPOS_ASSERT(CMDAccessorUtils::FCastExists(md_accessor, right_mdid,
													  left_mdid));
			// Create the two casts described above, first from right to left type
			pexprRight =
				CUtils::PexprCast(mp, md_accessor, pexprRight, left_mdid);
			if (!left_mdid->Equals(op_right_mdid))
			{
				// validate proposition A
				GPOS_ASSERT(CMDAccessorUtils::FCastExists(
					md_accessor, left_mdid, op_right_mdid));
				// and then from left type to the type the comparison operator expects
				pexprRight = CUtils::PexprCast(mp, md_accessor, pexprRight,
											   op_right_mdid);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::FCastExists
//
//	@doc:
//		Does if a cast object between given source and destination types exists
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FCastExists(CMDAccessor *md_accessor, IMDId *mdid_src,
							  IMDId *mdid_dest)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != mdid_src);
	GPOS_ASSERT(NULL != mdid_dest);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	GPOS_TRY
	{
		(void) md_accessor->Pmdcast(mdid_src, mdid_dest);

		return true;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(
			GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;

		return false;
	}
	GPOS_CATCH_END;
}


//---------------------------------------------------------------------------
//	@function:
//		CUtils::FScalarOpReturnsNullOnNullInput
//
//	@doc:
//		Does scalar operator return NULL on NULL input?
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(CMDAccessor *md_accessor,
												  IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != md_accessor);

	if (NULL == mdid_op || !mdid_op->IsValid())
	{
		// invalid mdid
		return false;
	}

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	GPOS_TRY
	{
		const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid_op);

		return md_scalar_op->ReturnsNullOnNullInput();
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(
			GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CUtils::FBoolType
//
//	@doc:
//		Return True if passed mdid is for BOOL type
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FBoolType(CMDAccessor *md_accessor, IMDId *mdid_type)
{
	GPOS_ASSERT(NULL != md_accessor);

	if (NULL != mdid_type && mdid_type->IsValid())
	{
		return (IMDType::EtiBool ==
				md_accessor->RetrieveType(mdid_type)->GetDatumType());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::FCommutativeScalarOp
//
//	@doc:
//		Is scalar operator commutative? This can be used with ScalarOp and ScalarCmp
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FCommutativeScalarOp(CMDAccessor *md_accessor, IMDId *mdid_op)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != mdid_op);

	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid_op);

	return mdid_op->Equals(md_scalar_op->GetCommuteOpMdid());
}