//---------------------------------------------------------------------------
//	@filename:
//		CScalarFunc.cpp
//
//	@doc:
//		Implementation of scalar function call operators
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarFunc.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::CScalarFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarFunc::CScalarFunc(CMemoryPool *mp)
	: CScalar(mp),
	  m_func_mdid(NULL),
	  m_return_type_mdid(NULL),
	  m_return_type_modifier(default_type_modifier),
	  m_pstrFunc(NULL),
	  m_efs(IMDFunction::EfsSentinel),
	  m_efda(IMDFunction::EfdaSentinel),
	  m_returns_set(false),
	  m_returns_null_on_null_input(false),
	  m_fBoolReturnType(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::CScalarFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarFunc::CScalarFunc(CMemoryPool *mp, IMDId *mdid_func,
						 IMDId *mdid_return_type, INT return_type_modifier,
						 const CWStringConst *pstrFunc)
	: CScalar(mp),
	  m_func_mdid(mdid_func),
	  m_return_type_mdid(mdid_return_type),
	  m_return_type_modifier(return_type_modifier),
	  m_pstrFunc(pstrFunc),
	  m_returns_set(false),
	  m_returns_null_on_null_input(false),
	  m_fBoolReturnType(false)
{
	GPOS_ASSERT(mdid_func->IsValid());
	GPOS_ASSERT(mdid_return_type->IsValid());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(m_func_mdid);

	m_efs = pmdfunc->GetFuncStability();
	m_efda = pmdfunc->GetFuncDataAccess();
	m_returns_set = pmdfunc->ReturnsSet();

	m_returns_null_on_null_input = pmdfunc->IsStrict();
	m_fBoolReturnType =
		CMDAccessorUtils::FBoolType(md_accessor, m_return_type_mdid);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::~CScalarFunc
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarFunc::~CScalarFunc()
{
	CRefCount::SafeRelease(m_func_mdid);
	CRefCount::SafeRelease(m_return_type_mdid);
	GPOS_DELETE(m_pstrFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::PstrFunc
//
//	@doc:
//		Function name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarFunc::PstrFunc() const
{
	return m_pstrFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::FuncMdId
//
//	@doc:
//		Func id
//
//---------------------------------------------------------------------------
IMDId *
CScalarFunc::FuncMdId() const
{
	return m_func_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::EFuncStbl
//
//	@doc:
//		Function stability enum
//
//---------------------------------------------------------------------------
IMDFunction::EFuncStbl
CScalarFunc::EfsGetFunctionStability() const
{
	return m_efs;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		id of function
//
//---------------------------------------------------------------------------
ULONG
CScalarFunc::HashValue() const
{
	return gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(m_func_mdid->HashValue(),
							m_return_type_mdid->HashValue()));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarFunc::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CScalarFunc *popScFunc = CScalarFunc::PopConvert(pop);

	// match if func ids are identical
	return popScFunc->FuncMdId()->Equals(m_func_mdid) &&
		   popScFunc->MdidType()->Equals(m_return_type_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarFunc::MdidType() const
{
	return m_return_type_mdid;
}

INT
CScalarFunc::TypeModifier() const
{
	return m_return_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::FHasNonScalarFunction
//
//	@doc:
//		Derive existence of non-scalar functions from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalarFunc::FHasNonScalarFunction(CExpressionHandle &	//exprhdl
)
{
	return m_returns_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarFunc::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << PstrFunc()->GetBuffer();
	os << ")";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarFunc::Eber(ULongPtrArray *pdrgpulChildren) const
{
	if (m_returns_null_on_null_input)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}

	return EberAny;
}