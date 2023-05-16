//---------------------------------------------------------------------------
//	@filename:
//		CScalarWindowFunc.cpp
//
//	@doc:
//		Implementation of scalar window function call operators
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarWindowFunc.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CScalarFunc.h"
#include "duckdb/optimizer/cascade/md/IMDAggregate.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::CScalarWindowFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarWindowFunc::CScalarWindowFunc(CMemoryPool *mp, IMDId *mdid_func,
									 IMDId *mdid_return_type,
									 const CWStringConst *pstrFunc,
									 EWinStage ewinstage, BOOL is_distinct,
									 BOOL is_star_arg, BOOL is_simple_agg)
	: CScalarFunc(mp),
	  m_ewinstage(ewinstage),
	  m_is_distinct(is_distinct),
	  m_is_star_arg(is_star_arg),
	  m_is_simple_agg(is_simple_agg),
	  m_fAgg(false)
{
	GPOS_ASSERT(mdid_func->IsValid());
	GPOS_ASSERT(mdid_return_type->IsValid());
	m_func_mdid = mdid_func;
	m_return_type_mdid = mdid_return_type;
	m_pstrFunc = pstrFunc;

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fAgg = md_accessor->FAggWindowFunc(m_func_mdid);
	if (!m_fAgg)
	{
		const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(m_func_mdid);
		m_efs = pmdfunc->GetFuncStability();
		m_efda = pmdfunc->GetFuncDataAccess();
	}
	else
	{
		// TODO: , Aug 15, 2012; pull out properties of aggregate functions
		m_efs = IMDFunction::EfsImmutable;
		m_efda = IMDFunction::EfdaNoSQL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarWindowFunc::HashValue() const
{
	return gpos::CombineHashes(
		CombineHashes(
			CombineHashes(
				CombineHashes(
					gpos::CombineHashes(
						COperator::HashValue(),
						gpos::CombineHashes(m_func_mdid->HashValue(),
											m_return_type_mdid->HashValue())),
					m_ewinstage),
				gpos::HashValue<BOOL>(&m_is_distinct)),
			gpos::HashValue<BOOL>(&m_is_star_arg)),
		gpos::HashValue<BOOL>(&m_is_simple_agg));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarWindowFunc::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarWindowFunc *popFunc = CScalarWindowFunc::PopConvert(pop);

		// match if the func id, and properties are identical
		return ((popFunc->IsDistinct() == m_is_distinct) &&
				(popFunc->IsStarArg() == m_is_star_arg) &&
				(popFunc->IsSimpleAgg() == m_is_simple_agg) &&
				(popFunc->FAgg() == m_fAgg) &&
				m_func_mdid->Equals(popFunc->FuncMdId()) &&
				m_return_type_mdid->Equals(popFunc->MdidType()) &&
				(popFunc->Ews() == m_ewinstage));
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream & CScalarWindowFunc::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << PstrFunc()->GetBuffer();
	os << " , Agg: " << (m_fAgg ? "true" : "false");
	os << " , Distinct: " << (m_is_distinct ? "true" : "false");
	os << " , StarArgument: " << (m_is_star_arg ? "true" : "false");
	os << " , SimpleAgg: " << (m_is_simple_agg ? "true" : "false");
	os << ")";
	return os;
}