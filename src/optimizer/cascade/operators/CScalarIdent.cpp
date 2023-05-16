//---------------------------------------------------------------------------
//	@filename:
//		CScalarIdent.cpp
//
//	@doc:
//		Implementation of scalar identity operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarIdent.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::HashValue
//
//	@doc:
//		Hash value built from colref and Eop
//
//---------------------------------------------------------------------------
ULONG CScalarIdent::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), gpos::HashPtr<CColRef>(m_pcr));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarIdent *popIdent = CScalarIdent::PopConvert(pop);

		// match if column reference is same
		return Pcr() == popIdent->Pcr();
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarIdent::PopCopyWithRemappedColumns(CMemoryPool *mp,
										 UlongToColRefMap *colref_mapping,
										 BOOL must_exist)
{
	ULONG id = m_pcr->Id();
	CColRef *colref = colref_mapping->Find(&id);
	if (NULL == colref)
	{
		if (must_exist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

			colref = col_factory->PcrCopy(m_pcr);

#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), colref);
			GPOS_ASSERT(result);
		}
		else
		{
			colref = const_cast<CColRef *>(m_pcr);
		}
	}

	return GPOS_NEW(mp) CScalarIdent(mp, colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarIdent::MdidType() const
{
	return m_pcr->RetrieveType()->MDId();
}

INT
CScalarIdent::TypeModifier() const
{
	return m_pcr->TypeModifier();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FCastedScId
//
//	@doc:
// 		Is the given expression a scalar cast of a scalar identifier
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FCastedScId(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	// cast(col1)
	if (COperator::EopScalarCast == pexpr->Pop()->Eopid())
	{
		if (COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid())
		{
			return true;
		}
	}

	return false;
}

BOOL
CScalarIdent::FCastedScId(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != colref);

	if (!FCastedScId(pexpr))
	{
		return false;
	}

	CScalarIdent *pScIdent = CScalarIdent::PopConvert((*pexpr)[0]->Pop());

	return colref == pScIdent->Pcr();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream & CScalarIdent::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	m_pcr->OsPrint(os);

	return os;
}