//---------------------------------------------------------------------------
//	@filename:
//		CScalarArray.cpp
//
//	@doc:
//		Implementation of scalar arrays
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarArray.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDAggregate.h"

using namespace gpopt;
using namespace gpmd;

// Ctor
CScalarArray::CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid, BOOL is_multidimenstional)
	: CScalar(mp), m_pmdidElem(elem_type_mdid), m_pmdidArray(array_type_mdid), m_fMultiDimensional(is_multidimenstional)
{
	GPOS_ASSERT(elem_type_mdid->IsValid());
	GPOS_ASSERT(array_type_mdid->IsValid());
	m_pdrgPconst = GPOS_NEW(mp) CScalarConstArray(mp);
}


// Ctor
CScalarArray::CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid,
						   IMDId *array_type_mdid, BOOL is_multidimenstional,
						   CScalarConstArray *pdrgPconst)
	: CScalar(mp),
	  m_pmdidElem(elem_type_mdid),
	  m_pmdidArray(array_type_mdid),
	  m_fMultiDimensional(is_multidimenstional),
	  m_pdrgPconst(pdrgPconst)
{
	GPOS_ASSERT(elem_type_mdid->IsValid());
	GPOS_ASSERT(array_type_mdid->IsValid());
}

// Dtor
CScalarArray::~CScalarArray()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
	m_pdrgPconst->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::PmdidElem
//
//	@doc:
//		Element type id
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::PmdidElem() const
{
	return m_pmdidElem;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::PmdidArray
//
//	@doc:
//		Array type id
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::PmdidArray() const
{
	return m_pmdidArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::FMultiDimensional
//
//	@doc:
//		Is array multi-dimensional
//
//---------------------------------------------------------------------------
BOOL
CScalarArray::FMultiDimensional() const
{
	return m_fMultiDimensional;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarArray::HashValue() const
{
	return gpos::CombineHashes(
		CombineHashes(m_pmdidElem->HashValue(), m_pmdidArray->HashValue()),
		gpos::HashValue<BOOL>(&m_fMultiDimensional));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArray::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarArray *popArray = CScalarArray::PopConvert(pop);

		// match if components are identical
		if (popArray->FMultiDimensional() == FMultiDimensional() &&
			PmdidElem()->Equals(popArray->PmdidElem()) &&
			PmdidArray()->Equals(popArray->PmdidArray()) &&
			m_pdrgPconst->Size() == popArray->PdrgPconst()->Size())
		{
			for (ULONG ul = 0; ul < m_pdrgPconst->Size(); ul++)
			{
				CScalarConst *popConst1 = (*m_pdrgPconst)[ul];
				CScalarConst *popConst2 = (*popArray->PdrgPconst())[ul];
				if (!popConst1->Matches(popConst2))
				{
					return false;
				}
			}
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::MdidType
//
//	@doc:
//		Type of expression's result
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::MdidType() const
{
	return m_pmdidArray;
}

CScalarConstArray *
CScalarArray::PdrgPconst() const
{
	return m_pdrgPconst;
}

IOstream &
CScalarArray::OsPrint(IOstream &os) const
{
	os << "CScalarArray: {eleMDId: ";
	m_pmdidElem->OsPrint(os);
	os << ", arrayMDId: ";
	m_pmdidArray->OsPrint(os);
	if (m_fMultiDimensional)
	{
		os << ", multidimensional";
	}
	for (ULONG ul = 0; ul < m_pdrgPconst->Size(); ul++)
	{
		os << " ";
		(*m_pdrgPconst)[ul]->OsPrint(os);
	}
	os << "}";
	return os;
}