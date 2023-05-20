//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredPoint.cpp
//
//	@doc:
//		Implementation of statistics filter that compares a column to a constant
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CStatsPredPoint.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"
#include "duckdb/optimizer/cascade/statistics/CPoint.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsPredPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint(ULONG colid, CStatsPred::EStatsCmpType stats_cmp_type, CPoint* point)
	: CStatsPred(colid), m_stats_cmp_type(stats_cmp_type), m_pred_point(point)
{
	GPOS_ASSERT(NULL != point);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsPredPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint(CMemoryPool* mp, const CColRef* colref, CStatsPred::EStatsCmpType stats_cmp_type, IDatum* datum)
	: CStatsPred(gpos::ulong_max), m_stats_cmp_type(stats_cmp_type), m_pred_point(NULL)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != datum);
	m_colid = colref->Id();
	IDatum *padded_datum = PreprocessDatum(mp, colref, datum);
	m_pred_point = GPOS_NEW(mp) CPoint(padded_datum);
}

//---------------------------------------------------------------------------
//		CStatsPredPoint::PreprocessDatum
//
//	@doc:
//		Add padding to datums when needed
//---------------------------------------------------------------------------
IDatum* CStatsPredPoint::PreprocessDatum(CMemoryPool *mp, const CColRef *colref, IDatum *datum)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != datum);
	if (!datum->NeedsPadding() || CColRef::EcrtTable != colref->Ecrt() || datum->IsNull())
	{
		// we do not pad datum for comparison against computed columns
		datum->AddRef();
		return datum;
	}
	const CColRefTable *colref_table = CColRefTable::PcrConvert(const_cast<CColRef *>(colref));
	return datum->MakePaddedDatum(mp, colref_table->Width());
}