//---------------------------------------------------------------------------
//	@filename:
//		CXform.cpp
//
//	@doc:
//		Base class for all transformations
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXform.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace gpopt {

//---------------------------------------------------------------------------
//	@function:
//		CXform::FEqualIds
//
//	@doc:
//		Equality function on xform ids
//
//---------------------------------------------------------------------------
BOOL CXform::FEqualIds(const CHAR *sz_id_one, const CHAR *sz_id_two) {
	return 0 == clib::Strcmp(sz_id_one, sz_id_two);
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsIndexJoinXforms
//
//	@doc:
//		Returns a set containing all xforms related to index join.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
bitset<CXform::EXformId::ExfSentinel> CXform::PbsIndexJoinXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfLeftOuterJoin2IndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2IndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoin2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoin2DynamicIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2DynamicIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoin2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2IndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2DynamicIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2PartialDynamicIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2IndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2DynamicIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply);
	return pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsBitmapIndexXforms
//
//	@doc:
//		Returns a set containing all xforms related to bitmap indexes.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
bitset<CXform::EXformId::ExfSentinel> CXform::PbsBitmapIndexXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfSelect2BitmapBoolOp);
	(void)pbs.set(CXform::ExfSelect2DynamicBitmapBoolOp);
	(void)pbs.set(CXform::ExfLeftOuterJoin2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoin2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply);
	return pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsHeterogeneousIndexXforms
//
//	@doc:
//		Returns a set containing all xforms related to heterogeneous indexes.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
bitset<CXform::EXformId::ExfSentinel> CXform::PbsHeterogeneousIndexXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfSelect2PartialDynamicIndexGet);
	(void)pbs.set(CXform::ExfInnerJoin2PartialDynamicIndexGetApply);
	(void)pbs.set(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply);
	return pbs;
}

//	returns a set containing all xforms that generate a plan with hash join
//	Caller takes ownership of the returned set
bitset<CXform::EXformId::ExfSentinel> CXform::PbsHashJoinXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfInnerJoin2HashJoin);
	(void)pbs.set(CXform::ExfLeftOuterJoin2HashJoin);
	(void)pbs.set(CXform::ExfLeftSemiJoin2HashJoin);
	(void)pbs.set(CXform::ExfLeftAntiSemiJoin2HashJoin);
	(void)pbs.set(CXform::ExfLeftAntiSemiJoinNotIn2HashJoinNotIn);
	return pbs;
}

bitset<CXform::EXformId::ExfSentinel> CXform::PbsJoinOrderInQueryXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfJoinAssociativity);
	(void)pbs.set(CXform::ExfJoinCommutativity);
	return pbs;
}

bitset<CXform::EXformId::ExfSentinel> CXform::PbsJoinOrderOnGreedyXforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfJoinAssociativity);
	(void)pbs.set(CXform::ExfJoinCommutativity);
	return pbs;
}

bitset<CXform::EXformId::ExfSentinel> CXform::PbsJoinOrderOnExhaustiveXforms() {
	bitset<ExfSentinel> pbs;
	return pbs;
}

bitset<CXform::EXformId::ExfSentinel> CXform::PbsJoinOrderOnExhaustive2Xforms() {
	bitset<ExfSentinel> pbs;
	(void)pbs.set(CXform::ExfPushDownLeftOuterJoin);
	return pbs;
}

bool CXform::IsApplyOnce() {
	return false;
}
} // namespace gpopt