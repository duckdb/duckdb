//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CBucketTest.cpp
//
//	@doc:
//		Testing operations on histogram buckets
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include "unittest/dxl/statistics/CBucketTest.h"

#include <stdint.h>

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/base/CDatumBoolGPDB.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

// unittest for statistics objects
GPOS_RESULT
CBucketTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] = {
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketInt4),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketBool),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketScale),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketDifference),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketIntersect),
		GPOS_UNITTEST_FUNC(
			CBucketTest::EresUnittest_CBucketMergeCommutativityUnion),
		GPOS_UNITTEST_FUNC(
			CBucketTest::EresUnittest_CBucketMergeCommutativitySameLowerBounds),
		GPOS_UNITTEST_FUNC(
			CBucketTest::EresUnittest_CBucketMergeCommutativitySameUpperBounds),
		GPOS_UNITTEST_FUNC(
			CBucketTest::EresUnittest_CBucketMergeCommutativityUnionAll),
		GPOS_UNITTEST_FUNC(
			CBucketTest::EresUnittest_CBucketMergeCommutativityDoubleDatum),
		GPOS_UNITTEST_FUNC(
			CBucketTest::
				EresUnittest_CBucketMergeCommutativityDoubleDatumSameLowerBounds),
		GPOS_UNITTEST_FUNC(
			CBucketTest::
				EresUnittest_CBucketMergeCommutativityDoubleDatumSameUpperBounds),
	};

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(mp, &mda, NULL /* pceeval */,
					 CTestUtils::GetCostModel(mp));

	return CUnittest::EresExecute(rgutSharedOptCtxt,
								  GPOS_ARRAY_SIZE(rgutSharedOptCtxt));
}

// basic int4 bucket tests;
GPOS_RESULT
CBucketTest::EresUnittest_CBucketInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// generate integer points
	CPoint *point1 = CTestUtils::PpointInt4(mp, 1);
	CPoint *point2 = CTestUtils::PpointInt4(mp, 2);
	CPoint *point3 = CTestUtils::PpointInt4(mp, 3);
	CPoint *point4 = CTestUtils::PpointInt4(mp, 4);
	CPoint *point10 = CTestUtils::PpointInt4(mp, 10);

	// bucket [1,1]
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 1, CDouble(1.0), CDouble(1.0));
	CCardinalityTestUtils::PrintBucket(mp, "b1", bucket1);

	GPOS_RTL_ASSERT_MSG(bucket1->Contains(point1), "[1,1] must contain 1");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == bucket1->GetOverlapPercentage(
											point1, true /*include_point*/),
						"overlap of 1 in [1,1] must 1.0");

	GPOS_RTL_ASSERT_MSG(!bucket1->Contains(point2), "[1,1] must not contain 2");

	// bucket [1,3)
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 3, CDouble(1.0), CDouble(10.0));
	CCardinalityTestUtils::PrintBucket(mp, "b2", bucket2);

	// overlap of [1,2) w.r.t [1,3) should be about 50%
	CDouble overlap =
		bucket2->GetOverlapPercentage(point2, false /*include_point*/);
	GPOS_RTL_ASSERT(0.49 <= overlap && overlap <= 0.51);

	// bucket [1,10)
	CBucket *bucket3 = GPOS_NEW(mp)
		CBucket(CTestUtils::PpointInt4(mp, 1), CTestUtils::PpointInt4(mp, 10),
				true /* is_lower_closed */, true /* is_upper_closed */,
				CDouble(1.0), CDouble(10.0));
	CCardinalityTestUtils::PrintBucket(mp, "b3", bucket3);
	// bucket (1, 5)
	CBucket *bucket4 = GPOS_NEW(mp)
		CBucket(CTestUtils::PpointInt4(mp, 1), CTestUtils::PpointInt4(mp, 5),
				false /* is_lower_closed */, false /* is_upper_closed */,
				CDouble(1.0), CDouble(5.0));
	CCardinalityTestUtils::PrintBucket(mp, "b4", bucket4);
	// bucket [1, 5)
	CBucket *bucket5 = GPOS_NEW(mp)
		CBucket(CTestUtils::PpointInt4(mp, 1), CTestUtils::PpointInt4(mp, 5),
				true /* is_lower_closed */, false /* is_upper_closed */,
				CDouble(1.0), CDouble(5.0));
	CCardinalityTestUtils::PrintBucket(mp, "b5", bucket5);

	// overlap of [1,4) w.r.t [1,10] should be about 30%
	CDouble overlap_open =
		bucket3->GetOverlapPercentage(point4, false /*include_point*/);
	GPOS_RTL_ASSERT(0.29 <= overlap_open && overlap_open <= 0.31);
	// overlap of [1,4] w.r.t [1,10] should be about 40%
	CDouble overlap_closed =
		bucket3->GetOverlapPercentage(point4, true /*include_point*/);
	GPOS_RTL_ASSERT(0.39 <= overlap_closed && overlap_closed <= 0.41);
	// overlap of [1,10) w.r.t [1,10] should be about 90%
	CDouble overlap_bound =
		bucket3->GetOverlapPercentage(point10, false /*include_point*/);
	GPOS_RTL_ASSERT(0.89 <= overlap_bound && overlap_bound <= 0.91);
	// overlap of (1,3) w.r.t (1,5) should be about 33%
	CDouble overlap_open2 =
		bucket4->GetOverlapPercentage(point3, false /*include_point*/);
	GPOS_RTL_ASSERT(0.32 <= overlap_open2 && overlap_open2 <= 0.34);
	// overlap of [1,3) w.r.t [1,5) should be about 50%
	CDouble overlap_closed2 =
		bucket5->GetOverlapPercentage(point3, false /*include_point*/);
	GPOS_RTL_ASSERT(0.49 <= overlap_closed2 && overlap_closed2 <= 0.51);

	// subsumption
	GPOS_RTL_ASSERT(bucket1->Subsumes(bucket1));
	GPOS_RTL_ASSERT(bucket2->Subsumes(bucket1));

	// width
	CDouble width = bucket1->Width();
	GPOS_RTL_ASSERT(0.99 <= width && width <= 1.01);

	// bucket [1,2] and (2,4)
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketInteger(
		mp, 1, 2, true, true, CDouble(1.0), CDouble(1.0));
	CBucket *pbucket4 = CCardinalityTestUtils::PbucketInteger(
		mp, 2, 4, false, false, CDouble(1.0), CDouble(1.0));

	// point IsBefore
	GPOS_RTL_ASSERT_MSG(pbucket4->IsBefore(point2), "2 must be before (2,4)");

	// bucket IsBefore
	GPOS_RTL_ASSERT_MSG(pbucket3->IsBefore(pbucket4),
						"[1,2] must be before (2,4)");

	point1->Release();
	point2->Release();
	point3->Release();
	point4->Release();
	point10->Release();
	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(bucket3);
	GPOS_DELETE(bucket4);
	GPOS_DELETE(bucket5);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);

	return GPOS_OK;
}

// basic BOOL bucket tests;
GPOS_RESULT
CBucketTest::EresUnittest_CBucketBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// generate boolean points
	CPoint *p1 = CTestUtils::PpointBool(mp, true);
	CPoint *p2 = CTestUtils::PpointBool(mp, false);

	// bucket for true
	CBucket *bucket =
		CCardinalityTestUtils::PbucketSingletonBoolVal(mp, true, CDouble(1.0));

	GPOS_RTL_ASSERT_MSG(bucket->Contains(p1), "true bucket must contain true");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == bucket->GetOverlapPercentage(p1),
						"overlap must 1.0");

	GPOS_RTL_ASSERT_MSG(!bucket->Contains(p2),
						"true bucket must not contain false");
	GPOS_RTL_ASSERT_MSG(CDouble(0.0) == bucket->GetOverlapPercentage(p2),
						"overlap must 0.0");

	p1->Release();
	p2->Release();

	GPOS_DELETE(bucket);

	return GPOS_OK;
}

// scaling a bucket
GPOS_RESULT
CBucketTest::EresUnittest_CBucketScale()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// generate integer point
	CPoint *point1 = CTestUtils::PpointInt4(mp, 10);

	// bucket [1,100)
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 100, CDouble(0.5), CDouble(20.0));

	CBucket *bucket2 =
		bucket1->MakeBucketScaleUpper(mp, point1, false /* include_upper */);

	// new bucket [1, 10) must not contain 10
	GPOS_RTL_ASSERT(!bucket2->Contains(point1));

	// new bucket [1, 10) must contain 9
	CPoint *point2 = CTestUtils::PpointInt4(mp, 9);
	GPOS_RTL_ASSERT(bucket2->Contains(point2));
	point2->Release();

	// new bucket's frequency and distinct values must be lesser than original
	GPOS_RTL_ASSERT(bucket2->GetFrequency() < bucket1->GetFrequency());
	GPOS_RTL_ASSERT(bucket2->GetNumDistinct() < bucket1->GetNumDistinct());

	// scale lower
	CBucket *pbucket3 =
		bucket1->MakeBucketScaleLower(mp, point1, true /* include_lower */);
	GPOS_RTL_ASSERT(pbucket3->Contains(point1));

	// scale to a singleton
	CBucket *pbucket4 = bucket1->MakeBucketSingleton(mp, point1);
	GPOS_RTL_ASSERT(pbucket4->GetNumDistinct() < 2.0);

	// clean up
	point1->Release();
	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);

	return GPOS_OK;
}

// difference operation on buckets
GPOS_RESULT
CBucketTest::EresUnittest_CBucketDifference()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// bucket [1,100)
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 100, CDouble(1.0), CDouble(1.0));

	// bucket [50,75)
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 50, 60, CDouble(1.0), CDouble(1.0));

	// bucket [200, 300)
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 200, 300, CDouble(1.0), CDouble(1.0));

	CBucket *pbucket4 = NULL;
	CBucket *pbucket5 = NULL;
	bucket1->Difference(mp, bucket2, &pbucket4, &pbucket5);
	GPOS_RTL_ASSERT(NULL != pbucket4);
	GPOS_RTL_ASSERT(NULL != pbucket5);
	CCardinalityTestUtils::PrintBucket(mp, "pbucket4", pbucket4);
	CCardinalityTestUtils::PrintBucket(mp, "pbucket5", pbucket4);

	CBucket *pbucket6 = NULL;
	CBucket *pbucket7 = NULL;
	bucket1->Difference(mp, pbucket3, &pbucket6, &pbucket7);
	GPOS_RTL_ASSERT(NULL != pbucket6);
	GPOS_RTL_ASSERT(NULL == pbucket7);
	CCardinalityTestUtils::PrintBucket(mp, "pbucket6", pbucket6);

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);
	GPOS_DELETE(pbucket5);
	GPOS_DELETE(pbucket6);

	return GPOS_OK;
}

// intersection of buckets
GPOS_RESULT
CBucketTest::EresUnittest_CBucketIntersect()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	SBucketsIntersectTestElem rgBucketsIntersectTestElem[] = {
		{7, 203, true, true, 3, 213, true, true, true, 7, 203, true,
		 true},	 // overlaps
		{3, 213, true, true, 7, 203, true, true, true, 7, 203, true,
		 true},	 // same as above but reversed
		{
			13,
			103,
			true,
			true,
			2,
			98,
			true,
			true,
			true,
			13,
			98,
			true,
			true,
		},	// subsumes
		{2, 99, true, true, 13, 103, true, true, true, 13, 99, true,
		 true},	 // same as above but reversed
		{0, 5, true, true, 10, 15, false, true, false, -1, -1, false,
		 false},  // negative
		{10, 15, true, true, 0, 5, false, true, false, -1, -1, false,
		 false},  // same as above but reversed
		{0, 5, true, true, 5, 10, true, true, true, 5, 5, true,
		 true},	 // ub of one bucket is the same as lb of the other and both bounds are closed
		{5, 10, true, true, 0, 5, true, true, true, 5, 5, true,
		 true},	 // same as above but reversed
		{0, 5, true, true, 5, 10, false, true, false, -1, -1, false,
		 false},  // ub of one bucket is the same as lb of the other but closing criteria are different
		{5, 10, false, true, 0, 5, true, true, false, -1, -1, false,
		 false},  // same as above but reversed
		{0, 5, true, true, 0, 5, false, true, true, 0, 5, false,
		 true},	 // exact match but only differ in closure of lb
		{0, 5, true, true, 0, 5, true, true, true, 0, 5, true,
		 true},	 // exact match with all bounds closed
		{0, 5, true, false, 0, 5, true, false, true, 0, 5, true,
		 false},  // exact match with ubs open
		{0, 5, false, false, 0, 5, true, false, true, 0, 5, false,
		 false},  // exact match with lbs differ in closure
		{0, 5, true, true, 0, 5, true, false, true, 0, 5, true,
		 false},  // exact match with ubs differ in closure
	};

	const ULONG length = GPOS_ARRAY_SIZE(rgBucketsIntersectTestElem);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CBucket *bucket1 = CCardinalityTestUtils::PbucketInteger(
			mp, rgBucketsIntersectTestElem[ul].m_iLb1,
			rgBucketsIntersectTestElem[ul].m_iUb1,
			rgBucketsIntersectTestElem[ul].m_fLb1Closed,
			rgBucketsIntersectTestElem[ul].m_fUb1Closed, CDouble(0.1),
			CDouble(100.0));

		CBucket *bucket2 = CCardinalityTestUtils::PbucketInteger(
			mp, rgBucketsIntersectTestElem[ul].m_iLb2,
			rgBucketsIntersectTestElem[ul].m_iUb2,
			rgBucketsIntersectTestElem[ul].m_fLb2Closed,
			rgBucketsIntersectTestElem[ul].m_fUb2Closed, CDouble(0.1),
			CDouble(100.0));

		BOOL result = bucket1->Intersects(bucket2);

		GPOS_RESULT eres = GPOS_FAILED;
		if (rgBucketsIntersectTestElem[ul].fIntersect == result)
		{
			eres = GPOS_OK;
		}

		if (true == result)
		{
			CDouble dDummy1(0.0);
			CDouble dDummy2(0.0);
			CBucket *pbucketOuput =
				bucket1->MakeBucketIntersect(mp, bucket2, &dDummy1, &dDummy2);
			CBucket *pbucketExpected = CCardinalityTestUtils::PbucketInteger(
				mp, rgBucketsIntersectTestElem[ul].m_iLbOutput,
				rgBucketsIntersectTestElem[ul].m_iUbOutput,
				rgBucketsIntersectTestElem[ul].m_fLbOutputClosed,
				rgBucketsIntersectTestElem[ul].m_fUbOutputClosed, CDouble(0.1),
				CDouble(100.0));

			BOOL fMatch = FMatchBucketBoundary(pbucketOuput, pbucketExpected);

			if (!fMatch)
			{
				eres = GPOS_FAILED;

				CWStringDynamic str(mp);
				COstreamString oss(&str);

				pbucketOuput->OsPrint(oss);
				oss << std::endl;
				pbucketExpected->OsPrint(oss);
				oss << std::endl;
				GPOS_TRACE(str.GetBuffer());
			}

			GPOS_DELETE(pbucketExpected);
			GPOS_DELETE(pbucketOuput);
		}
		// clean up
		GPOS_DELETE(bucket1);
		GPOS_DELETE(bucket2);

		if (GPOS_OK != eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}


// do the bucket boundaries match
BOOL
CBucketTest::FMatchBucketBoundary(CBucket *bucket1, CBucket *bucket2)
{
	GPOS_ASSERT(NULL != bucket1);
	GPOS_ASSERT(NULL != bucket2);

	if (bucket1->IsLowerClosed() != bucket2->IsLowerClosed())
	{
		return false;
	}

	if (bucket1->IsUpperClosed() != bucket2->IsUpperClosed())
	{
		return false;
	}

	if (bucket1->GetLowerBound()->Equals(bucket2->GetLowerBound()) &&
		bucket1->GetUpperBound()->Equals(bucket2->GetUpperBound()))
	{
		return true;
	}

	return false;
}

// basic merge commutativity test for union
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativityUnion()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// 1000 rows
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 0, 100, CDouble(0.6), CDouble(100.0));

	// 600 rows
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 50, 150, CDouble(0.3), CDouble(100.0));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);


	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// merge commutativity test for union when lower bounds have same value but
// one is closed and the other is open
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativitySameLowerBounds()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// 1000 rows
	// b1 = [0,100)
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 0, 100, CDouble(0.6), CDouble(100.0));

	// 600 rows
	// b2 = (0,50)
	CPoint *ppLower = CTestUtils::PpointInt4(mp, 0);
	CPoint *ppUpper = CTestUtils::PpointInt4(mp, 50);
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower, ppUpper, false /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	// returns [0,0]
	// bucket1_new1 = (0,100)
	// bucket2_new1 = (0,50)
	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);

	GPOS_ASSERT(bucket2->Equals(bucket2_new1));

	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));
	GPOS_ASSERT(result1->IsSingleton());
	GPOS_ASSERT(result2->IsSingleton());

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// merge commutativity test for union when lower bounds have same value but
// one is closed and the other is open
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativitySameUpperBounds()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// 1000 rows
	// b1 = [0,100)
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 0, 100, CDouble(0.6), CDouble(100.0));

	// 600 rows
	// b2 = [0,100]
	CPoint *ppLower = CTestUtils::PpointInt4(mp, 0);
	CPoint *ppUpper = CTestUtils::PpointInt4(mp, 100);
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower, ppUpper, true /* is_lower_closed */,
				true /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	// returns [0,100)
	// bucket1_new1 = NULL
	// bucket2_new1 = [100,100]
	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);

	GPOS_ASSERT(bucket2_new1->IsSingleton());

	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(bucket1_new2->IsSingleton());

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// basic merge commutativity test for union all
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativityUnionAll()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// 1000 rows
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 0, 100, CDouble(0.6), CDouble(100.0));

	// 600 rows
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 50, 150, CDouble(0.3), CDouble(100.0));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		true /*is_union_all*/);


	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		true /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// basic merge commutativity test for double datum
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativityDoubleDatum()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// [0.0, 100.0)
	CPoint *ppLower1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(0.0));
	CPoint *ppUpper1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(100.0));

	CBucket *bucket1 = GPOS_NEW(mp)
		CBucket(ppLower1, ppUpper1, true /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	// [50.0, 150.0)
	CPoint *ppLower2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(50.0));
	CPoint *ppUpper2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(150.0));
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower2, ppUpper2, true /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);


	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// merge commutativity test for union when lower bounds have same value but
// one is closed and the other is open for double datum
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativityDoubleDatumSameLowerBounds()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// b1 = [0,100)
	CPoint *ppLower1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(0.0));
	CPoint *ppUpper1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(100.0));

	CBucket *bucket1 = GPOS_NEW(mp)
		CBucket(ppLower1, ppUpper1, true /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	// b2 = (0,50)
	CPoint *ppLower2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(0.0));
	CPoint *ppUpper2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(50.0));
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower2, ppUpper2, false /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);


	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}

// merge commutativity test for union when upper bounds have same value but
// one is closed and the other is open for double datum
GPOS_RESULT
CBucketTest::EresUnittest_CBucketMergeCommutativityDoubleDatumSameUpperBounds()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// b1 = [0,100)
	CPoint *ppLower1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(0.0));
	CPoint *ppUpper1 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(100.0));

	CBucket *bucket1 = GPOS_NEW(mp)
		CBucket(ppLower1, ppUpper1, true /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.4), CDouble(50));

	// b2 = [0,100]
	CPoint *ppLower2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(0.0));
	CPoint *ppUpper2 =
		CCardinalityTestUtils::PpointDouble(mp, GPDB_FLOAT8, CDouble(100.0));
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower2, ppUpper2, true /* is_lower_closed */,
				true /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucket *bucket1_new1 = NULL;
	CBucket *bucket2_new1 = NULL;
	CDouble result_rows1(0.0);

	CBucket *result1 = bucket1->SplitAndMergeBuckets(
		mp, bucket2, 1000, 600, &bucket1_new1, &bucket2_new1, &result_rows1,
		false /*is_union_all*/);


	CBucket *bucket1_new2 = NULL;
	CBucket *bucket2_new2 = NULL;
	CDouble result_rows2(0.0);
	CBucket *result2 = bucket2->SplitAndMergeBuckets(
		mp, bucket1, 600, 1000, &bucket1_new2, &bucket2_new2, &result_rows2,
		false /*is_union_all*/);

	GPOS_ASSERT(result1->Equals(result2));

	if (NULL != bucket1_new1)
	{
		GPOS_ASSERT(bucket1_new1->Equals(bucket2_new2));
	}
	else if (NULL != bucket2_new1)
	{
		GPOS_ASSERT(bucket2_new1->Equals(bucket1_new2));
	}

	GPOS_DELETE(bucket1);
	GPOS_DELETE(bucket2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);
	GPOS_DELETE(bucket1_new1);
	GPOS_DELETE(bucket2_new1);
	GPOS_DELETE(bucket1_new2);
	GPOS_DELETE(bucket2_new2);

	return GPOS_OK;
}
// EOF
