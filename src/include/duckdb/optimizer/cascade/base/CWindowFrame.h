//---------------------------------------------------------------------------
//	@filename:
//		CWindowFrame.h
//
//	@doc:
//		Description of window frame
//---------------------------------------------------------------------------
#ifndef GPOPT_CWindowFrame_H
#define GPOPT_CWindowFrame_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CPropSpec.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"


namespace gpopt
{
// type definition of corresponding dynamic pointer array
class CWindowFrame;
typedef CDynamicPtrArray<CWindowFrame, CleanupRelease> CWindowFrameArray;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CWindowFrame
//
//	@doc:
//		Description of window frame
//
//---------------------------------------------------------------------------
class CWindowFrame : public CRefCount
{
public:
	// specification method
	enum EFrameSpec
	{
		EfsRows,   // frame is specified using Rows construct
		EfsRange,  // frame is specified using Range construct

		EfsSentinel
	};

	// types of frame boundary
	enum EFrameBoundary
	{
		EfbUnboundedPreceding,	// boundary is unlimited preceding current row
		EfbBoundedPreceding,	// boundary is limited preceding current row
		EfbCurrentRow,			// boundary is set to current row
		EfbUnboundedFollowing,	// boundary is unlimited following current row
		EfbBoundedFollowing,	// boundary is limited following current row
		EfbDelayedBoundedPreceding,	 // boundary is delayed preceding current row
		EfbDelayedBoundedFollowing,	 // boundary is delayed following current row

		EfbSentinel
	};

	// possible exclusion strategies
	enum EFrameExclusionStrategy
	{
		EfesNone,			 // no exclusion
		EfesNulls,			 // exclude nulls
		EfesCurrentRow,		 // exclude current row
		EfseMatchingOthers,	 // exclude other rows matching current row
		EfesTies,			 // exclude other matching rows and current row

		EfesSentinel
	};

private:
	// specification method
	const EFrameSpec m_efs;

	// type of leading edge
	const EFrameBoundary m_efbLeading;

	// type of trailing edge
	const EFrameBoundary m_efbTrailing;

	// scalar value of leading edge, memory owned by this class
	CExpression *m_pexprLeading;

	// scalar value of trailing edge, memory owned by this class
	CExpression *m_pexprTrailing;

	// exclusion strategy
	const EFrameExclusionStrategy m_efes;

	// columns used by frame edges
	CColRefSet *m_pcrsUsed;

	// singelton empty frame -- used with any unspecified window function frame
	static const CWindowFrame m_wfEmpty;

	// private copy ctor
	CWindowFrame(const CWindowFrame &);

	// private dummy ctor used to create empty frame
	CWindowFrame();

public:
	// ctor
	CWindowFrame(CMemoryPool *mp, EFrameSpec efs, EFrameBoundary efbLeading,
				 EFrameBoundary efbTrailing, CExpression *pexprLeading,
				 CExpression *pexprTrailing, EFrameExclusionStrategy efes);

	// dtor
	virtual ~CWindowFrame();

	// specification
	EFrameSpec
	Efs() const
	{
		return m_efs;
	}

	// type of leading edge
	EFrameBoundary
	EfbLeading() const
	{
		return m_efbLeading;
	}

	// type of trailing edge
	EFrameBoundary
	EfbTrailing() const
	{
		return m_efbTrailing;
	}

	// scalar value of leading edge
	CExpression *
	PexprLeading() const
	{
		return m_pexprLeading;
	}

	// scalar value of trailing edge
	CExpression *
	PexprTrailing() const
	{
		return m_pexprTrailing;
	}

	// exclusion strategy
	EFrameExclusionStrategy
	Efes() const
	{
		return m_efes;
	}

	// matching function
	BOOL Matches(const CWindowFrame *pwf) const;

	// hash function
	virtual ULONG HashValue() const;

	// return a copy of the window frame with remapped columns
	virtual CWindowFrame *PwfCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// return columns used by frame edges
	CColRefSet *
	PcrsUsed() const
	{
		return m_pcrsUsed;
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// matching function over frame arrays
	static BOOL Equals(const CWindowFrameArray *pdrgpwfFirst,
					   const CWindowFrameArray *pdrgpwfSecond);

	// combine hash values of a maximum number of entries
	static ULONG HashValue(const CWindowFrameArray *pdrgpwfFirst,
						   ULONG ulMaxSize);

	// print array of window frame objects
	static IOstream &OsPrint(IOstream &os, const CWindowFrameArray *pdrgpwf);

	// check if a given window frame is empty
	static BOOL
	IsEmpty(CWindowFrame *pwf)
	{
		return pwf == &m_wfEmpty;
	}

	// return pointer to singleton empty window frame
	static const CWindowFrame *
	PwfEmpty()
	{
		return &m_wfEmpty;
	}

};	// class CWindowFrame

}  // namespace gpopt

#endif
