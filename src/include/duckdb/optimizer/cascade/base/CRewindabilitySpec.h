//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CRewindabilitySpec.h
//
//	@doc:
//		Rewindability of intermediate query results;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CRewindabilitySpec_H
#define GPOPT_CRewindabilitySpec_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CPropSpec.h"

namespace gpopt
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CRewindabilitySpec
//
//	@doc:
//		Rewindability specification
//
//---------------------------------------------------------------------------
class CRewindabilitySpec : public CPropSpec
{
public:
	// From the perspective of the operator, the enum values mean the
	// following (in required & derived contexts):
	//
	// 1. ErtMarkRestore:
	//    require:
	//      I require my child to be mark-restorable. This is required by MJ of
	//      its inner child.
	//    derive:
	//      I am mark-restorable. (e.g Spool, Sort, Scan)
	//    (NB: I cannot derive mark-restorable just because my child is mark-restorable.
	//    	   However, I can derive rewindable.)
	//
	// 2. ErtRewindable:
	//    require:
	//      I require my child to be rewindable. This is required by NLJ of
	//      its inner child.
	//    derive:
	//      I am rewindable. (e.g Spool, Sort, Scan)
	//
	// 3. ErtRescannable:
	//    require:
	//      I require my child to be rescannable, so that I can re-execute
	//      the entire subtree if needed. This is required by correlated
	//      joins of their inner child.
	//    derive:
	//      I am not rewindable, but I am rescannable. (e.g TVF containing a
	//      volatile function)
	//
	// 4. ErtNone
	//    require:
	//      I do not require my child to be rewindable or rescannable. (e.g
	//      Sort that is not on the inner side of a correlated join)
	//    derive:
	//      I am neither rewindable nor rescannable. (e.g Motions, External
	//      table scans)
	enum ERewindabilityType
	{
		ErtMarkRestore,	 // rewindability with mark & restore support

		ErtRewindable,	// rewindability of all intermediate query results

		ErtRescannable,	 // not rewindable, but can be reexecuted from scratch

		ErtNone,  // neither rewindability nor rescannable

		ErtSentinel
	};

	// From the perspective of the operator, the enum values mean the
	// following (in required & derived contexts):
	//
	// 1. EmhtMotion:
	//	  require:
	//	    I require my child to handle motion hazard (if necessary)
	//	  derive:
	//	    I impose a motion hazard (for example, a streaming spool with a
	//	    motion underneath it, will derive this)
	//
	// 2. EmhtNoMotion:
	//	  require:
	//	    Motion hazard handling is unnecessary.
	//	  derive:
	//	    I do not impose motion hazard (derived by a rewindable operator
	//	    without no motion in its subtree or a blocking spool with or
	//	    without a motion underneath it)
	enum EMotionHazardType
	{
		EmhtMotion,	 // motion hazard in the tree

		EmhtNoMotion,  // no motion hazard in the tree

		EmhtSentinel
	};

private:
	// rewindability support
	ERewindabilityType m_rewindability;

	// Motion Hazard
	EMotionHazardType m_motion_hazard;

public:
	// ctor
	explicit CRewindabilitySpec(ERewindabilityType rewindability_type,
								EMotionHazardType motion_hazard);

	// dtor
	virtual ~CRewindabilitySpec();

	// check if rewindability specs match
	BOOL Matches(const CRewindabilitySpec *prs) const;

	// check if rewindability spec satisfies a req'd rewindability spec
	BOOL FSatisfies(const CRewindabilitySpec *prs) const;

	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prpp,
								 CExpressionArray *pdrgpexpr,
								 CExpression *pexpr);

	// hash function
	virtual ULONG HashValue() const;

	// extract columns used by the rewindability spec
	virtual CColRefSet *
	PcrsUsed(CMemoryPool *mp) const
	{
		// return an empty set
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// property type
	virtual EPropSpecType
	Epst() const
	{
		return EpstRewindability;
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	ERewindabilityType
	Ert() const
	{
		return m_rewindability;
	}

	EMotionHazardType
	Emht() const
	{
		return m_motion_hazard;
	}

	BOOL
	IsRewindable() const
	{
		return Ert() == ErtRewindable || Ert() == ErtMarkRestore;
	}

	BOOL
	IsRescannable() const
	{
		return Ert() == ErtRescannable;
	}

	BOOL
	IsCheckRequired() const
	{
		return Ert() != ErtNone;
	}

	BOOL
	HasMotionHazard() const
	{
		return Emht() == EmhtMotion;
	}

};	// class CRewindabilitySpec

}  // namespace gpopt

#endif
