//---------------------------------------------------------------------------
//	@filename:
//		CRewindabilitySpec.cpp
//
//	@doc:
//		Specification of rewindability of intermediate query results
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CRewindabilitySpec.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSpool.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::CRewindabilitySpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRewindabilitySpec::CRewindabilitySpec(ERewindabilityType rewindability_type, EMotionHazardType motion_hazard)
	: m_rewindability(rewindability_type), m_motion_hazard(motion_hazard)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::~CRewindabilitySpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRewindabilitySpec::~CRewindabilitySpec()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::Matches
//
//	@doc:
//		Check for equality between rewindability specs
//
//---------------------------------------------------------------------------
BOOL CRewindabilitySpec::Matches(const CRewindabilitySpec *prs) const
{
	GPOS_ASSERT(NULL != prs);

	return Ert() == prs->Ert() && Emht() == prs->Emht();
}


//	Check if this rewindability spec satisfies the given one
//	based on following satisfiability rules:
//
//	prs  = requested rewindability
//	this = derived rewindability
//
//	Table 1 - Rewindability satisfiability matrix:
//	  T - Satisfied
//	  F - Not satisfied; enforcer required
//	  M - Maybe satisfied; check motion hazard satisfiability (see below)
//	+-------------+------+-------------+------------+-------------+
//	|  Derive ->  | None | Rescannable | Rewindable | MarkRestore |
//	|  Required   |      |             |            |             |
//	+-------------+------+-------------+------------+-------------+
//	| None        | T    | T           | T          | T           |
//	| Rescannable | F    | M           | M          | M           |
//	| Rewindable  | F    | F           | M          | M           |
//	| MarkRestore | F    | F           | F          | M           |
//	+-------------+------+-------------+------------+-------------+
//
//	Table 2 - Motion hazard check matrix:
//	(NB: only applies to the 3 cases in the previous table):
//	+-----------+----------+--------+
//	| Derive -> | NoMotion | Motion |
//	| Required  |          |        |
//	+-----------+----------+--------+
//	| NoMotion  | T        | T      |
//	| Motion    | T        | F      |
//	+-----------+----------+--------+

BOOL CRewindabilitySpec::FSatisfies(const CRewindabilitySpec *prs) const
{
	// ErtNone requests always satisfied (row 1 in table 1)
	if (prs->Ert() == ErtNone)
	{
		return true;
	}
	// ErtNone derived op cannot satisfy rewindable or rescannable requests
	// (rows 2-4, col 1 in table 1)
	else if (Ert() == ErtNone)
	{
		return false;
	}
	// A rewindable/MarkRestore request cannot be satisfied by a rescannable op
	// (row 3-4 col 2 in table 1)
	if (Ert() == ErtRescannable &&
		(prs->Ert() == ErtRewindable || prs->Ert() == ErtMarkRestore))
	{
		return false;
	}
	// A MarkRestore request cannot be satisified by a rewindable op
	// (row 4 col 3 in table 1)
	if (Ert() == ErtRewindable && prs->Ert() == ErtMarkRestore)
	{
		return false;
	}
	// For the rest, check motion hazard satisfiability:

	// A request to handle motion hazard cannot be satisfied by an op that
	// derived a motion hazard (row 2 col 2 in table 2)
	if (prs->HasMotionHazard() && HasMotionHazard())
	{
		return false;
	}

	// A request without motion hazard handling is satisfied. Also, a op that
	// derived no motion hazard is satisfied
	// (row 1 in table 2 & row 2 col 1 in table 2)
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG CRewindabilitySpec::HashValue() const
{
	return gpos::CombineHashes(
		gpos::HashValue<ERewindabilityType>(&m_rewindability),
		gpos::HashValue<EMotionHazardType>(&m_motion_hazard));
}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void CRewindabilitySpec::AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(
		this == prpp->Per()->PrsRequired() &&
		"required plan properties don't match enforced rewindability spec");

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	BOOL eager = false;
	if (!GPOS_FTRACE(EopttraceMotionHazardHandling) ||
		(prpp->Per()->PrsRequired()->HasMotionHazard() &&
		 prs->HasMotionHazard()))
	{
		// If motion hazard handling is disabled then we always want a blocking spool.
		// otherwise, create a blocking spool *only if* the request alerts about motion
		// hazard and the group expression imposes a motion hazard as well, to prevent deadlock
		eager = true;
	}

	pexpr->AddRef();
	CExpression *pexprSpool = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalSpool(mp, eager), pexpr);
	pdrgpexpr->Append(pexprSpool);
}

//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::OsPrint
//
//	@doc:
//		Print rewindability spec
//
//---------------------------------------------------------------------------
IOstream& CRewindabilitySpec::OsPrint(IOstream &os) const
{
	switch (Ert())
	{
		case ErtRewindable:
			os << "REWINDABLE";
			break;

		case ErtRescannable:
			os << "RESCANNABLE";
			break;

		case ErtNone:
			os << "NONE";
			break;

		case ErtMarkRestore:
			os << "MARK-RESTORE";
			break;

		default:
			GPOS_ASSERT(!"Unrecognized rewindability type");
			return os;
	}

	switch (Emht())
	{
		case EmhtMotion:
			os << " MOTION";
			break;

		case EmhtNoMotion:
			os << " NO-MOTION";
			break;

		default:
			GPOS_ASSERT(!"Unrecognized motion hazard type");
			break;
	}

	return os;
}