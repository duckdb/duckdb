//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CTranslatorScalarToDXL.h
//
//	@doc:
//		Class providing methods for translating a GPDB Scalar Operation (in a Query / PlStmt object)
//		into a DXL tree.
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorScalarToDXL_H
#define GPDXL_CTranslatorScalarToDXL_H

#include "gpos/base.h"

extern "C" {
#include "postgres.h"

#include "nodes/primnodes.h"
}

#include "gpopt/translate/CCTEListEntry.h"
#include "gpopt/translate/CContextQueryToDXL.h"
#include "gpopt/translate/CMappingVarColId.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/md/IMDType.h"

// fwd declarations
namespace gpopt
{
class CMDAccessor;
}

struct Aggref;
struct BoolExpr;
struct BooleanTest;
struct CaseExpr;
struct Expr;
struct FuncExpr;
struct NullTest;
struct OpExpr;
struct Param;
struct RelabelType;
struct ScalarArrayOpExpr;

namespace gpdxl
{
using namespace gpopt;
using namespace gpmd;

// fwd decl
class CMappingVarColId;
class CDXLDatum;

class CTranslatorScalarToDXL
{
	// shorthand for functions for translating GPDB expressions into DXL nodes
	typedef CDXLNode *(CTranslatorScalarToDXL::*ExprToDXLFn)(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// shorthand for functions for translating DXL nodes to GPDB expressions
	typedef CDXLDatum *(DxlDatumFromDatum)(CMemoryPool *mp,
										   const IMDType *md_type, BOOL is_null,
										   ULONG len, Datum datum);

private:
	// private constructor for TranslateStandaloneExprToDXL
	CTranslatorScalarToDXL(CMemoryPool *mp, CMDAccessor *mda);

	// pair of node tag and translator function
	struct STranslatorElem
	{
		NodeTag tag;
		ExprToDXLFn func_ptr;
	};

	// context for the whole query being translated, or NULL if this is
	// standalone expression (e.g. the DEFAULT expression of a column).
	CContextQueryToDXL *m_context;

	// memory pool
	CMemoryPool *m_mp;

	// meta data accessor
	CMDAccessor *m_md_accessor;

	// absolute level of query whose vars will be translated
	ULONG m_query_level;

	// physical operator that created this translator
	EPlStmtPhysicalOpType m_op_type;

	// hash map that maintains the list of CTEs defined at a particular query level
	HMUlCTEListEntry *m_cte_entries;

	// list of CTE producers shared among the logical and scalar translators
	CDXLNodeArray *m_cte_producers;

	EdxlBoolExprType EdxlbooltypeFromGPDBBoolType(BoolExprType) const;

	CTranslatorQueryToDXL *CreateSubqueryTranslator(
		Query *subquery, const CMappingVarColId *var_colid_mapping);

	// translate list elements and add them as children of the DXL node
	void TranslateScalarChildren(CDXLNode *dxlnode, List *list,
								 const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar distinct comparison node from a GPDB DistinctExpr
	CDXLNode *TranslateDistinctExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar boolean expression node from a GPDB qual list
	CDXLNode *CreateScalarCondFromQual(
		List *quals, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar comparison node from a GPDB op expression
	CDXLNode *CreateScalarCmpFromOpExpr(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar opexpr node from a GPDB expression
	CDXLNode *TranslateOpExprToDXL(const Expr *expr,
								   const CMappingVarColId *var_colid_mapping);

	// translate an array expression
	CDXLNode *TranslateScalarArrayOpExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar array comparison node from a GPDB expression
	CDXLNode *CreateScalarArrayCompFromExpr(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar Const node from a GPDB expression
	CDXLNode *TranslateConstToDXL(const Expr *expr,
								  const CMappingVarColId *var_colid_mapping);

	// create a DXL node for a scalar nullif from a GPDB Expr
	CDXLNode *TranslateNullIfExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar boolexpr node from a GPDB expression
	CDXLNode *TranslateBoolExprToDXL(const Expr *expr,
									 const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar boolean test node from a GPDB expression
	CDXLNode *TranslateBooleanTestToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar nulltest node from a GPDB expression
	CDXLNode *TranslateNullTestToDXL(const Expr *expr,
									 const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar case statement node from a GPDB expression
	CDXLNode *TranslateCaseExprToDXL(const Expr *expr,
									 const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar if statement node from a GPDB case expression
	CDXLNode *CreateScalarIfStmtFromCaseExpr(
		const CaseExpr *case_expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar switch node from a GPDB case expression
	CDXLNode *CreateScalarSwitchFromCaseExpr(
		const CaseExpr *case_expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL node for a case test from a GPDB Expr.
	CDXLNode *TranslateCaseTestExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar coalesce node from a GPDB expression
	CDXLNode *TranslateCoalesceExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar minmax node from a GPDB expression
	CDXLNode *TranslateMinMaxExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar relabeltype node from a GPDB expression
	CDXLNode *TranslateRelabelTypeToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar coerce node from a GPDB expression
	CDXLNode *TranslateCoerceToDomainToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar coerceviaio node from a GPDB expression
	CDXLNode *TranslateCoerceViaIOToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar array coerce expression node from a GPDB expression
	CDXLNode *TranslateArrayCoerceExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar funcexpr node from a GPDB expression
	CDXLNode *TranslateFuncExprToDXL(const Expr *expr,
									 const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar WindowFunc node from a GPDB expression
	CDXLNode *TranslateWindowFuncToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar Aggref node from a GPDB expression
	CDXLNode *TranslateAggrefToDXL(const Expr *expr,
								   const CMappingVarColId *var_colid_mapping);

	CDXLNode *TranslateVarToDXL(const Expr *expr,
								const CMappingVarColId *var_colid_mapping);

	CDXLNode *CreateInitPlanFromParam(const Param *param) const;

	// create a DXL SubPlan node for a from a GPDB SubPlan
	CDXLNode *TranslateSubplanToDXL(const Expr *expr,
									const CMappingVarColId *var_colid_mapping);

	CDXLNode *CreatePlanFromParam(const Expr *expr,
								  const CMappingVarColId *var_colid_mapping);

	CDXLNode *TranslateSubLinkToDXL(const Expr *expr,
									const CMappingVarColId *var_colid_mapping);

	CDXLNode *CreateScalarSubqueryFromSublink(
		const SubLink *sublink, const CMappingVarColId *var_colid_mapping);

	CDXLNode *CreateExistSubqueryFromSublink(
		const SubLink *sublink, const CMappingVarColId *var_colid_mapping);

	CDXLNode *CreateQuantifiedSubqueryFromSublink(
		const SubLink *sublink, const CMappingVarColId *var_colid_mapping);

	// translate an array expression
	CDXLNode *TranslateArrayExprToDXL(
		const Expr *expr, const CMappingVarColId *var_colid_mapping);

	// translate an arrayref expression
	CDXLNode *TranslateArrayRefToDXL(const Expr *expr,
									 const CMappingVarColId *var_colid_mapping);

	// add an indexlist to the given DXL arrayref node
	void AddArrayIndexList(
		CDXLNode *dxlnode, List *list,
		CDXLScalarArrayRefIndexList::EIndexListBound index_list_bound,
		const CMappingVarColId *var_colid_mapping);

	// get the operator name
	const CWStringConst *GetDXLArrayCmpType(IMDId *mdid) const;

	// translate the window frame edge, if the column used in the edge is a
	// computed column then add it to the project list
	CDXLNode *TranslateWindowFrameEdgeToDXL(
		const Node *node, const CMappingVarColId *var_colid_mapping,
		CDXLNode *new_scalar_proj_list);

public:
	// ctor
	CTranslatorScalarToDXL(CContextQueryToDXL *context,
						   CMDAccessor *md_accessor, ULONG query_level,
						   HMUlCTEListEntry *cte_entries,
						   CDXLNodeArray *cte_dxlnode_array);

	// set the caller type
	void
	SetCallingPhysicalOpType(EPlStmtPhysicalOpType plstmt_physical_op_type)
	{
		m_op_type = plstmt_physical_op_type;
	}

	// create a DXL datum from a GPDB const
	CDXLDatum *TranslateConstToDXL(const Const *constant) const;

	// return the current caller type
	EPlStmtPhysicalOpType
	GetPhysicalOpType() const
	{
		return m_op_type;
	}
	// create a DXL scalar operator node from a GPDB expression
	// and a table descriptor for looking up column descriptors
	CDXLNode *TranslateScalarToDXL(const Expr *expr,
								   const CMappingVarColId *var_colid_mapping);

	// create a DXL scalar filter node from a GPDB qual list
	CDXLNode *CreateFilterFromQual(List *quals,
								   const CMappingVarColId *var_colid_mapping,
								   Edxlopid filter_type);

	// create a DXL WindowFrame node from a GPDB expression
	CDXLWindowFrame *TranslateWindowFrameToDXL(
		int frame_options, const Node *start_offset, const Node *end_offset,
		const CMappingVarColId *var_colid_mapping,
		CDXLNode *new_scalar_proj_list);

	// translate stand-alone expression that's not part of a query
	static CDXLNode *TranslateStandaloneExprToDXL(
		CMemoryPool *mp, CMDAccessor *mda,
		const CMappingVarColId *var_colid_mapping, const Expr *expr);

	// translate GPDB Const to CDXLDatum
	static CDXLDatum *TranslateConstToDXL(CMemoryPool *mp, CMDAccessor *mda,
										  const Const *constant);

	// translate GPDB datum to CDXLDatum
	static CDXLDatum *TranslateDatumToDXL(CMemoryPool *mp,
										  const IMDType *md_type,
										  INT type_modifier, BOOL is_null,
										  ULONG len, Datum datum);

	// translate GPDB datum to IDatum
	static IDatum *CreateIDatumFromGpdbDatum(CMemoryPool *mp,
											 const IMDType *md_type,
											 BOOL is_null, Datum datum);

	// extract the byte array value of the datum
	static BYTE *ExtractByteArrayFromDatum(CMemoryPool *mp,
										   const IMDType *md_type, BOOL is_null,
										   ULONG len, Datum datum);

	static CDouble ExtractDoubleValueFromDatum(IMDId *mdid, BOOL is_null,
											   BYTE *bytes, Datum datum);

	// extract the long int value of a datum
	static LINT ExtractLintValueFromDatum(const IMDType *md_type, BOOL is_null,
										  BYTE *bytes, ULONG len);

	// pair of DXL datum type and translator function
	struct SDXLDatumTranslatorElem
	{
		IMDType::ETypeInfo type_info;
		DxlDatumFromDatum *func_ptr;
	};

	// datum to oid CDXLDatum
	static CDXLDatum *TranslateOidDatumToDXL(CMemoryPool *mp,
											 const IMDType *md_type,
											 BOOL is_null, ULONG len,
											 Datum datum);

	// datum to int2 CDXLDatum
	static CDXLDatum *TranslateInt2DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to int4 CDXLDatum
	static CDXLDatum *TranslateInt4DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to int8 CDXLDatum
	static CDXLDatum *TranslateInt8DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to bool CDXLDatum
	static CDXLDatum *TranslateBoolDatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to generic CDXLDatum
	static CDXLDatum *TranslateGenericDatumToDXL(CMemoryPool *mp,
												 const IMDType *md_type,
												 INT type_modifier,
												 BOOL is_null, ULONG len,
												 Datum datum);
};
}  // namespace gpdxl
#endif	// GPDXL_CTranslatorScalarToDXL_H

// EOF
