/*-------------------------------------------------------------------------
 *
 * makefuncs.h
 *	  prototypes for the creator functions (for primitive nodes)
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/makefuncs.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/parsenodes.hpp"

extern PGAExpr *makeAExpr(PGAExpr_Kind kind, PGList *name,
		   PGNode *lexpr, PGNode *rexpr, int location);

extern PGAExpr *makeSimpleAExpr(PGAExpr_Kind kind, const char *name,
				 PGNode *lexpr, PGNode *rexpr, int location);

extern PGVar *makeVar(PGIndex varno,
		PGAttrNumber varattno,
		PGOid vartype,
		int32_t vartypmod,
		PGOid varcollid,
		PGIndex varlevelsup);

extern PGVar *makeVarFromTargetEntry(PGIndex varno,
					   PGTargetEntry *tle);

extern PGVar *makeWholeRowVar(PGRangeTblEntry *rte,
				PGIndex varno,
				PGIndex varlevelsup,
				bool allowScalar);

extern PGTargetEntry *makeTargetEntry(PGExpr *expr,
				PGAttrNumber resno,
				char *resname,
				bool resjunk);

extern PGTargetEntry *flatCopyTargetEntry(PGTargetEntry *src_tle);

extern PGFromExpr *makeFromExpr(PGList *fromlist, PGNode *quals);

extern PGConst *makeConst(PGOid consttype,
		  int32_t consttypmod,
		  PGOid constcollid,
		  int constlen,
		  PGDatum constvalue,
		  bool constisnull,
		  bool constbyval);

extern PGConst *makeNullConst(PGOid consttype, int32_t consttypmod, PGOid constcollid);

extern PGNode *makeBoolConst(bool value, bool isnull);

extern PGExpr *makeBoolExpr(PGBoolExprType boolop, PGList *args, int location);

extern PGAlias *makeAlias(const char *aliasname, PGList *colnames);

extern PGRelabelType *makeRelabelType(PGExpr *arg, PGOid rtype, int32_t rtypmod,
				PGOid rcollid, PGCoercionForm rformat);

extern PGRangeVar *makeRangeVar(char *schemaname, char *relname, int location);

extern PGTypeName *makeTypeName(char *typnam);
extern PGTypeName *makeTypeNameFromNameList(PGList *names);
extern PGTypeName *makeTypeNameFromOid(PGOid typeOid, int32_t typmod);

extern PGColumnDef *makeColumnDef(const char *colname,
			  PGOid typeOid, int32_t typmod, PGOid collOid);

extern PGFuncExpr *makeFuncExpr(PGOid funcid, PGOid rettype, PGList *args,
			 PGOid funccollid, PGOid inputcollid, PGCoercionForm fformat);

extern PGFuncCall *makeFuncCall(PGList *name, PGList *args, int location);

extern PGDefElem *makeDefElem(const char *name, PGNode *arg, int location);
extern PGDefElem *makeDefElemExtended(const char *nameSpace, const char *name, PGNode *arg,
					PGDefElemAction defaction, int location);

extern PGGroupingSet *makeGroupingSet(GroupingSetKind kind, PGList *content, int location);
