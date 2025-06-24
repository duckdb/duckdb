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

namespace duckdb_libpgquery {

PGAExpr *makeAExpr(PGAExpr_Kind kind, PGList *name, PGNode *lexpr, PGNode *rexpr, int location);

PGAExpr *makeSimpleAExpr(PGAExpr_Kind kind, const char *name, PGNode *lexpr, PGNode *rexpr, int location);

PGVar *makeVar(PGIndex varno, PGAttrNumber varattno, PGOid vartype, int32_t vartypmod, PGOid varcollid,
               PGIndex varlevelsup);

PGVar *makeVarFromTargetEntry(PGIndex varno, PGTargetEntry *tle);

PGVar *makeWholeRowVar(PGRangeTblEntry *rte, PGIndex varno, PGIndex varlevelsup, bool allowScalar);

PGTargetEntry *makeTargetEntry(PGExpr *expr, PGAttrNumber resno, char *resname, bool resjunk);

PGTargetEntry *flatCopyTargetEntry(PGTargetEntry *src_tle);

PGFromExpr *makeFromExpr(PGList *fromlist, PGNode *quals);

PGConst *makeConst(PGOid consttype, int32_t consttypmod, PGOid constcollid, int constlen, PGDatum constvalue,
                   bool constisnull, bool constbyval);

PGConst *makeNullConst(PGOid consttype, int32_t consttypmod, PGOid constcollid);

PGNode *makeBoolConst(bool value, bool isnull);

PGExpr *makeBoolExpr(PGBoolExprType boolop, PGList *args, int location);

PGAlias *makeAlias(const char *aliasname, PGList *colnames);

PGRelabelType *makeRelabelType(PGExpr *arg, PGOid rtype, int32_t rtypmod, PGOid rcollid, PGCoercionForm rformat);

PGRangeVar *makeRangeVar(char *schemaname, char *relname, int location);

PGTypeName *makeTypeName(char *typnam);
PGTypeName *makeTypeNameFromNameList(PGList *names);
PGTypeName *makeTypeNameFromOid(PGOid typeOid, int32_t typmod);

PGColumnDef *makeColumnDef(const char *colname, PGOid typeOid, int32_t typmod, PGOid collOid);

PGFuncExpr *makeFuncExpr(PGOid funcid, PGOid rettype, PGList *args, PGOid funccollid, PGOid inputcollid,
                         PGCoercionForm fformat);

PGFuncCall *makeFuncCall(PGList *name, PGList *args, int location);

PGDefElem *makeDefElem(const char *name, PGNode *arg, int location);
PGDefElem *makeDefElemExtended(const char *nameSpace, const char *name, PGNode *arg, PGDefElemAction defaction,
                               int location);

PGGroupingSet *makeGroupingSet(GroupingSetKind kind, PGList *content, int location);

}
