static void
_outAlias(StringInfo str, const Alias *node)
{
  WRITE_NODE_TYPE("Alias");

  WRITE_STRING_FIELD(aliasname);
  WRITE_NODE_PTR_FIELD(colnames);
}

static void
_outRangeVar(StringInfo str, const RangeVar *node)
{
  WRITE_NODE_TYPE("RangeVar");

  WRITE_STRING_FIELD(schemaname);
  WRITE_STRING_FIELD(relname);
  WRITE_ENUM_FIELD(inhOpt);
  WRITE_CHAR_FIELD(relpersistence);
  WRITE_NODE_PTR_FIELD(alias);
  WRITE_INT_FIELD(location);
}

static void
_outVar(StringInfo str, const Var *node)
{
  WRITE_NODE_TYPE("Var");

  WRITE_UINT_FIELD(varno);
  WRITE_INT_FIELD(varattno);
  WRITE_UINT_FIELD(vartype);
  WRITE_INT_FIELD(vartypmod);
  WRITE_UINT_FIELD(varcollid);
  WRITE_UINT_FIELD(varlevelsup);
  WRITE_UINT_FIELD(varnoold);
  WRITE_INT_FIELD(varoattno);
  WRITE_INT_FIELD(location);
}

static void
_outParam(StringInfo str, const Param *node)
{
  WRITE_NODE_TYPE("Param");

  WRITE_ENUM_FIELD(paramkind);
  WRITE_INT_FIELD(paramid);
  WRITE_UINT_FIELD(paramtype);
  WRITE_INT_FIELD(paramtypmod);
  WRITE_UINT_FIELD(paramcollid);
  WRITE_INT_FIELD(location);
}

static void
_outAggref(StringInfo str, const Aggref *node)
{
  WRITE_NODE_TYPE("Aggref");

  WRITE_UINT_FIELD(aggfnoid);
  WRITE_UINT_FIELD(aggtype);
  WRITE_UINT_FIELD(aggcollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(aggdirectargs);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(aggorder);
  WRITE_NODE_PTR_FIELD(aggdistinct);
  WRITE_NODE_PTR_FIELD(aggfilter);
  WRITE_BOOL_FIELD(aggstar);
  WRITE_BOOL_FIELD(aggvariadic);
  WRITE_CHAR_FIELD(aggkind);
  WRITE_UINT_FIELD(agglevelsup);
  WRITE_INT_FIELD(location);
}

static void
_outGroupingFunc(StringInfo str, const GroupingFunc *node)
{
  WRITE_NODE_TYPE("GroupingFunc");

  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(refs);
  WRITE_NODE_PTR_FIELD(cols);
  WRITE_UINT_FIELD(agglevelsup);
  WRITE_INT_FIELD(location);
}

static void
_outWindowFunc(StringInfo str, const WindowFunc *node)
{
  WRITE_NODE_TYPE("WindowFunc");

  WRITE_UINT_FIELD(winfnoid);
  WRITE_UINT_FIELD(wintype);
  WRITE_UINT_FIELD(wincollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(aggfilter);
  WRITE_UINT_FIELD(winref);
  WRITE_BOOL_FIELD(winstar);
  WRITE_BOOL_FIELD(winagg);
  WRITE_INT_FIELD(location);
}

static void
_outArrayRef(StringInfo str, const ArrayRef *node)
{
  WRITE_NODE_TYPE("ArrayRef");

  WRITE_UINT_FIELD(refarraytype);
  WRITE_UINT_FIELD(refelemtype);
  WRITE_INT_FIELD(reftypmod);
  WRITE_UINT_FIELD(refcollid);
  WRITE_NODE_PTR_FIELD(refupperindexpr);
  WRITE_NODE_PTR_FIELD(reflowerindexpr);
  WRITE_NODE_PTR_FIELD(refexpr);
  WRITE_NODE_PTR_FIELD(refassgnexpr);
}

static void
_outFuncExpr(StringInfo str, const FuncExpr *node)
{
  WRITE_NODE_TYPE("FuncExpr");

  WRITE_UINT_FIELD(funcid);
  WRITE_UINT_FIELD(funcresulttype);
  WRITE_BOOL_FIELD(funcretset);
  WRITE_BOOL_FIELD(funcvariadic);
  WRITE_ENUM_FIELD(funcformat);
  WRITE_UINT_FIELD(funccollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outNamedArgExpr(StringInfo str, const NamedArgExpr *node)
{
  WRITE_NODE_TYPE("NamedArgExpr");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_STRING_FIELD(name);
  WRITE_INT_FIELD(argnumber);
  WRITE_INT_FIELD(location);
}

static void
_outOpExpr(StringInfo str, const OpExpr *node)
{
  WRITE_NODE_TYPE("OpExpr");

  WRITE_UINT_FIELD(opno);
  WRITE_UINT_FIELD(opfuncid);
  WRITE_UINT_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_UINT_FIELD(opcollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outDistinctExpr(StringInfo str, const DistinctExpr *node)
{
  WRITE_NODE_TYPE("OpExpr");

  WRITE_UINT_FIELD(opno);
  WRITE_UINT_FIELD(opfuncid);
  WRITE_UINT_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_UINT_FIELD(opcollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outNullIfExpr(StringInfo str, const NullIfExpr *node)
{
  WRITE_NODE_TYPE("OpExpr");

  WRITE_UINT_FIELD(opno);
  WRITE_UINT_FIELD(opfuncid);
  WRITE_UINT_FIELD(opresulttype);
  WRITE_BOOL_FIELD(opretset);
  WRITE_UINT_FIELD(opcollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outScalarArrayOpExpr(StringInfo str, const ScalarArrayOpExpr *node)
{
  WRITE_NODE_TYPE("ScalarArrayOpExpr");

  WRITE_UINT_FIELD(opno);
  WRITE_UINT_FIELD(opfuncid);
  WRITE_BOOL_FIELD(useOr);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outBoolExpr(StringInfo str, const BoolExpr *node)
{
  WRITE_NODE_TYPE("BoolExpr");

  WRITE_ENUM_FIELD(boolop);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outSubLink(StringInfo str, const SubLink *node)
{
  WRITE_NODE_TYPE("SubLink");

  WRITE_ENUM_FIELD(subLinkType);
  WRITE_INT_FIELD(subLinkId);
  WRITE_NODE_PTR_FIELD(testexpr);
  WRITE_NODE_PTR_FIELD(operName);
  WRITE_NODE_PTR_FIELD(subselect);
  WRITE_INT_FIELD(location);
}

static void
_outSubPlan(StringInfo str, const SubPlan *node)
{
  WRITE_NODE_TYPE("SubPlan");

  WRITE_ENUM_FIELD(subLinkType);
  WRITE_NODE_PTR_FIELD(testexpr);
  WRITE_NODE_PTR_FIELD(paramIds);
  WRITE_INT_FIELD(plan_id);
  WRITE_STRING_FIELD(plan_name);
  WRITE_UINT_FIELD(firstColType);
  WRITE_INT_FIELD(firstColTypmod);
  WRITE_UINT_FIELD(firstColCollation);
  WRITE_BOOL_FIELD(useHashTable);
  WRITE_BOOL_FIELD(unknownEqFalse);
  WRITE_NODE_PTR_FIELD(setParam);
  WRITE_NODE_PTR_FIELD(parParam);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_FLOAT_FIELD(startup_cost);
  WRITE_FLOAT_FIELD(per_call_cost);
}

static void
_outAlternativeSubPlan(StringInfo str, const AlternativeSubPlan *node)
{
  WRITE_NODE_TYPE("AlternativeSubPlan");

  WRITE_NODE_PTR_FIELD(subplans);
}

static void
_outFieldSelect(StringInfo str, const FieldSelect *node)
{
  WRITE_NODE_TYPE("FieldSelect");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_INT_FIELD(fieldnum);
  WRITE_UINT_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_UINT_FIELD(resultcollid);
}

static void
_outFieldStore(StringInfo str, const FieldStore *node)
{
  WRITE_NODE_TYPE("FieldStore");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_NODE_PTR_FIELD(newvals);
  WRITE_NODE_PTR_FIELD(fieldnums);
  WRITE_UINT_FIELD(resulttype);
}

static void
_outRelabelType(StringInfo str, const RelabelType *node)
{
  WRITE_NODE_TYPE("RelabelType");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_UINT_FIELD(resultcollid);
  WRITE_ENUM_FIELD(relabelformat);
  WRITE_INT_FIELD(location);
}

static void
_outCoerceViaIO(StringInfo str, const CoerceViaIO *node)
{
  WRITE_NODE_TYPE("CoerceViaIO");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(resulttype);
  WRITE_UINT_FIELD(resultcollid);
  WRITE_ENUM_FIELD(coerceformat);
  WRITE_INT_FIELD(location);
}

static void
_outArrayCoerceExpr(StringInfo str, const ArrayCoerceExpr *node)
{
  WRITE_NODE_TYPE("ArrayCoerceExpr");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(elemfuncid);
  WRITE_UINT_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_UINT_FIELD(resultcollid);
  WRITE_BOOL_FIELD(isExplicit);
  WRITE_ENUM_FIELD(coerceformat);
  WRITE_INT_FIELD(location);
}

static void
_outConvertRowtypeExpr(StringInfo str, const ConvertRowtypeExpr *node)
{
  WRITE_NODE_TYPE("ConvertRowtypeExpr");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(resulttype);
  WRITE_ENUM_FIELD(convertformat);
  WRITE_INT_FIELD(location);
}

static void
_outCollateExpr(StringInfo str, const CollateExpr *node)
{
  WRITE_NODE_TYPE("CollateExpr");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(collOid);
  WRITE_INT_FIELD(location);
}

static void
_outCaseExpr(StringInfo str, const CaseExpr *node)
{
  WRITE_NODE_TYPE("CaseExpr");

  WRITE_UINT_FIELD(casetype);
  WRITE_UINT_FIELD(casecollid);
  WRITE_NODE_PTR_FIELD(arg);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(defresult);
  WRITE_INT_FIELD(location);
}

static void
_outCaseWhen(StringInfo str, const CaseWhen *node)
{
  WRITE_NODE_TYPE("CaseWhen");

  WRITE_NODE_PTR_FIELD(expr);
  WRITE_NODE_PTR_FIELD(result);
  WRITE_INT_FIELD(location);
}

static void
_outCaseTestExpr(StringInfo str, const CaseTestExpr *node)
{
  WRITE_NODE_TYPE("CaseTestExpr");

  WRITE_UINT_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_UINT_FIELD(collation);
}

static void
_outArrayExpr(StringInfo str, const ArrayExpr *node)
{
  WRITE_NODE_TYPE("ArrayExpr");

  WRITE_UINT_FIELD(array_typeid);
  WRITE_UINT_FIELD(array_collid);
  WRITE_UINT_FIELD(element_typeid);
  WRITE_NODE_PTR_FIELD(elements);
  WRITE_BOOL_FIELD(multidims);
  WRITE_INT_FIELD(location);
}

static void
_outRowExpr(StringInfo str, const RowExpr *node)
{
  WRITE_NODE_TYPE("RowExpr");

  WRITE_NODE_PTR_FIELD(args);
  WRITE_UINT_FIELD(row_typeid);
  WRITE_ENUM_FIELD(row_format);
  WRITE_NODE_PTR_FIELD(colnames);
  WRITE_INT_FIELD(location);
}

static void
_outRowCompareExpr(StringInfo str, const RowCompareExpr *node)
{
  WRITE_NODE_TYPE("RowCompareExpr");

  WRITE_ENUM_FIELD(rctype);
  WRITE_NODE_PTR_FIELD(opnos);
  WRITE_NODE_PTR_FIELD(opfamilies);
  WRITE_NODE_PTR_FIELD(inputcollids);
  WRITE_NODE_PTR_FIELD(largs);
  WRITE_NODE_PTR_FIELD(rargs);
}

static void
_outCoalesceExpr(StringInfo str, const CoalesceExpr *node)
{
  WRITE_NODE_TYPE("CoalesceExpr");

  WRITE_UINT_FIELD(coalescetype);
  WRITE_UINT_FIELD(coalescecollid);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outMinMaxExpr(StringInfo str, const MinMaxExpr *node)
{
  WRITE_NODE_TYPE("MinMaxExpr");

  WRITE_UINT_FIELD(minmaxtype);
  WRITE_UINT_FIELD(minmaxcollid);
  WRITE_UINT_FIELD(inputcollid);
  WRITE_ENUM_FIELD(op);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(location);
}

static void
_outXmlExpr(StringInfo str, const XmlExpr *node)
{
  WRITE_NODE_TYPE("XmlExpr");

  WRITE_ENUM_FIELD(op);
  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(named_args);
  WRITE_NODE_PTR_FIELD(arg_names);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_ENUM_FIELD(xmloption);
  WRITE_UINT_FIELD(type);
  WRITE_INT_FIELD(typmod);
  WRITE_INT_FIELD(location);
}

static void
_outNullTest(StringInfo str, const NullTest *node)
{
  WRITE_NODE_TYPE("NullTest");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_ENUM_FIELD(nulltesttype);
  WRITE_BOOL_FIELD(argisrow);
  WRITE_INT_FIELD(location);
}

static void
_outBooleanTest(StringInfo str, const BooleanTest *node)
{
  WRITE_NODE_TYPE("BooleanTest");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_ENUM_FIELD(booltesttype);
  WRITE_INT_FIELD(location);
}

static void
_outCoerceToDomain(StringInfo str, const CoerceToDomain *node)
{
  WRITE_NODE_TYPE("CoerceToDomain");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_UINT_FIELD(resulttype);
  WRITE_INT_FIELD(resulttypmod);
  WRITE_UINT_FIELD(resultcollid);
  WRITE_ENUM_FIELD(coercionformat);
  WRITE_INT_FIELD(location);
}

static void
_outCoerceToDomainValue(StringInfo str, const CoerceToDomainValue *node)
{
  WRITE_NODE_TYPE("CoerceToDomainValue");

  WRITE_UINT_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_UINT_FIELD(collation);
  WRITE_INT_FIELD(location);
}

static void
_outSetToDefault(StringInfo str, const SetToDefault *node)
{
  WRITE_NODE_TYPE("SetToDefault");

  WRITE_UINT_FIELD(typeId);
  WRITE_INT_FIELD(typeMod);
  WRITE_UINT_FIELD(collation);
  WRITE_INT_FIELD(location);
}

static void
_outCurrentOfExpr(StringInfo str, const CurrentOfExpr *node)
{
  WRITE_NODE_TYPE("CurrentOfExpr");

  WRITE_UINT_FIELD(cvarno);
  WRITE_STRING_FIELD(cursor_name);
  WRITE_INT_FIELD(cursor_param);
}

static void
_outInferenceElem(StringInfo str, const InferenceElem *node)
{
  WRITE_NODE_TYPE("InferenceElem");

  WRITE_NODE_PTR_FIELD(expr);
  WRITE_UINT_FIELD(infercollid);
  WRITE_UINT_FIELD(inferopclass);
}

static void
_outTargetEntry(StringInfo str, const TargetEntry *node)
{
  WRITE_NODE_TYPE("TargetEntry");

  WRITE_NODE_PTR_FIELD(expr);
  WRITE_INT_FIELD(resno);
  WRITE_STRING_FIELD(resname);
  WRITE_UINT_FIELD(ressortgroupref);
  WRITE_UINT_FIELD(resorigtbl);
  WRITE_INT_FIELD(resorigcol);
  WRITE_BOOL_FIELD(resjunk);
}

static void
_outRangeTblRef(StringInfo str, const RangeTblRef *node)
{
  WRITE_NODE_TYPE("RangeTblRef");

  WRITE_INT_FIELD(rtindex);
}

static void
_outJoinExpr(StringInfo str, const JoinExpr *node)
{
  WRITE_NODE_TYPE("JoinExpr");

  WRITE_ENUM_FIELD(jointype);
  WRITE_BOOL_FIELD(isNatural);
  WRITE_NODE_PTR_FIELD(larg);
  WRITE_NODE_PTR_FIELD(rarg);
  WRITE_NODE_PTR_FIELD(usingClause);
  WRITE_NODE_PTR_FIELD(quals);
  WRITE_NODE_PTR_FIELD(alias);
  WRITE_INT_FIELD(rtindex);
}

static void
_outFromExpr(StringInfo str, const FromExpr *node)
{
  WRITE_NODE_TYPE("FromExpr");

  WRITE_NODE_PTR_FIELD(fromlist);
  WRITE_NODE_PTR_FIELD(quals);
}

static void
_outOnConflictExpr(StringInfo str, const OnConflictExpr *node)
{
  WRITE_NODE_TYPE("OnConflictExpr");

  WRITE_ENUM_FIELD(action);
  WRITE_NODE_PTR_FIELD(arbiterElems);
  WRITE_NODE_PTR_FIELD(arbiterWhere);
  WRITE_UINT_FIELD(constraint);
  WRITE_NODE_PTR_FIELD(onConflictSet);
  WRITE_NODE_PTR_FIELD(onConflictWhere);
  WRITE_INT_FIELD(exclRelIndex);
  WRITE_NODE_PTR_FIELD(exclRelTlist);
}

static void
_outIntoClause(StringInfo str, const IntoClause *node)
{
  WRITE_NODE_TYPE("IntoClause");

  WRITE_NODE_PTR_FIELD(rel);
  WRITE_NODE_PTR_FIELD(colNames);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_ENUM_FIELD(onCommit);
  WRITE_STRING_FIELD(tableSpaceName);
  WRITE_NODE_PTR_FIELD(viewQuery);
  WRITE_BOOL_FIELD(skipData);
}

static void
_outQuery(StringInfo str, const Query *node)
{
  WRITE_NODE_TYPE("Query");

  WRITE_ENUM_FIELD(commandType);
  WRITE_ENUM_FIELD(querySource);
  WRITE_BOOL_FIELD(canSetTag);
  WRITE_NODE_PTR_FIELD(utilityStmt);
  WRITE_INT_FIELD(resultRelation);
  WRITE_BOOL_FIELD(hasAggs);
  WRITE_BOOL_FIELD(hasWindowFuncs);
  WRITE_BOOL_FIELD(hasSubLinks);
  WRITE_BOOL_FIELD(hasDistinctOn);
  WRITE_BOOL_FIELD(hasRecursive);
  WRITE_BOOL_FIELD(hasModifyingCTE);
  WRITE_BOOL_FIELD(hasForUpdate);
  WRITE_BOOL_FIELD(hasRowSecurity);
  WRITE_NODE_PTR_FIELD(cteList);
  WRITE_NODE_PTR_FIELD(rtable);
  WRITE_NODE_PTR_FIELD(jointree);
  WRITE_NODE_PTR_FIELD(targetList);
  WRITE_NODE_PTR_FIELD(onConflict);
  WRITE_NODE_PTR_FIELD(returningList);
  WRITE_NODE_PTR_FIELD(groupClause);
  WRITE_NODE_PTR_FIELD(groupingSets);
  WRITE_NODE_PTR_FIELD(havingQual);
  WRITE_NODE_PTR_FIELD(windowClause);
  WRITE_NODE_PTR_FIELD(distinctClause);
  WRITE_NODE_PTR_FIELD(sortClause);
  WRITE_NODE_PTR_FIELD(limitOffset);
  WRITE_NODE_PTR_FIELD(limitCount);
  WRITE_NODE_PTR_FIELD(rowMarks);
  WRITE_NODE_PTR_FIELD(setOperations);
  WRITE_NODE_PTR_FIELD(constraintDeps);
  WRITE_NODE_PTR_FIELD(withCheckOptions);
}

static void
_outInsertStmt(StringInfo str, const InsertStmt *node)
{
  WRITE_NODE_TYPE("InsertStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(cols);
  WRITE_NODE_PTR_FIELD(selectStmt);
  WRITE_NODE_PTR_FIELD(onConflictClause);
  WRITE_NODE_PTR_FIELD(returningList);
  WRITE_NODE_PTR_FIELD(withClause);
}

static void
_outDeleteStmt(StringInfo str, const DeleteStmt *node)
{
  WRITE_NODE_TYPE("DeleteStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(usingClause);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_NODE_PTR_FIELD(returningList);
  WRITE_NODE_PTR_FIELD(withClause);
}

static void
_outUpdateStmt(StringInfo str, const UpdateStmt *node)
{
  WRITE_NODE_TYPE("UpdateStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(targetList);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_NODE_PTR_FIELD(fromClause);
  WRITE_NODE_PTR_FIELD(returningList);
  WRITE_NODE_PTR_FIELD(withClause);
}

static void
_outSelectStmt(StringInfo str, const SelectStmt *node)
{
  WRITE_NODE_TYPE("SelectStmt");

  WRITE_NODE_PTR_FIELD(distinctClause);
  WRITE_NODE_PTR_FIELD(intoClause);
  WRITE_NODE_PTR_FIELD(targetList);
  WRITE_NODE_PTR_FIELD(fromClause);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_NODE_PTR_FIELD(groupClause);
  WRITE_NODE_PTR_FIELD(havingClause);
  WRITE_NODE_PTR_FIELD(windowClause);
  WRITE_NODE_PTR_FIELD(valuesLists);
  WRITE_NODE_PTR_FIELD(sortClause);
  WRITE_NODE_PTR_FIELD(limitOffset);
  WRITE_NODE_PTR_FIELD(limitCount);
  WRITE_NODE_PTR_FIELD(lockingClause);
  WRITE_NODE_PTR_FIELD(withClause);
  WRITE_ENUM_FIELD(op);
  WRITE_BOOL_FIELD(all);
  WRITE_NODE_PTR_FIELD(larg);
  WRITE_NODE_PTR_FIELD(rarg);
}

static void
_outAlterTableStmt(StringInfo str, const AlterTableStmt *node)
{
  WRITE_NODE_TYPE("AlterTableStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(cmds);
  WRITE_ENUM_FIELD(relkind);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterTableCmd(StringInfo str, const AlterTableCmd *node)
{
  WRITE_NODE_TYPE("AlterTableCmd");

  WRITE_ENUM_FIELD(subtype);
  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(newowner);
  WRITE_NODE_PTR_FIELD(def);
  WRITE_ENUM_FIELD(behavior);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterDomainStmt(StringInfo str, const AlterDomainStmt *node)
{
  WRITE_NODE_TYPE("AlterDomainStmt");

  WRITE_CHAR_FIELD(subtype);
  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(def);
  WRITE_ENUM_FIELD(behavior);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outSetOperationStmt(StringInfo str, const SetOperationStmt *node)
{
  WRITE_NODE_TYPE("SetOperationStmt");

  WRITE_ENUM_FIELD(op);
  WRITE_BOOL_FIELD(all);
  WRITE_NODE_PTR_FIELD(larg);
  WRITE_NODE_PTR_FIELD(rarg);
  WRITE_NODE_PTR_FIELD(colTypes);
  WRITE_NODE_PTR_FIELD(colTypmods);
  WRITE_NODE_PTR_FIELD(colCollations);
  WRITE_NODE_PTR_FIELD(groupClauses);
}

static void
_outGrantStmt(StringInfo str, const GrantStmt *node)
{
  WRITE_NODE_TYPE("GrantStmt");

  WRITE_BOOL_FIELD(is_grant);
  WRITE_ENUM_FIELD(targtype);
  WRITE_ENUM_FIELD(objtype);
  WRITE_NODE_PTR_FIELD(objects);
  WRITE_NODE_PTR_FIELD(privileges);
  WRITE_NODE_PTR_FIELD(grantees);
  WRITE_BOOL_FIELD(grant_option);
  WRITE_ENUM_FIELD(behavior);
}

static void
_outGrantRoleStmt(StringInfo str, const GrantRoleStmt *node)
{
  WRITE_NODE_TYPE("GrantRoleStmt");

  WRITE_NODE_PTR_FIELD(granted_roles);
  WRITE_NODE_PTR_FIELD(grantee_roles);
  WRITE_BOOL_FIELD(is_grant);
  WRITE_BOOL_FIELD(admin_opt);
  WRITE_NODE_PTR_FIELD(grantor);
  WRITE_ENUM_FIELD(behavior);
}

static void
_outAlterDefaultPrivilegesStmt(StringInfo str, const AlterDefaultPrivilegesStmt *node)
{
  WRITE_NODE_TYPE("AlterDefaultPrivilegesStmt");

  WRITE_NODE_PTR_FIELD(options);
  WRITE_NODE_PTR_FIELD(action);
}

static void
_outClosePortalStmt(StringInfo str, const ClosePortalStmt *node)
{
  WRITE_NODE_TYPE("ClosePortalStmt");

  WRITE_STRING_FIELD(portalname);
}

static void
_outClusterStmt(StringInfo str, const ClusterStmt *node)
{
  WRITE_NODE_TYPE("ClusterStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_STRING_FIELD(indexname);
  WRITE_BOOL_FIELD(verbose);
}

static void
_outCopyStmt(StringInfo str, const CopyStmt *node)
{
  WRITE_NODE_TYPE("CopyStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(query);
  WRITE_NODE_PTR_FIELD(attlist);
  WRITE_BOOL_FIELD(is_from);
  WRITE_BOOL_FIELD(is_program);
  WRITE_STRING_FIELD(filename);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outCreateStmt(StringInfo str, const CreateStmt *node)
{
  WRITE_NODE_TYPE("CreateStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(tableElts);
  WRITE_NODE_PTR_FIELD(inhRelations);
  WRITE_NODE_PTR_FIELD(ofTypename);
  WRITE_NODE_PTR_FIELD(constraints);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_ENUM_FIELD(oncommit);
  WRITE_STRING_FIELD(tablespacename);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outDefineStmt(StringInfo str, const DefineStmt *node)
{
  WRITE_NODE_TYPE("DefineStmt");

  WRITE_ENUM_FIELD(kind);
  WRITE_BOOL_FIELD(oldstyle);
  WRITE_NODE_PTR_FIELD(defnames);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(definition);
}

static void
_outDropStmt(StringInfo str, const DropStmt *node)
{
  WRITE_NODE_TYPE("DropStmt");

  WRITE_NODE_PTR_FIELD(objects);
  WRITE_NODE_PTR_FIELD(arguments);
  WRITE_ENUM_FIELD(removeType);
  WRITE_ENUM_FIELD(behavior);
  WRITE_BOOL_FIELD(missing_ok);
  WRITE_BOOL_FIELD(concurrent);
}

static void
_outTruncateStmt(StringInfo str, const TruncateStmt *node)
{
  WRITE_NODE_TYPE("TruncateStmt");

  WRITE_NODE_PTR_FIELD(relations);
  WRITE_BOOL_FIELD(restart_seqs);
  WRITE_ENUM_FIELD(behavior);
}

static void
_outCommentStmt(StringInfo str, const CommentStmt *node)
{
  WRITE_NODE_TYPE("CommentStmt");

  WRITE_ENUM_FIELD(objtype);
  WRITE_NODE_PTR_FIELD(objname);
  WRITE_NODE_PTR_FIELD(objargs);
  WRITE_STRING_FIELD(comment);
}

static void
_outFetchStmt(StringInfo str, const FetchStmt *node)
{
  WRITE_NODE_TYPE("FetchStmt");

  WRITE_ENUM_FIELD(direction);
  WRITE_LONG_FIELD(howMany);
  WRITE_STRING_FIELD(portalname);
  WRITE_BOOL_FIELD(ismove);
}

static void
_outIndexStmt(StringInfo str, const IndexStmt *node)
{
  WRITE_NODE_TYPE("IndexStmt");

  WRITE_STRING_FIELD(idxname);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_STRING_FIELD(accessMethod);
  WRITE_STRING_FIELD(tableSpace);
  WRITE_NODE_PTR_FIELD(indexParams);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_NODE_PTR_FIELD(excludeOpNames);
  WRITE_STRING_FIELD(idxcomment);
  WRITE_UINT_FIELD(indexOid);
  WRITE_UINT_FIELD(oldNode);
  WRITE_BOOL_FIELD(unique);
  WRITE_BOOL_FIELD(primary);
  WRITE_BOOL_FIELD(isconstraint);
  WRITE_BOOL_FIELD(deferrable);
  WRITE_BOOL_FIELD(initdeferred);
  WRITE_BOOL_FIELD(transformed);
  WRITE_BOOL_FIELD(concurrent);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outCreateFunctionStmt(StringInfo str, const CreateFunctionStmt *node)
{
  WRITE_NODE_TYPE("CreateFunctionStmt");

  WRITE_BOOL_FIELD(replace);
  WRITE_NODE_PTR_FIELD(funcname);
  WRITE_NODE_PTR_FIELD(parameters);
  WRITE_NODE_PTR_FIELD(returnType);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_NODE_PTR_FIELD(withClause);
}

static void
_outAlterFunctionStmt(StringInfo str, const AlterFunctionStmt *node)
{
  WRITE_NODE_TYPE("AlterFunctionStmt");

  WRITE_NODE_PTR_FIELD(func);
  WRITE_NODE_PTR_FIELD(actions);
}

static void
_outDoStmt(StringInfo str, const DoStmt *node)
{
  WRITE_NODE_TYPE("DoStmt");

  WRITE_NODE_PTR_FIELD(args);
}

static void
_outRenameStmt(StringInfo str, const RenameStmt *node)
{
  WRITE_NODE_TYPE("RenameStmt");

  WRITE_ENUM_FIELD(renameType);
  WRITE_ENUM_FIELD(relationType);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(object);
  WRITE_NODE_PTR_FIELD(objarg);
  WRITE_STRING_FIELD(subname);
  WRITE_STRING_FIELD(newname);
  WRITE_ENUM_FIELD(behavior);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outRuleStmt(StringInfo str, const RuleStmt *node)
{
  WRITE_NODE_TYPE("RuleStmt");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_STRING_FIELD(rulename);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_ENUM_FIELD(event);
  WRITE_BOOL_FIELD(instead);
  WRITE_NODE_PTR_FIELD(actions);
  WRITE_BOOL_FIELD(replace);
}

static void
_outNotifyStmt(StringInfo str, const NotifyStmt *node)
{
  WRITE_NODE_TYPE("NotifyStmt");

  WRITE_STRING_FIELD(conditionname);
  WRITE_STRING_FIELD(payload);
}

static void
_outListenStmt(StringInfo str, const ListenStmt *node)
{
  WRITE_NODE_TYPE("ListenStmt");

  WRITE_STRING_FIELD(conditionname);
}

static void
_outUnlistenStmt(StringInfo str, const UnlistenStmt *node)
{
  WRITE_NODE_TYPE("UnlistenStmt");

  WRITE_STRING_FIELD(conditionname);
}

static void
_outTransactionStmt(StringInfo str, const TransactionStmt *node)
{
  WRITE_NODE_TYPE("TransactionStmt");

  WRITE_ENUM_FIELD(kind);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_STRING_FIELD(gid);
}

static void
_outViewStmt(StringInfo str, const ViewStmt *node)
{
  WRITE_NODE_TYPE("ViewStmt");

  WRITE_NODE_PTR_FIELD(view);
  WRITE_NODE_PTR_FIELD(aliases);
  WRITE_NODE_PTR_FIELD(query);
  WRITE_BOOL_FIELD(replace);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_ENUM_FIELD(withCheckOption);
}

static void
_outLoadStmt(StringInfo str, const LoadStmt *node)
{
  WRITE_NODE_TYPE("LoadStmt");

  WRITE_STRING_FIELD(filename);
}

static void
_outCreateDomainStmt(StringInfo str, const CreateDomainStmt *node)
{
  WRITE_NODE_TYPE("CreateDomainStmt");

  WRITE_NODE_PTR_FIELD(domainname);
  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_NODE_PTR_FIELD(collClause);
  WRITE_NODE_PTR_FIELD(constraints);
}

static void
_outCreatedbStmt(StringInfo str, const CreatedbStmt *node)
{
  WRITE_NODE_TYPE("CreatedbStmt");

  WRITE_STRING_FIELD(dbname);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outDropdbStmt(StringInfo str, const DropdbStmt *node)
{
  WRITE_NODE_TYPE("DropdbStmt");

  WRITE_STRING_FIELD(dbname);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outVacuumStmt(StringInfo str, const VacuumStmt *node)
{
  WRITE_NODE_TYPE("VacuumStmt");

  WRITE_INT_FIELD(options);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(va_cols);
}

static void
_outExplainStmt(StringInfo str, const ExplainStmt *node)
{
  WRITE_NODE_TYPE("ExplainStmt");

  WRITE_NODE_PTR_FIELD(query);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outCreateTableAsStmt(StringInfo str, const CreateTableAsStmt *node)
{
  WRITE_NODE_TYPE("CreateTableAsStmt");

  WRITE_NODE_PTR_FIELD(query);
  WRITE_NODE_PTR_FIELD(into);
  WRITE_ENUM_FIELD(relkind);
  WRITE_BOOL_FIELD(is_select_into);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outCreateSeqStmt(StringInfo str, const CreateSeqStmt *node)
{
  WRITE_NODE_TYPE("CreateSeqStmt");

  WRITE_NODE_PTR_FIELD(sequence);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_UINT_FIELD(ownerId);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outAlterSeqStmt(StringInfo str, const AlterSeqStmt *node)
{
  WRITE_NODE_TYPE("AlterSeqStmt");

  WRITE_NODE_PTR_FIELD(sequence);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outVariableSetStmt(StringInfo str, const VariableSetStmt *node)
{
  WRITE_NODE_TYPE("VariableSetStmt");

  WRITE_ENUM_FIELD(kind);
  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_BOOL_FIELD(is_local);
}

static void
_outVariableShowStmt(StringInfo str, const VariableShowStmt *node)
{
  WRITE_NODE_TYPE("VariableShowStmt");

  WRITE_STRING_FIELD(name);
}

static void
_outDiscardStmt(StringInfo str, const DiscardStmt *node)
{
  WRITE_NODE_TYPE("DiscardStmt");

  WRITE_ENUM_FIELD(target);
}

static void
_outCreateTrigStmt(StringInfo str, const CreateTrigStmt *node)
{
  WRITE_NODE_TYPE("CreateTrigStmt");

  WRITE_STRING_FIELD(trigname);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(funcname);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_BOOL_FIELD(row);
  WRITE_INT_FIELD(timing);
  WRITE_INT_FIELD(events);
  WRITE_NODE_PTR_FIELD(columns);
  WRITE_NODE_PTR_FIELD(whenClause);
  WRITE_BOOL_FIELD(isconstraint);
  WRITE_BOOL_FIELD(deferrable);
  WRITE_BOOL_FIELD(initdeferred);
  WRITE_NODE_PTR_FIELD(constrrel);
}

static void
_outCreatePLangStmt(StringInfo str, const CreatePLangStmt *node)
{
  WRITE_NODE_TYPE("CreatePLangStmt");

  WRITE_BOOL_FIELD(replace);
  WRITE_STRING_FIELD(plname);
  WRITE_NODE_PTR_FIELD(plhandler);
  WRITE_NODE_PTR_FIELD(plinline);
  WRITE_NODE_PTR_FIELD(plvalidator);
  WRITE_BOOL_FIELD(pltrusted);
}

static void
_outCreateRoleStmt(StringInfo str, const CreateRoleStmt *node)
{
  WRITE_NODE_TYPE("CreateRoleStmt");

  WRITE_ENUM_FIELD(stmt_type);
  WRITE_STRING_FIELD(role);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterRoleStmt(StringInfo str, const AlterRoleStmt *node)
{
  WRITE_NODE_TYPE("AlterRoleStmt");

  WRITE_NODE_PTR_FIELD(role);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_INT_FIELD(action);
}

static void
_outDropRoleStmt(StringInfo str, const DropRoleStmt *node)
{
  WRITE_NODE_TYPE("DropRoleStmt");

  WRITE_NODE_PTR_FIELD(roles);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outLockStmt(StringInfo str, const LockStmt *node)
{
  WRITE_NODE_TYPE("LockStmt");

  WRITE_NODE_PTR_FIELD(relations);
  WRITE_INT_FIELD(mode);
  WRITE_BOOL_FIELD(nowait);
}

static void
_outConstraintsSetStmt(StringInfo str, const ConstraintsSetStmt *node)
{
  WRITE_NODE_TYPE("ConstraintsSetStmt");

  WRITE_NODE_PTR_FIELD(constraints);
  WRITE_BOOL_FIELD(deferred);
}

static void
_outReindexStmt(StringInfo str, const ReindexStmt *node)
{
  WRITE_NODE_TYPE("ReindexStmt");

  WRITE_ENUM_FIELD(kind);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_STRING_FIELD(name);
  WRITE_INT_FIELD(options);
}

static void
_outCheckPointStmt(StringInfo str, const CheckPointStmt *node)
{
  WRITE_NODE_TYPE("CheckPointStmt");

}

static void
_outCreateSchemaStmt(StringInfo str, const CreateSchemaStmt *node)
{
  WRITE_NODE_TYPE("CreateSchemaStmt");

  WRITE_STRING_FIELD(schemaname);
  WRITE_NODE_PTR_FIELD(authrole);
  WRITE_NODE_PTR_FIELD(schemaElts);
  WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outAlterDatabaseStmt(StringInfo str, const AlterDatabaseStmt *node)
{
  WRITE_NODE_TYPE("AlterDatabaseStmt");

  WRITE_STRING_FIELD(dbname);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterDatabaseSetStmt(StringInfo str, const AlterDatabaseSetStmt *node)
{
  WRITE_NODE_TYPE("AlterDatabaseSetStmt");

  WRITE_STRING_FIELD(dbname);
  WRITE_NODE_PTR_FIELD(setstmt);
}

static void
_outAlterRoleSetStmt(StringInfo str, const AlterRoleSetStmt *node)
{
  WRITE_NODE_TYPE("AlterRoleSetStmt");

  WRITE_NODE_PTR_FIELD(role);
  WRITE_STRING_FIELD(database);
  WRITE_NODE_PTR_FIELD(setstmt);
}

static void
_outCreateConversionStmt(StringInfo str, const CreateConversionStmt *node)
{
  WRITE_NODE_TYPE("CreateConversionStmt");

  WRITE_NODE_PTR_FIELD(conversion_name);
  WRITE_STRING_FIELD(for_encoding_name);
  WRITE_STRING_FIELD(to_encoding_name);
  WRITE_NODE_PTR_FIELD(func_name);
  WRITE_BOOL_FIELD(def);
}

static void
_outCreateCastStmt(StringInfo str, const CreateCastStmt *node)
{
  WRITE_NODE_TYPE("CreateCastStmt");

  WRITE_NODE_PTR_FIELD(sourcetype);
  WRITE_NODE_PTR_FIELD(targettype);
  WRITE_NODE_PTR_FIELD(func);
  WRITE_ENUM_FIELD(context);
  WRITE_BOOL_FIELD(inout);
}

static void
_outCreateOpClassStmt(StringInfo str, const CreateOpClassStmt *node)
{
  WRITE_NODE_TYPE("CreateOpClassStmt");

  WRITE_NODE_PTR_FIELD(opclassname);
  WRITE_NODE_PTR_FIELD(opfamilyname);
  WRITE_STRING_FIELD(amname);
  WRITE_NODE_PTR_FIELD(datatype);
  WRITE_NODE_PTR_FIELD(items);
  WRITE_BOOL_FIELD(isDefault);
}

static void
_outCreateOpFamilyStmt(StringInfo str, const CreateOpFamilyStmt *node)
{
  WRITE_NODE_TYPE("CreateOpFamilyStmt");

  WRITE_NODE_PTR_FIELD(opfamilyname);
  WRITE_STRING_FIELD(amname);
}

static void
_outAlterOpFamilyStmt(StringInfo str, const AlterOpFamilyStmt *node)
{
  WRITE_NODE_TYPE("AlterOpFamilyStmt");

  WRITE_NODE_PTR_FIELD(opfamilyname);
  WRITE_STRING_FIELD(amname);
  WRITE_BOOL_FIELD(isDrop);
  WRITE_NODE_PTR_FIELD(items);
}

static void
_outPrepareStmt(StringInfo str, const PrepareStmt *node)
{
  WRITE_NODE_TYPE("PrepareStmt");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(argtypes);
  WRITE_NODE_PTR_FIELD(query);
}

static void
_outExecuteStmt(StringInfo str, const ExecuteStmt *node)
{
  WRITE_NODE_TYPE("ExecuteStmt");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(params);
}

static void
_outDeallocateStmt(StringInfo str, const DeallocateStmt *node)
{
  WRITE_NODE_TYPE("DeallocateStmt");

  WRITE_STRING_FIELD(name);
}

static void
_outDeclareCursorStmt(StringInfo str, const DeclareCursorStmt *node)
{
  WRITE_NODE_TYPE("DeclareCursorStmt");

  WRITE_STRING_FIELD(portalname);
  WRITE_INT_FIELD(options);
  WRITE_NODE_PTR_FIELD(query);
}

static void
_outCreateTableSpaceStmt(StringInfo str, const CreateTableSpaceStmt *node)
{
  WRITE_NODE_TYPE("CreateTableSpaceStmt");

  WRITE_STRING_FIELD(tablespacename);
  WRITE_NODE_PTR_FIELD(owner);
  WRITE_STRING_FIELD(location);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outDropTableSpaceStmt(StringInfo str, const DropTableSpaceStmt *node)
{
  WRITE_NODE_TYPE("DropTableSpaceStmt");

  WRITE_STRING_FIELD(tablespacename);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterObjectSchemaStmt(StringInfo str, const AlterObjectSchemaStmt *node)
{
  WRITE_NODE_TYPE("AlterObjectSchemaStmt");

  WRITE_ENUM_FIELD(objectType);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(object);
  WRITE_NODE_PTR_FIELD(objarg);
  WRITE_STRING_FIELD(newschema);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterOwnerStmt(StringInfo str, const AlterOwnerStmt *node)
{
  WRITE_NODE_TYPE("AlterOwnerStmt");

  WRITE_ENUM_FIELD(objectType);
  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(object);
  WRITE_NODE_PTR_FIELD(objarg);
  WRITE_NODE_PTR_FIELD(newowner);
}

static void
_outDropOwnedStmt(StringInfo str, const DropOwnedStmt *node)
{
  WRITE_NODE_TYPE("DropOwnedStmt");

  WRITE_NODE_PTR_FIELD(roles);
  WRITE_ENUM_FIELD(behavior);
}

static void
_outReassignOwnedStmt(StringInfo str, const ReassignOwnedStmt *node)
{
  WRITE_NODE_TYPE("ReassignOwnedStmt");

  WRITE_NODE_PTR_FIELD(roles);
  WRITE_NODE_PTR_FIELD(newrole);
}

static void
_outCompositeTypeStmt(StringInfo str, const CompositeTypeStmt *node)
{
  WRITE_NODE_TYPE("CompositeTypeStmt");

  WRITE_NODE_PTR_FIELD(typevar);
  WRITE_NODE_PTR_FIELD(coldeflist);
}

static void
_outCreateEnumStmt(StringInfo str, const CreateEnumStmt *node)
{
  WRITE_NODE_TYPE("CreateEnumStmt");

  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_NODE_PTR_FIELD(vals);
}

static void
_outCreateRangeStmt(StringInfo str, const CreateRangeStmt *node)
{
  WRITE_NODE_TYPE("CreateRangeStmt");

  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_NODE_PTR_FIELD(params);
}

static void
_outAlterEnumStmt(StringInfo str, const AlterEnumStmt *node)
{
  WRITE_NODE_TYPE("AlterEnumStmt");

  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_STRING_FIELD(newVal);
  WRITE_STRING_FIELD(newValNeighbor);
  WRITE_BOOL_FIELD(newValIsAfter);
  WRITE_BOOL_FIELD(skipIfExists);
}

static void
_outAlterTSDictionaryStmt(StringInfo str, const AlterTSDictionaryStmt *node)
{
  WRITE_NODE_TYPE("AlterTSDictionaryStmt");

  WRITE_NODE_PTR_FIELD(dictname);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterTSConfigurationStmt(StringInfo str, const AlterTSConfigurationStmt *node)
{
  WRITE_NODE_TYPE("AlterTSConfigurationStmt");

  WRITE_ENUM_FIELD(kind);
  WRITE_NODE_PTR_FIELD(cfgname);
  WRITE_NODE_PTR_FIELD(tokentype);
  WRITE_NODE_PTR_FIELD(dicts);
  WRITE_BOOL_FIELD(override);
  WRITE_BOOL_FIELD(replace);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outCreateFdwStmt(StringInfo str, const CreateFdwStmt *node)
{
  WRITE_NODE_TYPE("CreateFdwStmt");

  WRITE_STRING_FIELD(fdwname);
  WRITE_NODE_PTR_FIELD(func_options);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterFdwStmt(StringInfo str, const AlterFdwStmt *node)
{
  WRITE_NODE_TYPE("AlterFdwStmt");

  WRITE_STRING_FIELD(fdwname);
  WRITE_NODE_PTR_FIELD(func_options);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outCreateForeignServerStmt(StringInfo str, const CreateForeignServerStmt *node)
{
  WRITE_NODE_TYPE("CreateForeignServerStmt");

  WRITE_STRING_FIELD(servername);
  WRITE_STRING_FIELD(servertype);
  WRITE_STRING_FIELD(version);
  WRITE_STRING_FIELD(fdwname);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterForeignServerStmt(StringInfo str, const AlterForeignServerStmt *node)
{
  WRITE_NODE_TYPE("AlterForeignServerStmt");

  WRITE_STRING_FIELD(servername);
  WRITE_STRING_FIELD(version);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_BOOL_FIELD(has_version);
}

static void
_outCreateUserMappingStmt(StringInfo str, const CreateUserMappingStmt *node)
{
  WRITE_NODE_TYPE("CreateUserMappingStmt");

  WRITE_NODE_PTR_FIELD(user);
  WRITE_STRING_FIELD(servername);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterUserMappingStmt(StringInfo str, const AlterUserMappingStmt *node)
{
  WRITE_NODE_TYPE("AlterUserMappingStmt");

  WRITE_NODE_PTR_FIELD(user);
  WRITE_STRING_FIELD(servername);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outDropUserMappingStmt(StringInfo str, const DropUserMappingStmt *node)
{
  WRITE_NODE_TYPE("DropUserMappingStmt");

  WRITE_NODE_PTR_FIELD(user);
  WRITE_STRING_FIELD(servername);
  WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterTableSpaceOptionsStmt(StringInfo str, const AlterTableSpaceOptionsStmt *node)
{
  WRITE_NODE_TYPE("AlterTableSpaceOptionsStmt");

  WRITE_STRING_FIELD(tablespacename);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_BOOL_FIELD(isReset);
}

static void
_outAlterTableMoveAllStmt(StringInfo str, const AlterTableMoveAllStmt *node)
{
  WRITE_NODE_TYPE("AlterTableMoveAllStmt");

  WRITE_STRING_FIELD(orig_tablespacename);
  WRITE_ENUM_FIELD(objtype);
  WRITE_NODE_PTR_FIELD(roles);
  WRITE_STRING_FIELD(new_tablespacename);
  WRITE_BOOL_FIELD(nowait);
}

static void
_outSecLabelStmt(StringInfo str, const SecLabelStmt *node)
{
  WRITE_NODE_TYPE("SecLabelStmt");

  WRITE_ENUM_FIELD(objtype);
  WRITE_NODE_PTR_FIELD(objname);
  WRITE_NODE_PTR_FIELD(objargs);
  WRITE_STRING_FIELD(provider);
  WRITE_STRING_FIELD(label);
}

static void
_outCreateForeignTableStmt(StringInfo str, const CreateForeignTableStmt *node)
{
  WRITE_NODE_TYPE("CreateForeignTableStmt");

  WRITE_NODE_FIELD_WITH_TYPE(base, CreateStmt);
  WRITE_STRING_FIELD(servername);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outImportForeignSchemaStmt(StringInfo str, const ImportForeignSchemaStmt *node)
{
  WRITE_NODE_TYPE("ImportForeignSchemaStmt");

  WRITE_STRING_FIELD(server_name);
  WRITE_STRING_FIELD(remote_schema);
  WRITE_STRING_FIELD(local_schema);
  WRITE_ENUM_FIELD(list_type);
  WRITE_NODE_PTR_FIELD(table_list);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outCreateExtensionStmt(StringInfo str, const CreateExtensionStmt *node)
{
  WRITE_NODE_TYPE("CreateExtensionStmt");

  WRITE_STRING_FIELD(extname);
  WRITE_BOOL_FIELD(if_not_exists);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterExtensionStmt(StringInfo str, const AlterExtensionStmt *node)
{
  WRITE_NODE_TYPE("AlterExtensionStmt");

  WRITE_STRING_FIELD(extname);
  WRITE_NODE_PTR_FIELD(options);
}

static void
_outAlterExtensionContentsStmt(StringInfo str, const AlterExtensionContentsStmt *node)
{
  WRITE_NODE_TYPE("AlterExtensionContentsStmt");

  WRITE_STRING_FIELD(extname);
  WRITE_INT_FIELD(action);
  WRITE_ENUM_FIELD(objtype);
  WRITE_NODE_PTR_FIELD(objname);
  WRITE_NODE_PTR_FIELD(objargs);
}

static void
_outCreateEventTrigStmt(StringInfo str, const CreateEventTrigStmt *node)
{
  WRITE_NODE_TYPE("CreateEventTrigStmt");

  WRITE_STRING_FIELD(trigname);
  WRITE_STRING_FIELD(eventname);
  WRITE_NODE_PTR_FIELD(whenclause);
  WRITE_NODE_PTR_FIELD(funcname);
}

static void
_outAlterEventTrigStmt(StringInfo str, const AlterEventTrigStmt *node)
{
  WRITE_NODE_TYPE("AlterEventTrigStmt");

  WRITE_STRING_FIELD(trigname);
  WRITE_CHAR_FIELD(tgenabled);
}

static void
_outRefreshMatViewStmt(StringInfo str, const RefreshMatViewStmt *node)
{
  WRITE_NODE_TYPE("RefreshMatViewStmt");

  WRITE_BOOL_FIELD(concurrent);
  WRITE_BOOL_FIELD(skipData);
  WRITE_NODE_PTR_FIELD(relation);
}

static void
_outReplicaIdentityStmt(StringInfo str, const ReplicaIdentityStmt *node)
{
  WRITE_NODE_TYPE("ReplicaIdentityStmt");

  WRITE_CHAR_FIELD(identity_type);
  WRITE_STRING_FIELD(name);
}

static void
_outAlterSystemStmt(StringInfo str, const AlterSystemStmt *node)
{
  WRITE_NODE_TYPE("AlterSystemStmt");

  WRITE_NODE_PTR_FIELD(setstmt);
}

static void
_outCreatePolicyStmt(StringInfo str, const CreatePolicyStmt *node)
{
  WRITE_NODE_TYPE("CreatePolicyStmt");

  WRITE_STRING_FIELD(policy_name);
  WRITE_NODE_PTR_FIELD(table);
  WRITE_STRING_FIELD(cmd_name);
  WRITE_NODE_PTR_FIELD(roles);
  WRITE_NODE_PTR_FIELD(qual);
  WRITE_NODE_PTR_FIELD(with_check);
}

static void
_outAlterPolicyStmt(StringInfo str, const AlterPolicyStmt *node)
{
  WRITE_NODE_TYPE("AlterPolicyStmt");

  WRITE_STRING_FIELD(policy_name);
  WRITE_NODE_PTR_FIELD(table);
  WRITE_NODE_PTR_FIELD(roles);
  WRITE_NODE_PTR_FIELD(qual);
  WRITE_NODE_PTR_FIELD(with_check);
}

static void
_outCreateTransformStmt(StringInfo str, const CreateTransformStmt *node)
{
  WRITE_NODE_TYPE("CreateTransformStmt");

  WRITE_BOOL_FIELD(replace);
  WRITE_NODE_PTR_FIELD(type_name);
  WRITE_STRING_FIELD(lang);
  WRITE_NODE_PTR_FIELD(fromsql);
  WRITE_NODE_PTR_FIELD(tosql);
}

static void
_outA_Expr(StringInfo str, const A_Expr *node)
{
  WRITE_NODE_TYPE("A_Expr");

  WRITE_ENUM_FIELD(kind);
  WRITE_NODE_PTR_FIELD(name);
  WRITE_NODE_PTR_FIELD(lexpr);
  WRITE_NODE_PTR_FIELD(rexpr);
  WRITE_INT_FIELD(location);
}

static void
_outColumnRef(StringInfo str, const ColumnRef *node)
{
  WRITE_NODE_TYPE("ColumnRef");

  WRITE_NODE_PTR_FIELD(fields);
  WRITE_INT_FIELD(location);
}

static void
_outParamRef(StringInfo str, const ParamRef *node)
{
  WRITE_NODE_TYPE("ParamRef");

  WRITE_INT_FIELD(number);
  WRITE_INT_FIELD(location);
}

static void
_outA_Const(StringInfo str, const A_Const *node)
{
  WRITE_NODE_TYPE("A_Const");

  WRITE_NODE_FIELD(val);
  WRITE_INT_FIELD(location);
}

static void
_outFuncCall(StringInfo str, const FuncCall *node)
{
  WRITE_NODE_TYPE("FuncCall");

  WRITE_NODE_PTR_FIELD(funcname);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(agg_order);
  WRITE_NODE_PTR_FIELD(agg_filter);
  WRITE_BOOL_FIELD(agg_within_group);
  WRITE_BOOL_FIELD(agg_star);
  WRITE_BOOL_FIELD(agg_distinct);
  WRITE_BOOL_FIELD(func_variadic);
  WRITE_NODE_PTR_FIELD(over);
  WRITE_INT_FIELD(location);
}

static void
_outA_Star(StringInfo str, const A_Star *node)
{
  WRITE_NODE_TYPE("A_Star");

}

static void
_outA_Indices(StringInfo str, const A_Indices *node)
{
  WRITE_NODE_TYPE("A_Indices");

  WRITE_NODE_PTR_FIELD(lidx);
  WRITE_NODE_PTR_FIELD(uidx);
}

static void
_outA_Indirection(StringInfo str, const A_Indirection *node)
{
  WRITE_NODE_TYPE("A_Indirection");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_NODE_PTR_FIELD(indirection);
}

static void
_outA_ArrayExpr(StringInfo str, const A_ArrayExpr *node)
{
  WRITE_NODE_TYPE("A_ArrayExpr");

  WRITE_NODE_PTR_FIELD(elements);
  WRITE_INT_FIELD(location);
}

static void
_outResTarget(StringInfo str, const ResTarget *node)
{
  WRITE_NODE_TYPE("ResTarget");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(indirection);
  WRITE_NODE_PTR_FIELD(val);
  WRITE_INT_FIELD(location);
}

static void
_outMultiAssignRef(StringInfo str, const MultiAssignRef *node)
{
  WRITE_NODE_TYPE("MultiAssignRef");

  WRITE_NODE_PTR_FIELD(source);
  WRITE_INT_FIELD(colno);
  WRITE_INT_FIELD(ncolumns);
}

static void
_outTypeCast(StringInfo str, const TypeCast *node)
{
  WRITE_NODE_TYPE("TypeCast");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_INT_FIELD(location);
}

static void
_outCollateClause(StringInfo str, const CollateClause *node)
{
  WRITE_NODE_TYPE("CollateClause");

  WRITE_NODE_PTR_FIELD(arg);
  WRITE_NODE_PTR_FIELD(collname);
  WRITE_INT_FIELD(location);
}

static void
_outSortBy(StringInfo str, const SortBy *node)
{
  WRITE_NODE_TYPE("SortBy");

  WRITE_NODE_PTR_FIELD(node);
  WRITE_ENUM_FIELD(sortby_dir);
  WRITE_ENUM_FIELD(sortby_nulls);
  WRITE_NODE_PTR_FIELD(useOp);
  WRITE_INT_FIELD(location);
}

static void
_outWindowDef(StringInfo str, const WindowDef *node)
{
  WRITE_NODE_TYPE("WindowDef");

  WRITE_STRING_FIELD(name);
  WRITE_STRING_FIELD(refname);
  WRITE_NODE_PTR_FIELD(partitionClause);
  WRITE_NODE_PTR_FIELD(orderClause);
  WRITE_INT_FIELD(frameOptions);
  WRITE_NODE_PTR_FIELD(startOffset);
  WRITE_NODE_PTR_FIELD(endOffset);
  WRITE_INT_FIELD(location);
}

static void
_outRangeSubselect(StringInfo str, const RangeSubselect *node)
{
  WRITE_NODE_TYPE("RangeSubselect");

  WRITE_BOOL_FIELD(lateral);
  WRITE_NODE_PTR_FIELD(subquery);
  WRITE_NODE_PTR_FIELD(alias);
}

static void
_outRangeFunction(StringInfo str, const RangeFunction *node)
{
  WRITE_NODE_TYPE("RangeFunction");

  WRITE_BOOL_FIELD(lateral);
  WRITE_BOOL_FIELD(ordinality);
  WRITE_BOOL_FIELD(is_rowsfrom);
  WRITE_NODE_PTR_FIELD(functions);
  WRITE_NODE_PTR_FIELD(alias);
  WRITE_NODE_PTR_FIELD(coldeflist);
}

static void
_outRangeTableSample(StringInfo str, const RangeTableSample *node)
{
  WRITE_NODE_TYPE("RangeTableSample");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_NODE_PTR_FIELD(method);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(repeatable);
  WRITE_INT_FIELD(location);
}

static void
_outTypeName(StringInfo str, const TypeName *node)
{
  WRITE_NODE_TYPE("TypeName");

  WRITE_NODE_PTR_FIELD(names);
  WRITE_UINT_FIELD(typeOid);
  WRITE_BOOL_FIELD(setof);
  WRITE_BOOL_FIELD(pct_type);
  WRITE_NODE_PTR_FIELD(typmods);
  WRITE_INT_FIELD(typemod);
  WRITE_NODE_PTR_FIELD(arrayBounds);
  WRITE_INT_FIELD(location);
}

static void
_outColumnDef(StringInfo str, const ColumnDef *node)
{
  WRITE_NODE_TYPE("ColumnDef");

  WRITE_STRING_FIELD(colname);
  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_INT_FIELD(inhcount);
  WRITE_BOOL_FIELD(is_local);
  WRITE_BOOL_FIELD(is_not_null);
  WRITE_BOOL_FIELD(is_from_type);
  WRITE_CHAR_FIELD(storage);
  WRITE_NODE_PTR_FIELD(raw_default);
  WRITE_NODE_PTR_FIELD(cooked_default);
  WRITE_NODE_PTR_FIELD(collClause);
  WRITE_UINT_FIELD(collOid);
  WRITE_NODE_PTR_FIELD(constraints);
  WRITE_NODE_PTR_FIELD(fdwoptions);
  WRITE_INT_FIELD(location);
}

static void
_outIndexElem(StringInfo str, const IndexElem *node)
{
  WRITE_NODE_TYPE("IndexElem");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(expr);
  WRITE_STRING_FIELD(indexcolname);
  WRITE_NODE_PTR_FIELD(collation);
  WRITE_NODE_PTR_FIELD(opclass);
  WRITE_ENUM_FIELD(ordering);
  WRITE_ENUM_FIELD(nulls_ordering);
}

static void
_outConstraint(StringInfo str, const Constraint *node)
{
  WRITE_NODE_TYPE("Constraint");

  WRITE_ENUM_FIELD(contype);
  WRITE_STRING_FIELD(conname);
  WRITE_BOOL_FIELD(deferrable);
  WRITE_BOOL_FIELD(initdeferred);
  WRITE_INT_FIELD(location);
  WRITE_BOOL_FIELD(is_no_inherit);
  WRITE_NODE_PTR_FIELD(raw_expr);
  WRITE_STRING_FIELD(cooked_expr);
  WRITE_NODE_PTR_FIELD(keys);
  WRITE_NODE_PTR_FIELD(exclusions);
  WRITE_NODE_PTR_FIELD(options);
  WRITE_STRING_FIELD(indexname);
  WRITE_STRING_FIELD(indexspace);
  WRITE_STRING_FIELD(access_method);
  WRITE_NODE_PTR_FIELD(where_clause);
  WRITE_NODE_PTR_FIELD(pktable);
  WRITE_NODE_PTR_FIELD(fk_attrs);
  WRITE_NODE_PTR_FIELD(pk_attrs);
  WRITE_CHAR_FIELD(fk_matchtype);
  WRITE_CHAR_FIELD(fk_upd_action);
  WRITE_CHAR_FIELD(fk_del_action);
  WRITE_NODE_PTR_FIELD(old_conpfeqop);
  WRITE_UINT_FIELD(old_pktable_oid);
  WRITE_BOOL_FIELD(skip_validation);
  WRITE_BOOL_FIELD(initially_valid);
}

static void
_outDefElem(StringInfo str, const DefElem *node)
{
  WRITE_NODE_TYPE("DefElem");

  WRITE_STRING_FIELD(defnamespace);
  WRITE_STRING_FIELD(defname);
  WRITE_NODE_PTR_FIELD(arg);
  WRITE_ENUM_FIELD(defaction);
  WRITE_INT_FIELD(location);
}

static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
  WRITE_NODE_TYPE("RangeTblEntry");

  WRITE_ENUM_FIELD(rtekind);
  WRITE_UINT_FIELD(relid);
  WRITE_CHAR_FIELD(relkind);
  WRITE_NODE_PTR_FIELD(tablesample);
  WRITE_NODE_PTR_FIELD(subquery);
  WRITE_BOOL_FIELD(security_barrier);
  WRITE_ENUM_FIELD(jointype);
  WRITE_NODE_PTR_FIELD(joinaliasvars);
  WRITE_NODE_PTR_FIELD(functions);
  WRITE_BOOL_FIELD(funcordinality);
  WRITE_NODE_PTR_FIELD(values_lists);
  WRITE_NODE_PTR_FIELD(values_collations);
  WRITE_STRING_FIELD(ctename);
  WRITE_UINT_FIELD(ctelevelsup);
  WRITE_BOOL_FIELD(self_reference);
  WRITE_NODE_PTR_FIELD(ctecoltypes);
  WRITE_NODE_PTR_FIELD(ctecoltypmods);
  WRITE_NODE_PTR_FIELD(ctecolcollations);
  WRITE_NODE_PTR_FIELD(alias);
  WRITE_NODE_PTR_FIELD(eref);
  WRITE_BOOL_FIELD(lateral);
  WRITE_BOOL_FIELD(inh);
  WRITE_BOOL_FIELD(inFromCl);
  WRITE_ENUM_FIELD(requiredPerms);
  WRITE_UINT_FIELD(checkAsUser);
  WRITE_BITMAPSET_FIELD(selectedCols);
  WRITE_BITMAPSET_FIELD(insertedCols);
  WRITE_BITMAPSET_FIELD(updatedCols);
  WRITE_NODE_PTR_FIELD(securityQuals);
}

static void
_outRangeTblFunction(StringInfo str, const RangeTblFunction *node)
{
  WRITE_NODE_TYPE("RangeTblFunction");

  WRITE_NODE_PTR_FIELD(funcexpr);
  WRITE_INT_FIELD(funccolcount);
  WRITE_NODE_PTR_FIELD(funccolnames);
  WRITE_NODE_PTR_FIELD(funccoltypes);
  WRITE_NODE_PTR_FIELD(funccoltypmods);
  WRITE_NODE_PTR_FIELD(funccolcollations);
  WRITE_BITMAPSET_FIELD(funcparams);
}

static void
_outTableSampleClause(StringInfo str, const TableSampleClause *node)
{
  WRITE_NODE_TYPE("TableSampleClause");

  WRITE_UINT_FIELD(tsmhandler);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_NODE_PTR_FIELD(repeatable);
}

static void
_outWithCheckOption(StringInfo str, const WithCheckOption *node)
{
  WRITE_NODE_TYPE("WithCheckOption");

  WRITE_ENUM_FIELD(kind);
  WRITE_STRING_FIELD(relname);
  WRITE_STRING_FIELD(polname);
  WRITE_NODE_PTR_FIELD(qual);
  WRITE_BOOL_FIELD(cascaded);
}

static void
_outSortGroupClause(StringInfo str, const SortGroupClause *node)
{
  WRITE_NODE_TYPE("SortGroupClause");

  WRITE_UINT_FIELD(tleSortGroupRef);
  WRITE_UINT_FIELD(eqop);
  WRITE_UINT_FIELD(sortop);
  WRITE_BOOL_FIELD(nulls_first);
  WRITE_BOOL_FIELD(hashable);
}

static void
_outGroupingSet(StringInfo str, const GroupingSet *node)
{
  WRITE_NODE_TYPE("GroupingSet");

  WRITE_ENUM_FIELD(kind);
  WRITE_NODE_PTR_FIELD(content);
  WRITE_INT_FIELD(location);
}

static void
_outWindowClause(StringInfo str, const WindowClause *node)
{
  WRITE_NODE_TYPE("WindowClause");

  WRITE_STRING_FIELD(name);
  WRITE_STRING_FIELD(refname);
  WRITE_NODE_PTR_FIELD(partitionClause);
  WRITE_NODE_PTR_FIELD(orderClause);
  WRITE_INT_FIELD(frameOptions);
  WRITE_NODE_PTR_FIELD(startOffset);
  WRITE_NODE_PTR_FIELD(endOffset);
  WRITE_UINT_FIELD(winref);
  WRITE_BOOL_FIELD(copiedOrder);
}

static void
_outFuncWithArgs(StringInfo str, const FuncWithArgs *node)
{
  WRITE_NODE_TYPE("FuncWithArgs");

  WRITE_NODE_PTR_FIELD(funcname);
  WRITE_NODE_PTR_FIELD(funcargs);
}

static void
_outAccessPriv(StringInfo str, const AccessPriv *node)
{
  WRITE_NODE_TYPE("AccessPriv");

  WRITE_STRING_FIELD(priv_name);
  WRITE_NODE_PTR_FIELD(cols);
}

static void
_outCreateOpClassItem(StringInfo str, const CreateOpClassItem *node)
{
  WRITE_NODE_TYPE("CreateOpClassItem");

  WRITE_INT_FIELD(itemtype);
  WRITE_NODE_PTR_FIELD(name);
  WRITE_NODE_PTR_FIELD(args);
  WRITE_INT_FIELD(number);
  WRITE_NODE_PTR_FIELD(order_family);
  WRITE_NODE_PTR_FIELD(class_args);
  WRITE_NODE_PTR_FIELD(storedtype);
}

static void
_outTableLikeClause(StringInfo str, const TableLikeClause *node)
{
  WRITE_NODE_TYPE("TableLikeClause");

  WRITE_NODE_PTR_FIELD(relation);
  WRITE_UINT_FIELD(options);
}

static void
_outFunctionParameter(StringInfo str, const FunctionParameter *node)
{
  WRITE_NODE_TYPE("FunctionParameter");

  WRITE_STRING_FIELD(name);
  WRITE_NODE_PTR_FIELD(argType);
  WRITE_ENUM_FIELD(mode);
  WRITE_NODE_PTR_FIELD(defexpr);
}

static void
_outLockingClause(StringInfo str, const LockingClause *node)
{
  WRITE_NODE_TYPE("LockingClause");

  WRITE_NODE_PTR_FIELD(lockedRels);
  WRITE_ENUM_FIELD(strength);
  WRITE_ENUM_FIELD(waitPolicy);
}

static void
_outRowMarkClause(StringInfo str, const RowMarkClause *node)
{
  WRITE_NODE_TYPE("RowMarkClause");

  WRITE_UINT_FIELD(rti);
  WRITE_ENUM_FIELD(strength);
  WRITE_ENUM_FIELD(waitPolicy);
  WRITE_BOOL_FIELD(pushedDown);
}

static void
_outXmlSerialize(StringInfo str, const XmlSerialize *node)
{
  WRITE_NODE_TYPE("XmlSerialize");

  WRITE_ENUM_FIELD(xmloption);
  WRITE_NODE_PTR_FIELD(expr);
  WRITE_NODE_PTR_FIELD(typeName);
  WRITE_INT_FIELD(location);
}

static void
_outWithClause(StringInfo str, const WithClause *node)
{
  WRITE_NODE_TYPE("WithClause");

  WRITE_NODE_PTR_FIELD(ctes);
  WRITE_BOOL_FIELD(recursive);
  WRITE_INT_FIELD(location);
}

static void
_outInferClause(StringInfo str, const InferClause *node)
{
  WRITE_NODE_TYPE("InferClause");

  WRITE_NODE_PTR_FIELD(indexElems);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_STRING_FIELD(conname);
  WRITE_INT_FIELD(location);
}

static void
_outOnConflictClause(StringInfo str, const OnConflictClause *node)
{
  WRITE_NODE_TYPE("OnConflictClause");

  WRITE_ENUM_FIELD(action);
  WRITE_NODE_PTR_FIELD(infer);
  WRITE_NODE_PTR_FIELD(targetList);
  WRITE_NODE_PTR_FIELD(whereClause);
  WRITE_INT_FIELD(location);
}

static void
_outCommonTableExpr(StringInfo str, const CommonTableExpr *node)
{
  WRITE_NODE_TYPE("CommonTableExpr");

  WRITE_STRING_FIELD(ctename);
  WRITE_NODE_PTR_FIELD(aliascolnames);
  WRITE_NODE_PTR_FIELD(ctequery);
  WRITE_INT_FIELD(location);
  WRITE_BOOL_FIELD(cterecursive);
  WRITE_INT_FIELD(cterefcount);
  WRITE_NODE_PTR_FIELD(ctecolnames);
  WRITE_NODE_PTR_FIELD(ctecoltypes);
  WRITE_NODE_PTR_FIELD(ctecoltypmods);
  WRITE_NODE_PTR_FIELD(ctecolcollations);
}

static void
_outRoleSpec(StringInfo str, const RoleSpec *node)
{
  WRITE_NODE_TYPE("RoleSpec");

  WRITE_ENUM_FIELD(roletype);
  WRITE_STRING_FIELD(rolename);
  WRITE_INT_FIELD(location);
}

static void
_outInlineCodeBlock(StringInfo str, const InlineCodeBlock *node)
{
  WRITE_NODE_TYPE("InlineCodeBlock");

  WRITE_STRING_FIELD(source_text);
  WRITE_UINT_FIELD(langOid);
  WRITE_BOOL_FIELD(langIsTrusted);
}

