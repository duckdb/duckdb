static void
_fingerprintAlias(FingerprintContext *ctx, const Alias *node, const void *parent, const char *field_name, unsigned int depth)
{
  // Intentionally ignoring all fields for fingerprinting
}

static void
_fingerprintRangeVar(FingerprintContext *ctx, const RangeVar *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeVar");
  if (node->alias != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->alias, node, "alias", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "alias");
  }

  if (node->catalogname != NULL) {
    _fingerprintString(ctx, "catalogname");
    _fingerprintString(ctx, node->catalogname);
  }

  if (node->inhOpt != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inhOpt);
    _fingerprintString(ctx, "inhOpt");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting

  if (node->relname != NULL) {
    _fingerprintString(ctx, "relname");
    _fingerprintString(ctx, node->relname);
  }

  if (node->relpersistence != 0) {
    char str[2] = {node->relpersistence, '\0'};
    _fingerprintString(ctx, "relpersistence");
    _fingerprintString(ctx, str);
  }


  if (node->schemaname != NULL) {
    _fingerprintString(ctx, "schemaname");
    _fingerprintString(ctx, node->schemaname);
  }

}

static void
_fingerprintExpr(FingerprintContext *ctx, const Expr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Expr");
}

static void
_fingerprintVar(FingerprintContext *ctx, const Var *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Var");
  // Intentionally ignoring node->location for fingerprinting
  if (node->varattno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varattno);
    _fingerprintString(ctx, "varattno");
    _fingerprintString(ctx, buffer);
  }

  if (node->varcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varcollid);
    _fingerprintString(ctx, "varcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->varlevelsup != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varlevelsup);
    _fingerprintString(ctx, "varlevelsup");
    _fingerprintString(ctx, buffer);
  }

  if (node->varno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varno);
    _fingerprintString(ctx, "varno");
    _fingerprintString(ctx, buffer);
  }

  if (node->varnoold != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varnoold);
    _fingerprintString(ctx, "varnoold");
    _fingerprintString(ctx, buffer);
  }

  if (node->varoattno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->varoattno);
    _fingerprintString(ctx, "varoattno");
    _fingerprintString(ctx, buffer);
  }

  if (node->vartype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->vartype);
    _fingerprintString(ctx, "vartype");
    _fingerprintString(ctx, buffer);
  }

  if (node->vartypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->vartypmod);
    _fingerprintString(ctx, "vartypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintConst(FingerprintContext *ctx, const Const *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Const");

  if (node->constbyval) {    _fingerprintString(ctx, "constbyval");
    _fingerprintString(ctx, "true");
  }

  if (node->constcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->constcollid);
    _fingerprintString(ctx, "constcollid");
    _fingerprintString(ctx, buffer);
  }


  if (node->constisnull) {    _fingerprintString(ctx, "constisnull");
    _fingerprintString(ctx, "true");
  }

  if (node->constlen != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->constlen);
    _fingerprintString(ctx, "constlen");
    _fingerprintString(ctx, buffer);
  }

  if (node->consttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->consttype);
    _fingerprintString(ctx, "consttype");
    _fingerprintString(ctx, buffer);
  }

  if (node->consttypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->consttypmod);
    _fingerprintString(ctx, "consttypmod");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintParam(FingerprintContext *ctx, const Param *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Param");
  // Intentionally ignoring node->location for fingerprinting
  if (node->paramcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->paramcollid);
    _fingerprintString(ctx, "paramcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->paramid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->paramid);
    _fingerprintString(ctx, "paramid");
    _fingerprintString(ctx, buffer);
  }

  if (node->paramkind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->paramkind);
    _fingerprintString(ctx, "paramkind");
    _fingerprintString(ctx, buffer);
  }

  if (node->paramtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->paramtype);
    _fingerprintString(ctx, "paramtype");
    _fingerprintString(ctx, buffer);
  }

  if (node->paramtypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->paramtypmod);
    _fingerprintString(ctx, "paramtypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintAggref(FingerprintContext *ctx, const Aggref *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Aggref");
  if (node->aggcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->aggcollid);
    _fingerprintString(ctx, "aggcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->aggdirectargs != NULL && node->aggdirectargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aggdirectargs, node, "aggdirectargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aggdirectargs");
  }
  if (node->aggdistinct != NULL && node->aggdistinct->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aggdistinct, node, "aggdistinct", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aggdistinct");
  }
  if (node->aggfilter != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aggfilter, node, "aggfilter", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aggfilter");
  }
  if (node->aggfnoid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->aggfnoid);
    _fingerprintString(ctx, "aggfnoid");
    _fingerprintString(ctx, buffer);
  }

  if (node->aggkind != 0) {
    char str[2] = {node->aggkind, '\0'};
    _fingerprintString(ctx, "aggkind");
    _fingerprintString(ctx, str);
  }

  if (node->agglevelsup != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->agglevelsup);
    _fingerprintString(ctx, "agglevelsup");
    _fingerprintString(ctx, buffer);
  }

  if (node->aggorder != NULL && node->aggorder->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aggorder, node, "aggorder", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aggorder");
  }

  if (node->aggstar) {    _fingerprintString(ctx, "aggstar");
    _fingerprintString(ctx, "true");
  }

  if (node->aggtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->aggtype);
    _fingerprintString(ctx, "aggtype");
    _fingerprintString(ctx, buffer);
  }


  if (node->aggvariadic) {    _fingerprintString(ctx, "aggvariadic");
    _fingerprintString(ctx, "true");
  }

  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintGroupingFunc(FingerprintContext *ctx, const GroupingFunc *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "GroupingFunc");
  if (node->agglevelsup != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->agglevelsup);
    _fingerprintString(ctx, "agglevelsup");
    _fingerprintString(ctx, buffer);
  }

  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->cols != NULL && node->cols->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cols, node, "cols", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cols");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->refs != NULL && node->refs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->refs, node, "refs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "refs");
  }
}

static void
_fingerprintWindowFunc(FingerprintContext *ctx, const WindowFunc *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "WindowFunc");
  if (node->aggfilter != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aggfilter, node, "aggfilter", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aggfilter");
  }
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting

  if (node->winagg) {    _fingerprintString(ctx, "winagg");
    _fingerprintString(ctx, "true");
  }

  if (node->wincollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->wincollid);
    _fingerprintString(ctx, "wincollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->winfnoid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->winfnoid);
    _fingerprintString(ctx, "winfnoid");
    _fingerprintString(ctx, buffer);
  }

  if (node->winref != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->winref);
    _fingerprintString(ctx, "winref");
    _fingerprintString(ctx, buffer);
  }


  if (node->winstar) {    _fingerprintString(ctx, "winstar");
    _fingerprintString(ctx, "true");
  }

  if (node->wintype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->wintype);
    _fingerprintString(ctx, "wintype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintArrayRef(FingerprintContext *ctx, const ArrayRef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ArrayRef");
  if (node->refarraytype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->refarraytype);
    _fingerprintString(ctx, "refarraytype");
    _fingerprintString(ctx, buffer);
  }

  if (node->refassgnexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->refassgnexpr, node, "refassgnexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "refassgnexpr");
  }
  if (node->refcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->refcollid);
    _fingerprintString(ctx, "refcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->refelemtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->refelemtype);
    _fingerprintString(ctx, "refelemtype");
    _fingerprintString(ctx, buffer);
  }

  if (node->refexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->refexpr, node, "refexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "refexpr");
  }
  if (node->reflowerindexpr != NULL && node->reflowerindexpr->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->reflowerindexpr, node, "reflowerindexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "reflowerindexpr");
  }
  if (node->reftypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->reftypmod);
    _fingerprintString(ctx, "reftypmod");
    _fingerprintString(ctx, buffer);
  }

  if (node->refupperindexpr != NULL && node->refupperindexpr->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->refupperindexpr, node, "refupperindexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "refupperindexpr");
  }
}

static void
_fingerprintFuncExpr(FingerprintContext *ctx, const FuncExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FuncExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->funccollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->funccollid);
    _fingerprintString(ctx, "funccollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->funcformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->funcformat);
    _fingerprintString(ctx, "funcformat");
    _fingerprintString(ctx, buffer);
  }

  if (node->funcid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->funcid);
    _fingerprintString(ctx, "funcid");
    _fingerprintString(ctx, buffer);
  }

  if (node->funcresulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->funcresulttype);
    _fingerprintString(ctx, "funcresulttype");
    _fingerprintString(ctx, buffer);
  }


  if (node->funcretset) {    _fingerprintString(ctx, "funcretset");
    _fingerprintString(ctx, "true");
  }


  if (node->funcvariadic) {    _fingerprintString(ctx, "funcvariadic");
    _fingerprintString(ctx, "true");
  }

  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintNamedArgExpr(FingerprintContext *ctx, const NamedArgExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "NamedArgExpr");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->argnumber != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->argnumber);
    _fingerprintString(ctx, "argnumber");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting

  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

}

static void
_fingerprintOpExpr(FingerprintContext *ctx, const OpExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "OpExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->opcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opcollid);
    _fingerprintString(ctx, "opcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->opfuncid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opfuncid);
    _fingerprintString(ctx, "opfuncid");
    _fingerprintString(ctx, buffer);
  }

  if (node->opno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opno);
    _fingerprintString(ctx, "opno");
    _fingerprintString(ctx, buffer);
  }

  if (node->opresulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opresulttype);
    _fingerprintString(ctx, "opresulttype");
    _fingerprintString(ctx, buffer);
  }


  if (node->opretset) {    _fingerprintString(ctx, "opretset");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintScalarArrayOpExpr(FingerprintContext *ctx, const ScalarArrayOpExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ScalarArrayOpExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->opfuncid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opfuncid);
    _fingerprintString(ctx, "opfuncid");
    _fingerprintString(ctx, buffer);
  }

  if (node->opno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->opno);
    _fingerprintString(ctx, "opno");
    _fingerprintString(ctx, buffer);
  }


  if (node->useOr) {    _fingerprintString(ctx, "useOr");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintBoolExpr(FingerprintContext *ctx, const BoolExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "BoolExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->boolop != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->boolop);
    _fingerprintString(ctx, "boolop");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintSubLink(FingerprintContext *ctx, const SubLink *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SubLink");
  // Intentionally ignoring node->location for fingerprinting
  if (node->operName != NULL && node->operName->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->operName, node, "operName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "operName");
  }
  if (node->subLinkId != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->subLinkId);
    _fingerprintString(ctx, "subLinkId");
    _fingerprintString(ctx, buffer);
  }

  if (node->subLinkType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->subLinkType);
    _fingerprintString(ctx, "subLinkType");
    _fingerprintString(ctx, buffer);
  }

  if (node->subselect != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->subselect, node, "subselect", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "subselect");
  }
  if (node->testexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->testexpr, node, "testexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "testexpr");
  }
}

static void
_fingerprintSubPlan(FingerprintContext *ctx, const SubPlan *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SubPlan");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->firstColCollation != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->firstColCollation);
    _fingerprintString(ctx, "firstColCollation");
    _fingerprintString(ctx, buffer);
  }

  if (node->firstColType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->firstColType);
    _fingerprintString(ctx, "firstColType");
    _fingerprintString(ctx, buffer);
  }

  if (node->firstColTypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->firstColTypmod);
    _fingerprintString(ctx, "firstColTypmod");
    _fingerprintString(ctx, buffer);
  }

  if (node->parParam != NULL && node->parParam->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->parParam, node, "parParam", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "parParam");
  }
  if (node->paramIds != NULL && node->paramIds->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->paramIds, node, "paramIds", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "paramIds");
  }
if (node->per_call_cost != 0) {
  char buffer[50];
  sprintf(buffer, "%f", node->per_call_cost);
  _fingerprintString(ctx, "per_call_cost");
  _fingerprintString(ctx, buffer);
}

  if (node->plan_id != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->plan_id);
    _fingerprintString(ctx, "plan_id");
    _fingerprintString(ctx, buffer);
  }


  if (node->plan_name != NULL) {
    _fingerprintString(ctx, "plan_name");
    _fingerprintString(ctx, node->plan_name);
  }

  if (node->setParam != NULL && node->setParam->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->setParam, node, "setParam", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "setParam");
  }
if (node->startup_cost != 0) {
  char buffer[50];
  sprintf(buffer, "%f", node->startup_cost);
  _fingerprintString(ctx, "startup_cost");
  _fingerprintString(ctx, buffer);
}

  if (node->subLinkType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->subLinkType);
    _fingerprintString(ctx, "subLinkType");
    _fingerprintString(ctx, buffer);
  }

  if (node->testexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->testexpr, node, "testexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "testexpr");
  }

  if (node->unknownEqFalse) {    _fingerprintString(ctx, "unknownEqFalse");
    _fingerprintString(ctx, "true");
  }


  if (node->useHashTable) {    _fingerprintString(ctx, "useHashTable");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintAlternativeSubPlan(FingerprintContext *ctx, const AlternativeSubPlan *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlternativeSubPlan");
  if (node->subplans != NULL && node->subplans->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->subplans, node, "subplans", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "subplans");
  }
}

static void
_fingerprintFieldSelect(FingerprintContext *ctx, const FieldSelect *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FieldSelect");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->fieldnum != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->fieldnum);
    _fingerprintString(ctx, "fieldnum");
    _fingerprintString(ctx, buffer);
  }

  if (node->resultcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultcollid);
    _fingerprintString(ctx, "resultcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttypmod);
    _fingerprintString(ctx, "resulttypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintFieldStore(FingerprintContext *ctx, const FieldStore *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FieldStore");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->fieldnums != NULL && node->fieldnums->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fieldnums, node, "fieldnums", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fieldnums");
  }
  if (node->newvals != NULL && node->newvals->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->newvals, node, "newvals", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "newvals");
  }
  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintRelabelType(FingerprintContext *ctx, const RelabelType *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RelabelType");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->relabelformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->relabelformat);
    _fingerprintString(ctx, "relabelformat");
    _fingerprintString(ctx, buffer);
  }

  if (node->resultcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultcollid);
    _fingerprintString(ctx, "resultcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttypmod);
    _fingerprintString(ctx, "resulttypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCoerceViaIO(FingerprintContext *ctx, const CoerceViaIO *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CoerceViaIO");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->coerceformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->coerceformat);
    _fingerprintString(ctx, "coerceformat");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->resultcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultcollid);
    _fingerprintString(ctx, "resultcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintArrayCoerceExpr(FingerprintContext *ctx, const ArrayCoerceExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ArrayCoerceExpr");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->coerceformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->coerceformat);
    _fingerprintString(ctx, "coerceformat");
    _fingerprintString(ctx, buffer);
  }

  if (node->elemfuncid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->elemfuncid);
    _fingerprintString(ctx, "elemfuncid");
    _fingerprintString(ctx, buffer);
  }


  if (node->isExplicit) {    _fingerprintString(ctx, "isExplicit");
    _fingerprintString(ctx, "true");
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->resultcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultcollid);
    _fingerprintString(ctx, "resultcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttypmod);
    _fingerprintString(ctx, "resulttypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintConvertRowtypeExpr(FingerprintContext *ctx, const ConvertRowtypeExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ConvertRowtypeExpr");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->convertformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->convertformat);
    _fingerprintString(ctx, "convertformat");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCollateExpr(FingerprintContext *ctx, const CollateExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CollateExpr");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->collOid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->collOid);
    _fingerprintString(ctx, "collOid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintCaseExpr(FingerprintContext *ctx, const CaseExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CaseExpr");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->casecollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->casecollid);
    _fingerprintString(ctx, "casecollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->casetype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->casetype);
    _fingerprintString(ctx, "casetype");
    _fingerprintString(ctx, buffer);
  }

  if (node->defresult != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->defresult, node, "defresult", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "defresult");
  }
  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintCaseWhen(FingerprintContext *ctx, const CaseWhen *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CaseWhen");
  if (node->expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->expr, node, "expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "expr");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->result != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->result, node, "result", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "result");
  }
}

static void
_fingerprintCaseTestExpr(FingerprintContext *ctx, const CaseTestExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CaseTestExpr");
  if (node->collation != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->collation);
    _fingerprintString(ctx, "collation");
    _fingerprintString(ctx, buffer);
  }

  if (node->typeId != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typeId);
    _fingerprintString(ctx, "typeId");
    _fingerprintString(ctx, buffer);
  }

  if (node->typeMod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typeMod);
    _fingerprintString(ctx, "typeMod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintArrayExpr(FingerprintContext *ctx, const ArrayExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ArrayExpr");
  if (node->array_collid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->array_collid);
    _fingerprintString(ctx, "array_collid");
    _fingerprintString(ctx, buffer);
  }

  if (node->array_typeid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->array_typeid);
    _fingerprintString(ctx, "array_typeid");
    _fingerprintString(ctx, buffer);
  }

  if (node->element_typeid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->element_typeid);
    _fingerprintString(ctx, "element_typeid");
    _fingerprintString(ctx, buffer);
  }

  if (node->elements != NULL && node->elements->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->elements, node, "elements", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "elements");
  }
  // Intentionally ignoring node->location for fingerprinting

  if (node->multidims) {    _fingerprintString(ctx, "multidims");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintRowExpr(FingerprintContext *ctx, const RowExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RowExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->colnames != NULL && node->colnames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->colnames, node, "colnames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "colnames");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->row_format != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->row_format);
    _fingerprintString(ctx, "row_format");
    _fingerprintString(ctx, buffer);
  }

  if (node->row_typeid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->row_typeid);
    _fingerprintString(ctx, "row_typeid");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintRowCompareExpr(FingerprintContext *ctx, const RowCompareExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RowCompareExpr");
  if (node->inputcollids != NULL && node->inputcollids->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->inputcollids, node, "inputcollids", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "inputcollids");
  }
  if (node->largs != NULL && node->largs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->largs, node, "largs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "largs");
  }
  if (node->opfamilies != NULL && node->opfamilies->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opfamilies, node, "opfamilies", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opfamilies");
  }
  if (node->opnos != NULL && node->opnos->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opnos, node, "opnos", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opnos");
  }
  if (node->rargs != NULL && node->rargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rargs, node, "rargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rargs");
  }
  if (node->rctype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->rctype);
    _fingerprintString(ctx, "rctype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCoalesceExpr(FingerprintContext *ctx, const CoalesceExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CoalesceExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->coalescecollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->coalescecollid);
    _fingerprintString(ctx, "coalescecollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->coalescetype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->coalescetype);
    _fingerprintString(ctx, "coalescetype");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintMinMaxExpr(FingerprintContext *ctx, const MinMaxExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "MinMaxExpr");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->inputcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inputcollid);
    _fingerprintString(ctx, "inputcollid");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->minmaxcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->minmaxcollid);
    _fingerprintString(ctx, "minmaxcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->minmaxtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->minmaxtype);
    _fingerprintString(ctx, "minmaxtype");
    _fingerprintString(ctx, buffer);
  }

  if (node->op != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->op);
    _fingerprintString(ctx, "op");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintXmlExpr(FingerprintContext *ctx, const XmlExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "XmlExpr");
  if (node->arg_names != NULL && node->arg_names->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg_names, node, "arg_names", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg_names");
  }
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  // Intentionally ignoring node->location for fingerprinting

  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->named_args != NULL && node->named_args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->named_args, node, "named_args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "named_args");
  }
  if (node->op != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->op);
    _fingerprintString(ctx, "op");
    _fingerprintString(ctx, buffer);
  }

  if (node->type != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->type);
    _fingerprintString(ctx, "type");
    _fingerprintString(ctx, buffer);
  }

  if (node->typmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typmod);
    _fingerprintString(ctx, "typmod");
    _fingerprintString(ctx, buffer);
  }

  if (node->xmloption != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->xmloption);
    _fingerprintString(ctx, "xmloption");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintNullTest(FingerprintContext *ctx, const NullTest *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "NullTest");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }

  if (node->argisrow) {    _fingerprintString(ctx, "argisrow");
    _fingerprintString(ctx, "true");
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->nulltesttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->nulltesttype);
    _fingerprintString(ctx, "nulltesttype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintBooleanTest(FingerprintContext *ctx, const BooleanTest *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "BooleanTest");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->booltesttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->booltesttype);
    _fingerprintString(ctx, "booltesttype");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintCoerceToDomain(FingerprintContext *ctx, const CoerceToDomain *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CoerceToDomain");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->coercionformat != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->coercionformat);
    _fingerprintString(ctx, "coercionformat");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->resultcollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultcollid);
    _fingerprintString(ctx, "resultcollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttype);
    _fingerprintString(ctx, "resulttype");
    _fingerprintString(ctx, buffer);
  }

  if (node->resulttypmod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resulttypmod);
    _fingerprintString(ctx, "resulttypmod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCoerceToDomainValue(FingerprintContext *ctx, const CoerceToDomainValue *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CoerceToDomainValue");
  if (node->collation != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->collation);
    _fingerprintString(ctx, "collation");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->typeId != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typeId);
    _fingerprintString(ctx, "typeId");
    _fingerprintString(ctx, buffer);
  }

  if (node->typeMod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typeMod);
    _fingerprintString(ctx, "typeMod");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintSetToDefault(FingerprintContext *ctx, const SetToDefault *node, const void *parent, const char *field_name, unsigned int depth)
{
  // Intentionally ignoring all fields for fingerprinting
}

static void
_fingerprintCurrentOfExpr(FingerprintContext *ctx, const CurrentOfExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CurrentOfExpr");

  if (node->cursor_name != NULL) {
    _fingerprintString(ctx, "cursor_name");
    _fingerprintString(ctx, node->cursor_name);
  }

  if (node->cursor_param != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->cursor_param);
    _fingerprintString(ctx, "cursor_param");
    _fingerprintString(ctx, buffer);
  }

  if (node->cvarno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->cvarno);
    _fingerprintString(ctx, "cvarno");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintInferenceElem(FingerprintContext *ctx, const InferenceElem *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "InferenceElem");
  if (node->expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->expr, node, "expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "expr");
  }
  if (node->infercollid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->infercollid);
    _fingerprintString(ctx, "infercollid");
    _fingerprintString(ctx, buffer);
  }

  if (node->inferopclass != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inferopclass);
    _fingerprintString(ctx, "inferopclass");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintTargetEntry(FingerprintContext *ctx, const TargetEntry *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TargetEntry");
  if (node->expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->expr, node, "expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "expr");
  }

  if (node->resjunk) {    _fingerprintString(ctx, "resjunk");
    _fingerprintString(ctx, "true");
  }


  if (node->resname != NULL) {
    _fingerprintString(ctx, "resname");
    _fingerprintString(ctx, node->resname);
  }

  if (node->resno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resno);
    _fingerprintString(ctx, "resno");
    _fingerprintString(ctx, buffer);
  }

  if (node->resorigcol != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resorigcol);
    _fingerprintString(ctx, "resorigcol");
    _fingerprintString(ctx, buffer);
  }

  if (node->resorigtbl != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resorigtbl);
    _fingerprintString(ctx, "resorigtbl");
    _fingerprintString(ctx, buffer);
  }

  if (node->ressortgroupref != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->ressortgroupref);
    _fingerprintString(ctx, "ressortgroupref");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintRangeTblRef(FingerprintContext *ctx, const RangeTblRef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeTblRef");
  if (node->rtindex != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->rtindex);
    _fingerprintString(ctx, "rtindex");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintJoinExpr(FingerprintContext *ctx, const JoinExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "JoinExpr");
  if (node->alias != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->alias, node, "alias", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "alias");
  }

  if (node->isNatural) {    _fingerprintString(ctx, "isNatural");
    _fingerprintString(ctx, "true");
  }

  if (node->jointype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->jointype);
    _fingerprintString(ctx, "jointype");
    _fingerprintString(ctx, buffer);
  }

  if (node->larg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->larg, node, "larg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "larg");
  }
  if (node->quals != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->quals, node, "quals", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "quals");
  }
  if (node->rarg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rarg, node, "rarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rarg");
  }
  if (node->rtindex != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->rtindex);
    _fingerprintString(ctx, "rtindex");
    _fingerprintString(ctx, buffer);
  }

  if (node->usingClause != NULL && node->usingClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->usingClause, node, "usingClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "usingClause");
  }
}

static void
_fingerprintFromExpr(FingerprintContext *ctx, const FromExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FromExpr");
  if (node->fromlist != NULL && node->fromlist->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fromlist, node, "fromlist", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fromlist");
  }
  if (node->quals != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->quals, node, "quals", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "quals");
  }
}

static void
_fingerprintOnConflictExpr(FingerprintContext *ctx, const OnConflictExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "OnConflictExpr");
  if (node->action != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->action);
    _fingerprintString(ctx, "action");
    _fingerprintString(ctx, buffer);
  }

  if (node->arbiterElems != NULL && node->arbiterElems->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arbiterElems, node, "arbiterElems", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arbiterElems");
  }
  if (node->arbiterWhere != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arbiterWhere, node, "arbiterWhere", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arbiterWhere");
  }
  if (node->constraint != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->constraint);
    _fingerprintString(ctx, "constraint");
    _fingerprintString(ctx, buffer);
  }

  if (node->exclRelIndex != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->exclRelIndex);
    _fingerprintString(ctx, "exclRelIndex");
    _fingerprintString(ctx, buffer);
  }

  if (node->exclRelTlist != NULL && node->exclRelTlist->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->exclRelTlist, node, "exclRelTlist", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "exclRelTlist");
  }
  if (node->onConflictSet != NULL && node->onConflictSet->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->onConflictSet, node, "onConflictSet", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "onConflictSet");
  }
  if (node->onConflictWhere != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->onConflictWhere, node, "onConflictWhere", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "onConflictWhere");
  }
}

static void
_fingerprintIntoClause(FingerprintContext *ctx, const IntoClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "IntoClause");
  if (node->colNames != NULL && node->colNames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->colNames, node, "colNames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "colNames");
  }
  if (node->onCommit != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->onCommit);
    _fingerprintString(ctx, "onCommit");
    _fingerprintString(ctx, buffer);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->rel != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rel, node, "rel", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rel");
  }

  if (node->skipData) {    _fingerprintString(ctx, "skipData");
    _fingerprintString(ctx, "true");
  }


  if (node->tableSpaceName != NULL) {
    _fingerprintString(ctx, "tableSpaceName");
    _fingerprintString(ctx, node->tableSpaceName);
  }

  if (node->viewQuery != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->viewQuery, node, "viewQuery", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "viewQuery");
  }
}

static void
_fingerprintQuery(FingerprintContext *ctx, const Query *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Query");

  if (node->canSetTag) {    _fingerprintString(ctx, "canSetTag");
    _fingerprintString(ctx, "true");
  }

  if (node->commandType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->commandType);
    _fingerprintString(ctx, "commandType");
    _fingerprintString(ctx, buffer);
  }

  if (node->constraintDeps != NULL && node->constraintDeps->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constraintDeps, node, "constraintDeps", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constraintDeps");
  }
  if (node->cteList != NULL && node->cteList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cteList, node, "cteList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cteList");
  }
  if (node->distinctClause != NULL && node->distinctClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->distinctClause, node, "distinctClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "distinctClause");
  }
  if (node->groupClause != NULL && node->groupClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->groupClause, node, "groupClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "groupClause");
  }
  if (node->groupingSets != NULL && node->groupingSets->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->groupingSets, node, "groupingSets", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "groupingSets");
  }

  if (node->hasAggs) {    _fingerprintString(ctx, "hasAggs");
    _fingerprintString(ctx, "true");
  }


  if (node->hasDistinctOn) {    _fingerprintString(ctx, "hasDistinctOn");
    _fingerprintString(ctx, "true");
  }


  if (node->hasForUpdate) {    _fingerprintString(ctx, "hasForUpdate");
    _fingerprintString(ctx, "true");
  }


  if (node->hasModifyingCTE) {    _fingerprintString(ctx, "hasModifyingCTE");
    _fingerprintString(ctx, "true");
  }


  if (node->hasRecursive) {    _fingerprintString(ctx, "hasRecursive");
    _fingerprintString(ctx, "true");
  }


  if (node->hasRowSecurity) {    _fingerprintString(ctx, "hasRowSecurity");
    _fingerprintString(ctx, "true");
  }


  if (node->hasSubLinks) {    _fingerprintString(ctx, "hasSubLinks");
    _fingerprintString(ctx, "true");
  }


  if (node->hasWindowFuncs) {    _fingerprintString(ctx, "hasWindowFuncs");
    _fingerprintString(ctx, "true");
  }

  if (node->havingQual != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->havingQual, node, "havingQual", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "havingQual");
  }
  if (node->jointree != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->jointree, node, "jointree", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "jointree");
  }
  if (node->limitCount != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->limitCount, node, "limitCount", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "limitCount");
  }
  if (node->limitOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->limitOffset, node, "limitOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "limitOffset");
  }
  if (node->onConflict != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->onConflict, node, "onConflict", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "onConflict");
  }
  if (node->queryId != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->queryId);
    _fingerprintString(ctx, "queryId");
    _fingerprintString(ctx, buffer);
  }

  if (node->querySource != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->querySource);
    _fingerprintString(ctx, "querySource");
    _fingerprintString(ctx, buffer);
  }

  if (node->resultRelation != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->resultRelation);
    _fingerprintString(ctx, "resultRelation");
    _fingerprintString(ctx, buffer);
  }

  if (node->returningList != NULL && node->returningList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->returningList, node, "returningList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "returningList");
  }
  if (node->rowMarks != NULL && node->rowMarks->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rowMarks, node, "rowMarks", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rowMarks");
  }
  if (node->rtable != NULL && node->rtable->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rtable, node, "rtable", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rtable");
  }
  if (node->setOperations != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->setOperations, node, "setOperations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "setOperations");
  }
  if (node->sortClause != NULL && node->sortClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->sortClause, node, "sortClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "sortClause");
  }
  if (node->targetList != NULL && node->targetList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->targetList, node, "targetList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "targetList");
  }
  if (node->utilityStmt != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->utilityStmt, node, "utilityStmt", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "utilityStmt");
  }
  if (node->windowClause != NULL && node->windowClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->windowClause, node, "windowClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "windowClause");
  }
  if (node->withCheckOptions != NULL && node->withCheckOptions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withCheckOptions, node, "withCheckOptions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withCheckOptions");
  }
}

static void
_fingerprintInsertStmt(FingerprintContext *ctx, const InsertStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "InsertStmt");
  if (node->cols != NULL && node->cols->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cols, node, "cols", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cols");
  }
  if (node->onConflictClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->onConflictClause, node, "onConflictClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "onConflictClause");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->returningList != NULL && node->returningList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->returningList, node, "returningList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "returningList");
  }
  if (node->selectStmt != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->selectStmt, node, "selectStmt", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "selectStmt");
  }
  if (node->withClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withClause, node, "withClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withClause");
  }
}

static void
_fingerprintDeleteStmt(FingerprintContext *ctx, const DeleteStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DeleteStmt");
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->returningList != NULL && node->returningList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->returningList, node, "returningList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "returningList");
  }
  if (node->usingClause != NULL && node->usingClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->usingClause, node, "usingClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "usingClause");
  }
  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
  if (node->withClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withClause, node, "withClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withClause");
  }
}

static void
_fingerprintUpdateStmt(FingerprintContext *ctx, const UpdateStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "UpdateStmt");
  if (node->fromClause != NULL && node->fromClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fromClause, node, "fromClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fromClause");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->returningList != NULL && node->returningList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->returningList, node, "returningList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "returningList");
  }
  if (node->targetList != NULL && node->targetList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->targetList, node, "targetList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "targetList");
  }
  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
  if (node->withClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withClause, node, "withClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withClause");
  }
}

static void
_fingerprintSelectStmt(FingerprintContext *ctx, const SelectStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SelectStmt");

  if (node->all) {    _fingerprintString(ctx, "all");
    _fingerprintString(ctx, "true");
  }

  if (node->distinctClause != NULL && node->distinctClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->distinctClause, node, "distinctClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "distinctClause");
  }
  if (node->fromClause != NULL && node->fromClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fromClause, node, "fromClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fromClause");
  }
  if (node->groupClause != NULL && node->groupClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->groupClause, node, "groupClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "groupClause");
  }
  if (node->havingClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->havingClause, node, "havingClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "havingClause");
  }
  if (node->intoClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->intoClause, node, "intoClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "intoClause");
  }
  if (node->larg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->larg, node, "larg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "larg");
  }
  if (node->limitCount != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->limitCount, node, "limitCount", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "limitCount");
  }
  if (node->limitOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->limitOffset, node, "limitOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "limitOffset");
  }
  if (node->lockingClause != NULL && node->lockingClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->lockingClause, node, "lockingClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "lockingClause");
  }
  if (node->op != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->op);
    _fingerprintString(ctx, "op");
    _fingerprintString(ctx, buffer);
  }

  if (node->rarg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rarg, node, "rarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rarg");
  }
  if (node->sortClause != NULL && node->sortClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->sortClause, node, "sortClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "sortClause");
  }
  if (node->targetList != NULL && node->targetList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->targetList, node, "targetList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "targetList");
  }
  if (node->valuesLists != NULL && node->valuesLists->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->valuesLists, node, "valuesLists", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "valuesLists");
  }
  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
  if (node->windowClause != NULL && node->windowClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->windowClause, node, "windowClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "windowClause");
  }
  if (node->withClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withClause, node, "withClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withClause");
  }
}

static void
_fingerprintAlterTableStmt(FingerprintContext *ctx, const AlterTableStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTableStmt");
  if (node->cmds != NULL && node->cmds->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cmds, node, "cmds", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cmds");
  }

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->relkind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->relkind);
    _fingerprintString(ctx, "relkind");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintAlterTableCmd(FingerprintContext *ctx, const AlterTableCmd *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTableCmd");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }

  if (node->def != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->def, node, "def", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "def");
  }

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->newowner != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->newowner, node, "newowner", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "newowner");
  }
  if (node->subtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->subtype);
    _fingerprintString(ctx, "subtype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintAlterDomainStmt(FingerprintContext *ctx, const AlterDomainStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterDomainStmt");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }

  if (node->def != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->def, node, "def", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "def");
  }

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->subtype != 0) {
    char str[2] = {node->subtype, '\0'};
    _fingerprintString(ctx, "subtype");
    _fingerprintString(ctx, str);
  }

  if (node->typeName != NULL && node->typeName->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintSetOperationStmt(FingerprintContext *ctx, const SetOperationStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SetOperationStmt");

  if (node->all) {    _fingerprintString(ctx, "all");
    _fingerprintString(ctx, "true");
  }

  if (node->colCollations != NULL && node->colCollations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->colCollations, node, "colCollations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "colCollations");
  }
  if (node->colTypes != NULL && node->colTypes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->colTypes, node, "colTypes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "colTypes");
  }
  if (node->colTypmods != NULL && node->colTypmods->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->colTypmods, node, "colTypmods", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "colTypmods");
  }
  if (node->groupClauses != NULL && node->groupClauses->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->groupClauses, node, "groupClauses", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "groupClauses");
  }
  if (node->larg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->larg, node, "larg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "larg");
  }
  if (node->op != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->op);
    _fingerprintString(ctx, "op");
    _fingerprintString(ctx, buffer);
  }

  if (node->rarg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rarg, node, "rarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rarg");
  }
}

static void
_fingerprintGrantStmt(FingerprintContext *ctx, const GrantStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "GrantStmt");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }


  if (node->grant_option) {    _fingerprintString(ctx, "grant_option");
    _fingerprintString(ctx, "true");
  }

  if (node->grantees != NULL && node->grantees->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->grantees, node, "grantees", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "grantees");
  }

  if (node->is_grant) {    _fingerprintString(ctx, "is_grant");
    _fingerprintString(ctx, "true");
  }

  if (node->objects != NULL && node->objects->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objects, node, "objects", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objects");
  }
  if (node->objtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objtype);
    _fingerprintString(ctx, "objtype");
    _fingerprintString(ctx, buffer);
  }

  if (node->privileges != NULL && node->privileges->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->privileges, node, "privileges", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "privileges");
  }
  if (node->targtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->targtype);
    _fingerprintString(ctx, "targtype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintGrantRoleStmt(FingerprintContext *ctx, const GrantRoleStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "GrantRoleStmt");

  if (node->admin_opt) {    _fingerprintString(ctx, "admin_opt");
    _fingerprintString(ctx, "true");
  }

  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }

  if (node->granted_roles != NULL && node->granted_roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->granted_roles, node, "granted_roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "granted_roles");
  }
  if (node->grantee_roles != NULL && node->grantee_roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->grantee_roles, node, "grantee_roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "grantee_roles");
  }
  if (node->grantor != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->grantor, node, "grantor", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "grantor");
  }

  if (node->is_grant) {    _fingerprintString(ctx, "is_grant");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintAlterDefaultPrivilegesStmt(FingerprintContext *ctx, const AlterDefaultPrivilegesStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterDefaultPrivilegesStmt");
  if (node->action != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->action, node, "action", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "action");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintClosePortalStmt(FingerprintContext *ctx, const ClosePortalStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ClosePortalStmt");

  if (node->portalname != NULL) {
    _fingerprintString(ctx, "portalname");
    _fingerprintString(ctx, node->portalname);
  }

}

static void
_fingerprintClusterStmt(FingerprintContext *ctx, const ClusterStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ClusterStmt");

  if (node->indexname != NULL) {
    _fingerprintString(ctx, "indexname");
    _fingerprintString(ctx, node->indexname);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }

  if (node->verbose) {    _fingerprintString(ctx, "verbose");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintCopyStmt(FingerprintContext *ctx, const CopyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CopyStmt");
  if (node->attlist != NULL && node->attlist->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->attlist, node, "attlist", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "attlist");
  }

  if (node->filename != NULL) {
    _fingerprintString(ctx, "filename");
    _fingerprintString(ctx, node->filename);
  }


  if (node->is_from) {    _fingerprintString(ctx, "is_from");
    _fingerprintString(ctx, "true");
  }


  if (node->is_program) {    _fingerprintString(ctx, "is_program");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
}

static void
_fingerprintCreateStmt(FingerprintContext *ctx, const CreateStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateStmt");
  if (node->constraints != NULL && node->constraints->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constraints, node, "constraints", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constraints");
  }

  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->inhRelations != NULL && node->inhRelations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->inhRelations, node, "inhRelations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "inhRelations");
  }
  if (node->ofTypename != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ofTypename, node, "ofTypename", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ofTypename");
  }
  if (node->oncommit != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->oncommit);
    _fingerprintString(ctx, "oncommit");
    _fingerprintString(ctx, buffer);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->tableElts != NULL && node->tableElts->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->tableElts, node, "tableElts", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "tableElts");
  }

  if (node->tablespacename != NULL) {
    _fingerprintString(ctx, "tablespacename");
    _fingerprintString(ctx, node->tablespacename);
  }

}

static void
_fingerprintDefineStmt(FingerprintContext *ctx, const DefineStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DefineStmt");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->definition != NULL && node->definition->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->definition, node, "definition", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "definition");
  }
  if (node->defnames != NULL && node->defnames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->defnames, node, "defnames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "defnames");
  }
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }


  if (node->oldstyle) {    _fingerprintString(ctx, "oldstyle");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintDropStmt(FingerprintContext *ctx, const DropStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropStmt");
  if (node->arguments != NULL && node->arguments->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arguments, node, "arguments", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arguments");
  }
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }


  if (node->concurrent) {    _fingerprintString(ctx, "concurrent");
    _fingerprintString(ctx, "true");
  }


  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }

  if (node->objects != NULL && node->objects->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objects, node, "objects", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objects");
  }
  if (node->removeType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->removeType);
    _fingerprintString(ctx, "removeType");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintTruncateStmt(FingerprintContext *ctx, const TruncateStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TruncateStmt");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }

  if (node->relations != NULL && node->relations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relations, node, "relations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relations");
  }

  if (node->restart_seqs) {    _fingerprintString(ctx, "restart_seqs");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintCommentStmt(FingerprintContext *ctx, const CommentStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CommentStmt");

  if (node->comment != NULL) {
    _fingerprintString(ctx, "comment");
    _fingerprintString(ctx, node->comment);
  }

  if (node->objargs != NULL && node->objargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objargs, node, "objargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objargs");
  }
  if (node->objname != NULL && node->objname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objname, node, "objname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objname");
  }
  if (node->objtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objtype);
    _fingerprintString(ctx, "objtype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintFetchStmt(FingerprintContext *ctx, const FetchStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FetchStmt");
  if (node->direction != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->direction);
    _fingerprintString(ctx, "direction");
    _fingerprintString(ctx, buffer);
  }

  if (node->howMany != 0) {
    char buffer[50];
    sprintf(buffer, "%ld", node->howMany);
    _fingerprintString(ctx, "howMany");
    _fingerprintString(ctx, buffer);
  }


  if (node->ismove) {    _fingerprintString(ctx, "ismove");
    _fingerprintString(ctx, "true");
  }


  if (node->portalname != NULL) {
    _fingerprintString(ctx, "portalname");
    _fingerprintString(ctx, node->portalname);
  }

}

static void
_fingerprintIndexStmt(FingerprintContext *ctx, const IndexStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "IndexStmt");

  if (node->accessMethod != NULL) {
    _fingerprintString(ctx, "accessMethod");
    _fingerprintString(ctx, node->accessMethod);
  }


  if (node->concurrent) {    _fingerprintString(ctx, "concurrent");
    _fingerprintString(ctx, "true");
  }


  if (node->deferrable) {    _fingerprintString(ctx, "deferrable");
    _fingerprintString(ctx, "true");
  }

  if (node->excludeOpNames != NULL && node->excludeOpNames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->excludeOpNames, node, "excludeOpNames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "excludeOpNames");
  }

  if (node->idxcomment != NULL) {
    _fingerprintString(ctx, "idxcomment");
    _fingerprintString(ctx, node->idxcomment);
  }


  if (node->idxname != NULL) {
    _fingerprintString(ctx, "idxname");
    _fingerprintString(ctx, node->idxname);
  }


  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->indexOid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->indexOid);
    _fingerprintString(ctx, "indexOid");
    _fingerprintString(ctx, buffer);
  }

  if (node->indexParams != NULL && node->indexParams->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->indexParams, node, "indexParams", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "indexParams");
  }

  if (node->initdeferred) {    _fingerprintString(ctx, "initdeferred");
    _fingerprintString(ctx, "true");
  }


  if (node->isconstraint) {    _fingerprintString(ctx, "isconstraint");
    _fingerprintString(ctx, "true");
  }

  if (node->oldNode != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->oldNode);
    _fingerprintString(ctx, "oldNode");
    _fingerprintString(ctx, buffer);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->primary) {    _fingerprintString(ctx, "primary");
    _fingerprintString(ctx, "true");
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }

  if (node->tableSpace != NULL) {
    _fingerprintString(ctx, "tableSpace");
    _fingerprintString(ctx, node->tableSpace);
  }


  if (node->transformed) {    _fingerprintString(ctx, "transformed");
    _fingerprintString(ctx, "true");
  }


  if (node->unique) {    _fingerprintString(ctx, "unique");
    _fingerprintString(ctx, "true");
  }

  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
}

static void
_fingerprintCreateFunctionStmt(FingerprintContext *ctx, const CreateFunctionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateFunctionStmt");
  if (node->funcname != NULL && node->funcname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcname, node, "funcname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcname");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->parameters != NULL && node->parameters->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->parameters, node, "parameters", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "parameters");
  }

  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }

  if (node->returnType != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->returnType, node, "returnType", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "returnType");
  }
  if (node->withClause != NULL && node->withClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->withClause, node, "withClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "withClause");
  }
}

static void
_fingerprintAlterFunctionStmt(FingerprintContext *ctx, const AlterFunctionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterFunctionStmt");
  if (node->actions != NULL && node->actions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->actions, node, "actions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "actions");
  }
  if (node->func != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->func, node, "func", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "func");
  }
}

static void
_fingerprintDoStmt(FingerprintContext *ctx, const DoStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DoStmt");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
}

static void
_fingerprintRenameStmt(FingerprintContext *ctx, const RenameStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RenameStmt");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }


  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->newname != NULL) {
    _fingerprintString(ctx, "newname");
    _fingerprintString(ctx, node->newname);
  }

  if (node->objarg != NULL && node->objarg->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objarg, node, "objarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objarg");
  }
  if (node->object != NULL && node->object->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->object, node, "object", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "object");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->relationType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->relationType);
    _fingerprintString(ctx, "relationType");
    _fingerprintString(ctx, buffer);
  }

  if (node->renameType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->renameType);
    _fingerprintString(ctx, "renameType");
    _fingerprintString(ctx, buffer);
  }


  if (node->subname != NULL) {
    _fingerprintString(ctx, "subname");
    _fingerprintString(ctx, node->subname);
  }

}

static void
_fingerprintRuleStmt(FingerprintContext *ctx, const RuleStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RuleStmt");
  if (node->actions != NULL && node->actions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->actions, node, "actions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "actions");
  }
  if (node->event != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->event);
    _fingerprintString(ctx, "event");
    _fingerprintString(ctx, buffer);
  }


  if (node->instead) {    _fingerprintString(ctx, "instead");
    _fingerprintString(ctx, "true");
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }

  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }


  if (node->rulename != NULL) {
    _fingerprintString(ctx, "rulename");
    _fingerprintString(ctx, node->rulename);
  }

  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
}

static void
_fingerprintNotifyStmt(FingerprintContext *ctx, const NotifyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "NotifyStmt");

  if (node->conditionname != NULL) {
    _fingerprintString(ctx, "conditionname");
    _fingerprintString(ctx, node->conditionname);
  }


  if (node->payload != NULL) {
    _fingerprintString(ctx, "payload");
    _fingerprintString(ctx, node->payload);
  }

}

static void
_fingerprintListenStmt(FingerprintContext *ctx, const ListenStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ListenStmt");

  if (node->conditionname != NULL) {
    _fingerprintString(ctx, "conditionname");
    _fingerprintString(ctx, node->conditionname);
  }

}

static void
_fingerprintUnlistenStmt(FingerprintContext *ctx, const UnlistenStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "UnlistenStmt");

  if (node->conditionname != NULL) {
    _fingerprintString(ctx, "conditionname");
    _fingerprintString(ctx, node->conditionname);
  }

}

static void
_fingerprintTransactionStmt(FingerprintContext *ctx, const TransactionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TransactionStmt");
  // Intentionally ignoring node->gid for fingerprinting
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->options for fingerprinting
}

static void
_fingerprintViewStmt(FingerprintContext *ctx, const ViewStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ViewStmt");
  if (node->aliases != NULL && node->aliases->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aliases, node, "aliases", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aliases");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }

  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }

  if (node->view != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->view, node, "view", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "view");
  }
  if (node->withCheckOption != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->withCheckOption);
    _fingerprintString(ctx, "withCheckOption");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintLoadStmt(FingerprintContext *ctx, const LoadStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "LoadStmt");

  if (node->filename != NULL) {
    _fingerprintString(ctx, "filename");
    _fingerprintString(ctx, node->filename);
  }

}

static void
_fingerprintCreateDomainStmt(FingerprintContext *ctx, const CreateDomainStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateDomainStmt");
  if (node->collClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->collClause, node, "collClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "collClause");
  }
  if (node->constraints != NULL && node->constraints->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constraints, node, "constraints", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constraints");
  }
  if (node->domainname != NULL && node->domainname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->domainname, node, "domainname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "domainname");
  }
  if (node->typeName != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintCreatedbStmt(FingerprintContext *ctx, const CreatedbStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreatedbStmt");

  if (node->dbname != NULL) {
    _fingerprintString(ctx, "dbname");
    _fingerprintString(ctx, node->dbname);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintDropdbStmt(FingerprintContext *ctx, const DropdbStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropdbStmt");

  if (node->dbname != NULL) {
    _fingerprintString(ctx, "dbname");
    _fingerprintString(ctx, node->dbname);
  }


  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintVacuumStmt(FingerprintContext *ctx, const VacuumStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "VacuumStmt");
  if (node->options != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->options);
    _fingerprintString(ctx, "options");
    _fingerprintString(ctx, buffer);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->va_cols != NULL && node->va_cols->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->va_cols, node, "va_cols", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "va_cols");
  }
}

static void
_fingerprintExplainStmt(FingerprintContext *ctx, const ExplainStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ExplainStmt");
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }
}

static void
_fingerprintCreateTableAsStmt(FingerprintContext *ctx, const CreateTableAsStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateTableAsStmt");

  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->into != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->into, node, "into", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "into");
  }

  if (node->is_select_into) {    _fingerprintString(ctx, "is_select_into");
    _fingerprintString(ctx, "true");
  }

  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }
  if (node->relkind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->relkind);
    _fingerprintString(ctx, "relkind");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCreateSeqStmt(FingerprintContext *ctx, const CreateSeqStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateSeqStmt");

  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->ownerId != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->ownerId);
    _fingerprintString(ctx, "ownerId");
    _fingerprintString(ctx, buffer);
  }

  if (node->sequence != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->sequence, node, "sequence", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "sequence");
  }
}

static void
_fingerprintAlterSeqStmt(FingerprintContext *ctx, const AlterSeqStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterSeqStmt");

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->sequence != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->sequence, node, "sequence", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "sequence");
  }
}

static void
_fingerprintVariableSetStmt(FingerprintContext *ctx, const VariableSetStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "VariableSetStmt");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }

  if (node->is_local) {    _fingerprintString(ctx, "is_local");
    _fingerprintString(ctx, "true");
  }

  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

}

static void
_fingerprintVariableShowStmt(FingerprintContext *ctx, const VariableShowStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "VariableShowStmt");

  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

}

static void
_fingerprintDiscardStmt(FingerprintContext *ctx, const DiscardStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DiscardStmt");
  if (node->target != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->target);
    _fingerprintString(ctx, "target");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCreateTrigStmt(FingerprintContext *ctx, const CreateTrigStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateTrigStmt");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->columns != NULL && node->columns->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->columns, node, "columns", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "columns");
  }
  if (node->constrrel != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constrrel, node, "constrrel", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constrrel");
  }

  if (node->deferrable) {    _fingerprintString(ctx, "deferrable");
    _fingerprintString(ctx, "true");
  }

  if (node->events != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->events);
    _fingerprintString(ctx, "events");
    _fingerprintString(ctx, buffer);
  }

  if (node->funcname != NULL && node->funcname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcname, node, "funcname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcname");
  }

  if (node->initdeferred) {    _fingerprintString(ctx, "initdeferred");
    _fingerprintString(ctx, "true");
  }


  if (node->isconstraint) {    _fingerprintString(ctx, "isconstraint");
    _fingerprintString(ctx, "true");
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }

  if (node->row) {    _fingerprintString(ctx, "row");
    _fingerprintString(ctx, "true");
  }

  if (node->timing != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->timing);
    _fingerprintString(ctx, "timing");
    _fingerprintString(ctx, buffer);
  }


  if (node->trigname != NULL) {
    _fingerprintString(ctx, "trigname");
    _fingerprintString(ctx, node->trigname);
  }

  if (node->whenClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whenClause, node, "whenClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whenClause");
  }
}

static void
_fingerprintCreatePLangStmt(FingerprintContext *ctx, const CreatePLangStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreatePLangStmt");
  if (node->plhandler != NULL && node->plhandler->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->plhandler, node, "plhandler", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "plhandler");
  }
  if (node->plinline != NULL && node->plinline->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->plinline, node, "plinline", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "plinline");
  }

  if (node->plname != NULL) {
    _fingerprintString(ctx, "plname");
    _fingerprintString(ctx, node->plname);
  }


  if (node->pltrusted) {    _fingerprintString(ctx, "pltrusted");
    _fingerprintString(ctx, "true");
  }

  if (node->plvalidator != NULL && node->plvalidator->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->plvalidator, node, "plvalidator", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "plvalidator");
  }

  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintCreateRoleStmt(FingerprintContext *ctx, const CreateRoleStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateRoleStmt");
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->role != NULL) {
    _fingerprintString(ctx, "role");
    _fingerprintString(ctx, node->role);
  }

  if (node->stmt_type != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->stmt_type);
    _fingerprintString(ctx, "stmt_type");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintAlterRoleStmt(FingerprintContext *ctx, const AlterRoleStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterRoleStmt");
  if (node->action != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->action);
    _fingerprintString(ctx, "action");
    _fingerprintString(ctx, buffer);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->role != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->role, node, "role", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "role");
  }
}

static void
_fingerprintDropRoleStmt(FingerprintContext *ctx, const DropRoleStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropRoleStmt");

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }

  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
}

static void
_fingerprintLockStmt(FingerprintContext *ctx, const LockStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "LockStmt");
  if (node->mode != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->mode);
    _fingerprintString(ctx, "mode");
    _fingerprintString(ctx, buffer);
  }


  if (node->nowait) {    _fingerprintString(ctx, "nowait");
    _fingerprintString(ctx, "true");
  }

  if (node->relations != NULL && node->relations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relations, node, "relations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relations");
  }
}

static void
_fingerprintConstraintsSetStmt(FingerprintContext *ctx, const ConstraintsSetStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ConstraintsSetStmt");
  if (node->constraints != NULL && node->constraints->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constraints, node, "constraints", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constraints");
  }

  if (node->deferred) {    _fingerprintString(ctx, "deferred");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintReindexStmt(FingerprintContext *ctx, const ReindexStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ReindexStmt");
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->options != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->options);
    _fingerprintString(ctx, "options");
    _fingerprintString(ctx, buffer);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
}

static void
_fingerprintCheckPointStmt(FingerprintContext *ctx, const CheckPointStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CheckPointStmt");
}

static void
_fingerprintCreateSchemaStmt(FingerprintContext *ctx, const CreateSchemaStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateSchemaStmt");
  if (node->authrole != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->authrole, node, "authrole", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "authrole");
  }

  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->schemaElts != NULL && node->schemaElts->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->schemaElts, node, "schemaElts", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "schemaElts");
  }

  if (node->schemaname != NULL) {
    _fingerprintString(ctx, "schemaname");
    _fingerprintString(ctx, node->schemaname);
  }

}

static void
_fingerprintAlterDatabaseStmt(FingerprintContext *ctx, const AlterDatabaseStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterDatabaseStmt");

  if (node->dbname != NULL) {
    _fingerprintString(ctx, "dbname");
    _fingerprintString(ctx, node->dbname);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintAlterDatabaseSetStmt(FingerprintContext *ctx, const AlterDatabaseSetStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterDatabaseSetStmt");

  if (node->dbname != NULL) {
    _fingerprintString(ctx, "dbname");
    _fingerprintString(ctx, node->dbname);
  }

  if (node->setstmt != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->setstmt, node, "setstmt", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "setstmt");
  }
}

static void
_fingerprintAlterRoleSetStmt(FingerprintContext *ctx, const AlterRoleSetStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterRoleSetStmt");

  if (node->database != NULL) {
    _fingerprintString(ctx, "database");
    _fingerprintString(ctx, node->database);
  }

  if (node->role != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->role, node, "role", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "role");
  }
  if (node->setstmt != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->setstmt, node, "setstmt", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "setstmt");
  }
}

static void
_fingerprintCreateConversionStmt(FingerprintContext *ctx, const CreateConversionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateConversionStmt");
  if (node->conversion_name != NULL && node->conversion_name->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->conversion_name, node, "conversion_name", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "conversion_name");
  }

  if (node->def) {    _fingerprintString(ctx, "def");
    _fingerprintString(ctx, "true");
  }


  if (node->for_encoding_name != NULL) {
    _fingerprintString(ctx, "for_encoding_name");
    _fingerprintString(ctx, node->for_encoding_name);
  }

  if (node->func_name != NULL && node->func_name->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->func_name, node, "func_name", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "func_name");
  }

  if (node->to_encoding_name != NULL) {
    _fingerprintString(ctx, "to_encoding_name");
    _fingerprintString(ctx, node->to_encoding_name);
  }

}

static void
_fingerprintCreateCastStmt(FingerprintContext *ctx, const CreateCastStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateCastStmt");
  if (node->context != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->context);
    _fingerprintString(ctx, "context");
    _fingerprintString(ctx, buffer);
  }

  if (node->func != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->func, node, "func", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "func");
  }

  if (node->inout) {    _fingerprintString(ctx, "inout");
    _fingerprintString(ctx, "true");
  }

  if (node->sourcetype != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->sourcetype, node, "sourcetype", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "sourcetype");
  }
  if (node->targettype != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->targettype, node, "targettype", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "targettype");
  }
}

static void
_fingerprintCreateOpClassStmt(FingerprintContext *ctx, const CreateOpClassStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateOpClassStmt");

  if (node->amname != NULL) {
    _fingerprintString(ctx, "amname");
    _fingerprintString(ctx, node->amname);
  }

  if (node->datatype != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->datatype, node, "datatype", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "datatype");
  }

  if (node->isDefault) {    _fingerprintString(ctx, "isDefault");
    _fingerprintString(ctx, "true");
  }

  if (node->items != NULL && node->items->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->items, node, "items", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "items");
  }
  if (node->opclassname != NULL && node->opclassname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opclassname, node, "opclassname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opclassname");
  }
  if (node->opfamilyname != NULL && node->opfamilyname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opfamilyname, node, "opfamilyname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opfamilyname");
  }
}

static void
_fingerprintCreateOpFamilyStmt(FingerprintContext *ctx, const CreateOpFamilyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateOpFamilyStmt");

  if (node->amname != NULL) {
    _fingerprintString(ctx, "amname");
    _fingerprintString(ctx, node->amname);
  }

  if (node->opfamilyname != NULL && node->opfamilyname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opfamilyname, node, "opfamilyname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opfamilyname");
  }
}

static void
_fingerprintAlterOpFamilyStmt(FingerprintContext *ctx, const AlterOpFamilyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterOpFamilyStmt");

  if (node->amname != NULL) {
    _fingerprintString(ctx, "amname");
    _fingerprintString(ctx, node->amname);
  }


  if (node->isDrop) {    _fingerprintString(ctx, "isDrop");
    _fingerprintString(ctx, "true");
  }

  if (node->items != NULL && node->items->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->items, node, "items", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "items");
  }
  if (node->opfamilyname != NULL && node->opfamilyname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opfamilyname, node, "opfamilyname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opfamilyname");
  }
}

static void
_fingerprintPrepareStmt(FingerprintContext *ctx, const PrepareStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "PrepareStmt");
  if (node->argtypes != NULL && node->argtypes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->argtypes, node, "argtypes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "argtypes");
  }
  // Intentionally ignoring node->name for fingerprinting
  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }
}

static void
_fingerprintExecuteStmt(FingerprintContext *ctx, const ExecuteStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ExecuteStmt");
  // Intentionally ignoring node->name for fingerprinting
  if (node->params != NULL && node->params->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->params, node, "params", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "params");
  }
}

static void
_fingerprintDeallocateStmt(FingerprintContext *ctx, const DeallocateStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DeallocateStmt");
  // Intentionally ignoring node->name for fingerprinting
}

static void
_fingerprintDeclareCursorStmt(FingerprintContext *ctx, const DeclareCursorStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DeclareCursorStmt");
  if (node->options != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->options);
    _fingerprintString(ctx, "options");
    _fingerprintString(ctx, buffer);
  }


  if (node->portalname != NULL) {
    _fingerprintString(ctx, "portalname");
    _fingerprintString(ctx, node->portalname);
  }

  if (node->query != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->query, node, "query", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "query");
  }
}

static void
_fingerprintCreateTableSpaceStmt(FingerprintContext *ctx, const CreateTableSpaceStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateTableSpaceStmt");
  // Intentionally ignoring node->location for fingerprinting
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->owner != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->owner, node, "owner", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "owner");
  }

  if (node->tablespacename != NULL) {
    _fingerprintString(ctx, "tablespacename");
    _fingerprintString(ctx, node->tablespacename);
  }

}

static void
_fingerprintDropTableSpaceStmt(FingerprintContext *ctx, const DropTableSpaceStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropTableSpaceStmt");

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->tablespacename != NULL) {
    _fingerprintString(ctx, "tablespacename");
    _fingerprintString(ctx, node->tablespacename);
  }

}

static void
_fingerprintAlterObjectSchemaStmt(FingerprintContext *ctx, const AlterObjectSchemaStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterObjectSchemaStmt");

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->newschema != NULL) {
    _fingerprintString(ctx, "newschema");
    _fingerprintString(ctx, node->newschema);
  }

  if (node->objarg != NULL && node->objarg->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objarg, node, "objarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objarg");
  }
  if (node->object != NULL && node->object->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->object, node, "object", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "object");
  }
  if (node->objectType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objectType);
    _fingerprintString(ctx, "objectType");
    _fingerprintString(ctx, buffer);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
}

static void
_fingerprintAlterOwnerStmt(FingerprintContext *ctx, const AlterOwnerStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterOwnerStmt");
  if (node->newowner != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->newowner, node, "newowner", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "newowner");
  }
  if (node->objarg != NULL && node->objarg->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objarg, node, "objarg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objarg");
  }
  if (node->object != NULL && node->object->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->object, node, "object", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "object");
  }
  if (node->objectType != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objectType);
    _fingerprintString(ctx, "objectType");
    _fingerprintString(ctx, buffer);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
}

static void
_fingerprintDropOwnedStmt(FingerprintContext *ctx, const DropOwnedStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropOwnedStmt");
  if (node->behavior != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->behavior);
    _fingerprintString(ctx, "behavior");
    _fingerprintString(ctx, buffer);
  }

  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
}

static void
_fingerprintReassignOwnedStmt(FingerprintContext *ctx, const ReassignOwnedStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ReassignOwnedStmt");
  if (node->newrole != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->newrole, node, "newrole", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "newrole");
  }
  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
}

static void
_fingerprintCompositeTypeStmt(FingerprintContext *ctx, const CompositeTypeStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CompositeTypeStmt");
  if (node->coldeflist != NULL && node->coldeflist->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->coldeflist, node, "coldeflist", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "coldeflist");
  }
  if (node->typevar != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typevar, node, "typevar", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typevar");
  }
}

static void
_fingerprintCreateEnumStmt(FingerprintContext *ctx, const CreateEnumStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateEnumStmt");
  if (node->typeName != NULL && node->typeName->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
  if (node->vals != NULL && node->vals->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->vals, node, "vals", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "vals");
  }
}

static void
_fingerprintCreateRangeStmt(FingerprintContext *ctx, const CreateRangeStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateRangeStmt");
  if (node->params != NULL && node->params->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->params, node, "params", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "params");
  }
  if (node->typeName != NULL && node->typeName->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintAlterEnumStmt(FingerprintContext *ctx, const AlterEnumStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterEnumStmt");

  if (node->newVal != NULL) {
    _fingerprintString(ctx, "newVal");
    _fingerprintString(ctx, node->newVal);
  }


  if (node->newValIsAfter) {    _fingerprintString(ctx, "newValIsAfter");
    _fingerprintString(ctx, "true");
  }


  if (node->newValNeighbor != NULL) {
    _fingerprintString(ctx, "newValNeighbor");
    _fingerprintString(ctx, node->newValNeighbor);
  }


  if (node->skipIfExists) {    _fingerprintString(ctx, "skipIfExists");
    _fingerprintString(ctx, "true");
  }

  if (node->typeName != NULL && node->typeName->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintAlterTSDictionaryStmt(FingerprintContext *ctx, const AlterTSDictionaryStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTSDictionaryStmt");
  if (node->dictname != NULL && node->dictname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->dictname, node, "dictname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "dictname");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintAlterTSConfigurationStmt(FingerprintContext *ctx, const AlterTSConfigurationStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTSConfigurationStmt");
  if (node->cfgname != NULL && node->cfgname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cfgname, node, "cfgname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cfgname");
  }
  if (node->dicts != NULL && node->dicts->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->dicts, node, "dicts", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "dicts");
  }
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }


  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->override) {    _fingerprintString(ctx, "override");
    _fingerprintString(ctx, "true");
  }


  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }

  if (node->tokentype != NULL && node->tokentype->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->tokentype, node, "tokentype", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "tokentype");
  }
}

static void
_fingerprintCreateFdwStmt(FingerprintContext *ctx, const CreateFdwStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateFdwStmt");

  if (node->fdwname != NULL) {
    _fingerprintString(ctx, "fdwname");
    _fingerprintString(ctx, node->fdwname);
  }

  if (node->func_options != NULL && node->func_options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->func_options, node, "func_options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "func_options");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintAlterFdwStmt(FingerprintContext *ctx, const AlterFdwStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterFdwStmt");

  if (node->fdwname != NULL) {
    _fingerprintString(ctx, "fdwname");
    _fingerprintString(ctx, node->fdwname);
  }

  if (node->func_options != NULL && node->func_options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->func_options, node, "func_options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "func_options");
  }
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintCreateForeignServerStmt(FingerprintContext *ctx, const CreateForeignServerStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateForeignServerStmt");

  if (node->fdwname != NULL) {
    _fingerprintString(ctx, "fdwname");
    _fingerprintString(ctx, node->fdwname);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }


  if (node->servertype != NULL) {
    _fingerprintString(ctx, "servertype");
    _fingerprintString(ctx, node->servertype);
  }


  if (node->version != NULL) {
    _fingerprintString(ctx, "version");
    _fingerprintString(ctx, node->version);
  }

}

static void
_fingerprintAlterForeignServerStmt(FingerprintContext *ctx, const AlterForeignServerStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterForeignServerStmt");

  if (node->has_version) {    _fingerprintString(ctx, "has_version");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }


  if (node->version != NULL) {
    _fingerprintString(ctx, "version");
    _fingerprintString(ctx, node->version);
  }

}

static void
_fingerprintCreateUserMappingStmt(FingerprintContext *ctx, const CreateUserMappingStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateUserMappingStmt");
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }

  if (node->user != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->user, node, "user", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "user");
  }
}

static void
_fingerprintAlterUserMappingStmt(FingerprintContext *ctx, const AlterUserMappingStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterUserMappingStmt");
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }

  if (node->user != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->user, node, "user", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "user");
  }
}

static void
_fingerprintDropUserMappingStmt(FingerprintContext *ctx, const DropUserMappingStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DropUserMappingStmt");

  if (node->missing_ok) {    _fingerprintString(ctx, "missing_ok");
    _fingerprintString(ctx, "true");
  }


  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }

  if (node->user != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->user, node, "user", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "user");
  }
}

static void
_fingerprintAlterTableSpaceOptionsStmt(FingerprintContext *ctx, const AlterTableSpaceOptionsStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTableSpaceOptionsStmt");

  if (node->isReset) {    _fingerprintString(ctx, "isReset");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->tablespacename != NULL) {
    _fingerprintString(ctx, "tablespacename");
    _fingerprintString(ctx, node->tablespacename);
  }

}

static void
_fingerprintAlterTableMoveAllStmt(FingerprintContext *ctx, const AlterTableMoveAllStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterTableMoveAllStmt");

  if (node->new_tablespacename != NULL) {
    _fingerprintString(ctx, "new_tablespacename");
    _fingerprintString(ctx, node->new_tablespacename);
  }


  if (node->nowait) {    _fingerprintString(ctx, "nowait");
    _fingerprintString(ctx, "true");
  }

  if (node->objtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objtype);
    _fingerprintString(ctx, "objtype");
    _fingerprintString(ctx, buffer);
  }


  if (node->orig_tablespacename != NULL) {
    _fingerprintString(ctx, "orig_tablespacename");
    _fingerprintString(ctx, node->orig_tablespacename);
  }

  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
}

static void
_fingerprintSecLabelStmt(FingerprintContext *ctx, const SecLabelStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SecLabelStmt");

  if (node->label != NULL) {
    _fingerprintString(ctx, "label");
    _fingerprintString(ctx, node->label);
  }

  if (node->objargs != NULL && node->objargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objargs, node, "objargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objargs");
  }
  if (node->objname != NULL && node->objname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objname, node, "objname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objname");
  }
  if (node->objtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objtype);
    _fingerprintString(ctx, "objtype");
    _fingerprintString(ctx, buffer);
  }


  if (node->provider != NULL) {
    _fingerprintString(ctx, "provider");
    _fingerprintString(ctx, node->provider);
  }

}

static void
_fingerprintCreateForeignTableStmt(FingerprintContext *ctx, const CreateForeignTableStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateForeignTableStmt");
  _fingerprintString(ctx, "base");
  _fingerprintCreateStmt(ctx, (const CreateStmt*) &node->base, node, "base", depth);
  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->servername != NULL) {
    _fingerprintString(ctx, "servername");
    _fingerprintString(ctx, node->servername);
  }

}

static void
_fingerprintImportForeignSchemaStmt(FingerprintContext *ctx, const ImportForeignSchemaStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ImportForeignSchemaStmt");
  if (node->list_type != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->list_type);
    _fingerprintString(ctx, "list_type");
    _fingerprintString(ctx, buffer);
  }


  if (node->local_schema != NULL) {
    _fingerprintString(ctx, "local_schema");
    _fingerprintString(ctx, node->local_schema);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }

  if (node->remote_schema != NULL) {
    _fingerprintString(ctx, "remote_schema");
    _fingerprintString(ctx, node->remote_schema);
  }


  if (node->server_name != NULL) {
    _fingerprintString(ctx, "server_name");
    _fingerprintString(ctx, node->server_name);
  }

  if (node->table_list != NULL && node->table_list->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->table_list, node, "table_list", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "table_list");
  }
}

static void
_fingerprintCreateExtensionStmt(FingerprintContext *ctx, const CreateExtensionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateExtensionStmt");

  if (node->extname != NULL) {
    _fingerprintString(ctx, "extname");
    _fingerprintString(ctx, node->extname);
  }


  if (node->if_not_exists) {    _fingerprintString(ctx, "if_not_exists");
    _fingerprintString(ctx, "true");
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintAlterExtensionStmt(FingerprintContext *ctx, const AlterExtensionStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterExtensionStmt");

  if (node->extname != NULL) {
    _fingerprintString(ctx, "extname");
    _fingerprintString(ctx, node->extname);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
}

static void
_fingerprintAlterExtensionContentsStmt(FingerprintContext *ctx, const AlterExtensionContentsStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterExtensionContentsStmt");
  if (node->action != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->action);
    _fingerprintString(ctx, "action");
    _fingerprintString(ctx, buffer);
  }


  if (node->extname != NULL) {
    _fingerprintString(ctx, "extname");
    _fingerprintString(ctx, node->extname);
  }

  if (node->objargs != NULL && node->objargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objargs, node, "objargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objargs");
  }
  if (node->objname != NULL && node->objname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->objname, node, "objname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "objname");
  }
  if (node->objtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->objtype);
    _fingerprintString(ctx, "objtype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintCreateEventTrigStmt(FingerprintContext *ctx, const CreateEventTrigStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateEventTrigStmt");

  if (node->eventname != NULL) {
    _fingerprintString(ctx, "eventname");
    _fingerprintString(ctx, node->eventname);
  }

  if (node->funcname != NULL && node->funcname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcname, node, "funcname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcname");
  }

  if (node->trigname != NULL) {
    _fingerprintString(ctx, "trigname");
    _fingerprintString(ctx, node->trigname);
  }

  if (node->whenclause != NULL && node->whenclause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whenclause, node, "whenclause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whenclause");
  }
}

static void
_fingerprintAlterEventTrigStmt(FingerprintContext *ctx, const AlterEventTrigStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterEventTrigStmt");
  if (node->tgenabled != 0) {
    char str[2] = {node->tgenabled, '\0'};
    _fingerprintString(ctx, "tgenabled");
    _fingerprintString(ctx, str);
  }


  if (node->trigname != NULL) {
    _fingerprintString(ctx, "trigname");
    _fingerprintString(ctx, node->trigname);
  }

}

static void
_fingerprintRefreshMatViewStmt(FingerprintContext *ctx, const RefreshMatViewStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RefreshMatViewStmt");

  if (node->concurrent) {    _fingerprintString(ctx, "concurrent");
    _fingerprintString(ctx, "true");
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }

  if (node->skipData) {    _fingerprintString(ctx, "skipData");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintReplicaIdentityStmt(FingerprintContext *ctx, const ReplicaIdentityStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ReplicaIdentityStmt");
  if (node->identity_type != 0) {
    char str[2] = {node->identity_type, '\0'};
    _fingerprintString(ctx, "identity_type");
    _fingerprintString(ctx, str);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

}

static void
_fingerprintAlterSystemStmt(FingerprintContext *ctx, const AlterSystemStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterSystemStmt");
  if (node->setstmt != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->setstmt, node, "setstmt", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "setstmt");
  }
}

static void
_fingerprintCreatePolicyStmt(FingerprintContext *ctx, const CreatePolicyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreatePolicyStmt");

  if (node->cmd_name != NULL) {
    _fingerprintString(ctx, "cmd_name");
    _fingerprintString(ctx, node->cmd_name);
  }


  if (node->policy_name != NULL) {
    _fingerprintString(ctx, "policy_name");
    _fingerprintString(ctx, node->policy_name);
  }

  if (node->qual != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->qual, node, "qual", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "qual");
  }
  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
  if (node->table != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->table, node, "table", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "table");
  }
  if (node->with_check != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->with_check, node, "with_check", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "with_check");
  }
}

static void
_fingerprintAlterPolicyStmt(FingerprintContext *ctx, const AlterPolicyStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AlterPolicyStmt");

  if (node->policy_name != NULL) {
    _fingerprintString(ctx, "policy_name");
    _fingerprintString(ctx, node->policy_name);
  }

  if (node->qual != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->qual, node, "qual", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "qual");
  }
  if (node->roles != NULL && node->roles->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->roles, node, "roles", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "roles");
  }
  if (node->table != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->table, node, "table", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "table");
  }
  if (node->with_check != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->with_check, node, "with_check", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "with_check");
  }
}

static void
_fingerprintCreateTransformStmt(FingerprintContext *ctx, const CreateTransformStmt *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateTransformStmt");
  if (node->fromsql != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fromsql, node, "fromsql", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fromsql");
  }

  if (node->lang != NULL) {
    _fingerprintString(ctx, "lang");
    _fingerprintString(ctx, node->lang);
  }


  if (node->replace) {    _fingerprintString(ctx, "replace");
    _fingerprintString(ctx, "true");
  }

  if (node->tosql != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->tosql, node, "tosql", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "tosql");
  }
  if (node->type_name != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->type_name, node, "type_name", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "type_name");
  }
}

static void
_fingerprintA_Expr(FingerprintContext *ctx, const A_Expr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "A_Expr");
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }

  if (node->lexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->lexpr, node, "lexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "lexpr");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->name != NULL && node->name->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->name, node, "name", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "name");
  }
  if (node->rexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->rexpr, node, "rexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "rexpr");
  }
}

static void
_fingerprintColumnRef(FingerprintContext *ctx, const ColumnRef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ColumnRef");
  if (node->fields != NULL && node->fields->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fields, node, "fields", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fields");
  }
  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintParamRef(FingerprintContext *ctx, const ParamRef *node, const void *parent, const char *field_name, unsigned int depth)
{
  // Intentionally ignoring all fields for fingerprinting
}

static void
_fingerprintA_Const(FingerprintContext *ctx, const A_Const *node, const void *parent, const char *field_name, unsigned int depth)
{
  // Intentionally ignoring all fields for fingerprinting
}

static void
_fingerprintFuncCall(FingerprintContext *ctx, const FuncCall *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FuncCall");

  if (node->agg_distinct) {    _fingerprintString(ctx, "agg_distinct");
    _fingerprintString(ctx, "true");
  }

  if (node->agg_filter != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->agg_filter, node, "agg_filter", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "agg_filter");
  }
  if (node->agg_order != NULL && node->agg_order->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->agg_order, node, "agg_order", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "agg_order");
  }

  if (node->agg_star) {    _fingerprintString(ctx, "agg_star");
    _fingerprintString(ctx, "true");
  }


  if (node->agg_within_group) {    _fingerprintString(ctx, "agg_within_group");
    _fingerprintString(ctx, "true");
  }

  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }

  if (node->func_variadic) {    _fingerprintString(ctx, "func_variadic");
    _fingerprintString(ctx, "true");
  }

  if (node->funcname != NULL && node->funcname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcname, node, "funcname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcname");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->over != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->over, node, "over", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "over");
  }
}

static void
_fingerprintA_Star(FingerprintContext *ctx, const A_Star *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "A_Star");
}

static void
_fingerprintA_Indices(FingerprintContext *ctx, const A_Indices *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "A_Indices");
  if (node->lidx != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->lidx, node, "lidx", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "lidx");
  }
  if (node->uidx != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->uidx, node, "uidx", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "uidx");
  }
}

static void
_fingerprintA_Indirection(FingerprintContext *ctx, const A_Indirection *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "A_Indirection");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->indirection != NULL && node->indirection->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->indirection, node, "indirection", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "indirection");
  }
}

static void
_fingerprintA_ArrayExpr(FingerprintContext *ctx, const A_ArrayExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "A_ArrayExpr");
  if (node->elements != NULL && node->elements->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->elements, node, "elements", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "elements");
  }
  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintResTarget(FingerprintContext *ctx, const ResTarget *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ResTarget");
  if (node->indirection != NULL && node->indirection->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->indirection, node, "indirection", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "indirection");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->name != NULL && (field_name == NULL || parent == NULL || !IsA(parent, SelectStmt) || strcmp(field_name, "targetList") != 0)) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }
  if (node->val != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->val, node, "val", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "val");
  }
}

static void
_fingerprintMultiAssignRef(FingerprintContext *ctx, const MultiAssignRef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "MultiAssignRef");
  if (node->colno != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->colno);
    _fingerprintString(ctx, "colno");
    _fingerprintString(ctx, buffer);
  }

  if (node->ncolumns != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->ncolumns);
    _fingerprintString(ctx, "ncolumns");
    _fingerprintString(ctx, buffer);
  }

  if (node->source != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->source, node, "source", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "source");
  }
}

static void
_fingerprintTypeCast(FingerprintContext *ctx, const TypeCast *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TypeCast");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->typeName != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintCollateClause(FingerprintContext *ctx, const CollateClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CollateClause");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->collname != NULL && node->collname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->collname, node, "collname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "collname");
  }
  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintSortBy(FingerprintContext *ctx, const SortBy *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SortBy");
  // Intentionally ignoring node->location for fingerprinting
  if (node->node != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->node, node, "node", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "node");
  }
  if (node->sortby_dir != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->sortby_dir);
    _fingerprintString(ctx, "sortby_dir");
    _fingerprintString(ctx, buffer);
  }

  if (node->sortby_nulls != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->sortby_nulls);
    _fingerprintString(ctx, "sortby_nulls");
    _fingerprintString(ctx, buffer);
  }

  if (node->useOp != NULL && node->useOp->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->useOp, node, "useOp", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "useOp");
  }
}

static void
_fingerprintWindowDef(FingerprintContext *ctx, const WindowDef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "WindowDef");
  if (node->endOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->endOffset, node, "endOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "endOffset");
  }
  if (node->frameOptions != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->frameOptions);
    _fingerprintString(ctx, "frameOptions");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting

  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->orderClause != NULL && node->orderClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->orderClause, node, "orderClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "orderClause");
  }
  if (node->partitionClause != NULL && node->partitionClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->partitionClause, node, "partitionClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "partitionClause");
  }

  if (node->refname != NULL) {
    _fingerprintString(ctx, "refname");
    _fingerprintString(ctx, node->refname);
  }

  if (node->startOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->startOffset, node, "startOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "startOffset");
  }
}

static void
_fingerprintRangeSubselect(FingerprintContext *ctx, const RangeSubselect *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeSubselect");
  if (node->alias != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->alias, node, "alias", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "alias");
  }

  if (node->lateral) {    _fingerprintString(ctx, "lateral");
    _fingerprintString(ctx, "true");
  }

  if (node->subquery != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->subquery, node, "subquery", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "subquery");
  }
}

static void
_fingerprintRangeFunction(FingerprintContext *ctx, const RangeFunction *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeFunction");
  if (node->alias != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->alias, node, "alias", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "alias");
  }
  if (node->coldeflist != NULL && node->coldeflist->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->coldeflist, node, "coldeflist", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "coldeflist");
  }
  if (node->functions != NULL && node->functions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->functions, node, "functions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "functions");
  }

  if (node->is_rowsfrom) {    _fingerprintString(ctx, "is_rowsfrom");
    _fingerprintString(ctx, "true");
  }


  if (node->lateral) {    _fingerprintString(ctx, "lateral");
    _fingerprintString(ctx, "true");
  }


  if (node->ordinality) {    _fingerprintString(ctx, "ordinality");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintRangeTableSample(FingerprintContext *ctx, const RangeTableSample *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeTableSample");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->method != NULL && node->method->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->method, node, "method", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "method");
  }
  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
  if (node->repeatable != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->repeatable, node, "repeatable", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "repeatable");
  }
}

static void
_fingerprintTypeName(FingerprintContext *ctx, const TypeName *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TypeName");
  if (node->arrayBounds != NULL && node->arrayBounds->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arrayBounds, node, "arrayBounds", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arrayBounds");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->names != NULL && node->names->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->names, node, "names", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "names");
  }

  if (node->pct_type) {    _fingerprintString(ctx, "pct_type");
    _fingerprintString(ctx, "true");
  }


  if (node->setof) {    _fingerprintString(ctx, "setof");
    _fingerprintString(ctx, "true");
  }

  if (node->typeOid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typeOid);
    _fingerprintString(ctx, "typeOid");
    _fingerprintString(ctx, buffer);
  }

  if (node->typemod != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->typemod);
    _fingerprintString(ctx, "typemod");
    _fingerprintString(ctx, buffer);
  }

  if (node->typmods != NULL && node->typmods->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typmods, node, "typmods", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typmods");
  }
}

static void
_fingerprintColumnDef(FingerprintContext *ctx, const ColumnDef *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "ColumnDef");
  if (node->collClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->collClause, node, "collClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "collClause");
  }
  if (node->collOid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->collOid);
    _fingerprintString(ctx, "collOid");
    _fingerprintString(ctx, buffer);
  }


  if (node->colname != NULL) {
    _fingerprintString(ctx, "colname");
    _fingerprintString(ctx, node->colname);
  }

  if (node->constraints != NULL && node->constraints->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->constraints, node, "constraints", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "constraints");
  }
  if (node->cooked_default != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cooked_default, node, "cooked_default", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cooked_default");
  }
  if (node->fdwoptions != NULL && node->fdwoptions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fdwoptions, node, "fdwoptions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fdwoptions");
  }
  if (node->inhcount != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->inhcount);
    _fingerprintString(ctx, "inhcount");
    _fingerprintString(ctx, buffer);
  }


  if (node->is_from_type) {    _fingerprintString(ctx, "is_from_type");
    _fingerprintString(ctx, "true");
  }


  if (node->is_local) {    _fingerprintString(ctx, "is_local");
    _fingerprintString(ctx, "true");
  }


  if (node->is_not_null) {    _fingerprintString(ctx, "is_not_null");
    _fingerprintString(ctx, "true");
  }

  // Intentionally ignoring node->location for fingerprinting
  if (node->raw_default != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->raw_default, node, "raw_default", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "raw_default");
  }
  if (node->storage != 0) {
    char str[2] = {node->storage, '\0'};
    _fingerprintString(ctx, "storage");
    _fingerprintString(ctx, str);
  }

  if (node->typeName != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
}

static void
_fingerprintIndexElem(FingerprintContext *ctx, const IndexElem *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "IndexElem");
  if (node->collation != NULL && node->collation->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->collation, node, "collation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "collation");
  }
  if (node->expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->expr, node, "expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "expr");
  }

  if (node->indexcolname != NULL) {
    _fingerprintString(ctx, "indexcolname");
    _fingerprintString(ctx, node->indexcolname);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->nulls_ordering != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->nulls_ordering);
    _fingerprintString(ctx, "nulls_ordering");
    _fingerprintString(ctx, buffer);
  }

  if (node->opclass != NULL && node->opclass->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->opclass, node, "opclass", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "opclass");
  }
  if (node->ordering != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->ordering);
    _fingerprintString(ctx, "ordering");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintConstraint(FingerprintContext *ctx, const Constraint *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "Constraint");

  if (node->access_method != NULL) {
    _fingerprintString(ctx, "access_method");
    _fingerprintString(ctx, node->access_method);
  }


  if (node->conname != NULL) {
    _fingerprintString(ctx, "conname");
    _fingerprintString(ctx, node->conname);
  }

  if (node->contype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->contype);
    _fingerprintString(ctx, "contype");
    _fingerprintString(ctx, buffer);
  }


  if (node->cooked_expr != NULL) {
    _fingerprintString(ctx, "cooked_expr");
    _fingerprintString(ctx, node->cooked_expr);
  }


  if (node->deferrable) {    _fingerprintString(ctx, "deferrable");
    _fingerprintString(ctx, "true");
  }

  if (node->exclusions != NULL && node->exclusions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->exclusions, node, "exclusions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "exclusions");
  }
  if (node->fk_attrs != NULL && node->fk_attrs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->fk_attrs, node, "fk_attrs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "fk_attrs");
  }
  if (node->fk_del_action != 0) {
    char str[2] = {node->fk_del_action, '\0'};
    _fingerprintString(ctx, "fk_del_action");
    _fingerprintString(ctx, str);
  }

  if (node->fk_matchtype != 0) {
    char str[2] = {node->fk_matchtype, '\0'};
    _fingerprintString(ctx, "fk_matchtype");
    _fingerprintString(ctx, str);
  }

  if (node->fk_upd_action != 0) {
    char str[2] = {node->fk_upd_action, '\0'};
    _fingerprintString(ctx, "fk_upd_action");
    _fingerprintString(ctx, str);
  }


  if (node->indexname != NULL) {
    _fingerprintString(ctx, "indexname");
    _fingerprintString(ctx, node->indexname);
  }


  if (node->indexspace != NULL) {
    _fingerprintString(ctx, "indexspace");
    _fingerprintString(ctx, node->indexspace);
  }


  if (node->initdeferred) {    _fingerprintString(ctx, "initdeferred");
    _fingerprintString(ctx, "true");
  }


  if (node->initially_valid) {    _fingerprintString(ctx, "initially_valid");
    _fingerprintString(ctx, "true");
  }


  if (node->is_no_inherit) {    _fingerprintString(ctx, "is_no_inherit");
    _fingerprintString(ctx, "true");
  }

  if (node->keys != NULL && node->keys->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->keys, node, "keys", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "keys");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->old_conpfeqop != NULL && node->old_conpfeqop->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->old_conpfeqop, node, "old_conpfeqop", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "old_conpfeqop");
  }
  if (node->old_pktable_oid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->old_pktable_oid);
    _fingerprintString(ctx, "old_pktable_oid");
    _fingerprintString(ctx, buffer);
  }

  if (node->options != NULL && node->options->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->options, node, "options", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "options");
  }
  if (node->pk_attrs != NULL && node->pk_attrs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->pk_attrs, node, "pk_attrs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "pk_attrs");
  }
  if (node->pktable != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->pktable, node, "pktable", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "pktable");
  }
  if (node->raw_expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->raw_expr, node, "raw_expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "raw_expr");
  }

  if (node->skip_validation) {    _fingerprintString(ctx, "skip_validation");
    _fingerprintString(ctx, "true");
  }

  if (node->where_clause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->where_clause, node, "where_clause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "where_clause");
  }
}

static void
_fingerprintDefElem(FingerprintContext *ctx, const DefElem *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "DefElem");
  if (node->arg != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->arg, node, "arg", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "arg");
  }
  if (node->defaction != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->defaction);
    _fingerprintString(ctx, "defaction");
    _fingerprintString(ctx, buffer);
  }


  if (node->defname != NULL) {
    _fingerprintString(ctx, "defname");
    _fingerprintString(ctx, node->defname);
  }


  if (node->defnamespace != NULL) {
    _fingerprintString(ctx, "defnamespace");
    _fingerprintString(ctx, node->defnamespace);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintRangeTblEntry(FingerprintContext *ctx, const RangeTblEntry *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeTblEntry");
  if (node->alias != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->alias, node, "alias", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "alias");
  }
  if (node->checkAsUser != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->checkAsUser);
    _fingerprintString(ctx, "checkAsUser");
    _fingerprintString(ctx, buffer);
  }

  if (node->ctecolcollations != NULL && node->ctecolcollations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecolcollations, node, "ctecolcollations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecolcollations");
  }
  if (node->ctecoltypes != NULL && node->ctecoltypes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecoltypes, node, "ctecoltypes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecoltypes");
  }
  if (node->ctecoltypmods != NULL && node->ctecoltypmods->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecoltypmods, node, "ctecoltypmods", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecoltypmods");
  }
  if (node->ctelevelsup != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->ctelevelsup);
    _fingerprintString(ctx, "ctelevelsup");
    _fingerprintString(ctx, buffer);
  }


  if (node->ctename != NULL) {
    _fingerprintString(ctx, "ctename");
    _fingerprintString(ctx, node->ctename);
  }

  if (node->eref != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->eref, node, "eref", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "eref");
  }

  if (node->funcordinality) {    _fingerprintString(ctx, "funcordinality");
    _fingerprintString(ctx, "true");
  }

  if (node->functions != NULL && node->functions->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->functions, node, "functions", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "functions");
  }

  if (node->inFromCl) {    _fingerprintString(ctx, "inFromCl");
    _fingerprintString(ctx, "true");
  }


  if (node->inh) {    _fingerprintString(ctx, "inh");
    _fingerprintString(ctx, "true");
  }

  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->insertedCols);

    _fingerprintString(ctx, "insertedCols");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }
  if (node->joinaliasvars != NULL && node->joinaliasvars->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->joinaliasvars, node, "joinaliasvars", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "joinaliasvars");
  }
  if (node->jointype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->jointype);
    _fingerprintString(ctx, "jointype");
    _fingerprintString(ctx, buffer);
  }


  if (node->lateral) {    _fingerprintString(ctx, "lateral");
    _fingerprintString(ctx, "true");
  }

  if (node->relid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->relid);
    _fingerprintString(ctx, "relid");
    _fingerprintString(ctx, buffer);
  }

  if (node->relkind != 0) {
    char str[2] = {node->relkind, '\0'};
    _fingerprintString(ctx, "relkind");
    _fingerprintString(ctx, str);
  }

  if (node->requiredPerms != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->requiredPerms);
    _fingerprintString(ctx, "requiredPerms");
    _fingerprintString(ctx, buffer);
  }

  if (node->rtekind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->rtekind);
    _fingerprintString(ctx, "rtekind");
    _fingerprintString(ctx, buffer);
  }

  if (node->securityQuals != NULL && node->securityQuals->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->securityQuals, node, "securityQuals", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "securityQuals");
  }

  if (node->security_barrier) {    _fingerprintString(ctx, "security_barrier");
    _fingerprintString(ctx, "true");
  }

  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->selectedCols);

    _fingerprintString(ctx, "selectedCols");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }

  if (node->self_reference) {    _fingerprintString(ctx, "self_reference");
    _fingerprintString(ctx, "true");
  }

  if (node->subquery != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->subquery, node, "subquery", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "subquery");
  }
  if (node->tablesample != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->tablesample, node, "tablesample", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "tablesample");
  }
  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->updatedCols);

    _fingerprintString(ctx, "updatedCols");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }
  if (node->values_collations != NULL && node->values_collations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->values_collations, node, "values_collations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "values_collations");
  }
  if (node->values_lists != NULL && node->values_lists->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->values_lists, node, "values_lists", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "values_lists");
  }
}

static void
_fingerprintRangeTblFunction(FingerprintContext *ctx, const RangeTblFunction *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RangeTblFunction");
  if (node->funccolcollations != NULL && node->funccolcollations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funccolcollations, node, "funccolcollations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funccolcollations");
  }
  if (node->funccolcount != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->funccolcount);
    _fingerprintString(ctx, "funccolcount");
    _fingerprintString(ctx, buffer);
  }

  if (node->funccolnames != NULL && node->funccolnames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funccolnames, node, "funccolnames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funccolnames");
  }
  if (node->funccoltypes != NULL && node->funccoltypes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funccoltypes, node, "funccoltypes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funccoltypes");
  }
  if (node->funccoltypmods != NULL && node->funccoltypmods->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funccoltypmods, node, "funccoltypmods", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funccoltypmods");
  }
  if (node->funcexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcexpr, node, "funcexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcexpr");
  }
  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->funcparams);

    _fingerprintString(ctx, "funcparams");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }
}

static void
_fingerprintTableSampleClause(FingerprintContext *ctx, const TableSampleClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TableSampleClause");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->repeatable != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->repeatable, node, "repeatable", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "repeatable");
  }
  if (node->tsmhandler != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->tsmhandler);
    _fingerprintString(ctx, "tsmhandler");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintWithCheckOption(FingerprintContext *ctx, const WithCheckOption *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "WithCheckOption");

  if (node->cascaded) {    _fingerprintString(ctx, "cascaded");
    _fingerprintString(ctx, "true");
  }

  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }


  if (node->polname != NULL) {
    _fingerprintString(ctx, "polname");
    _fingerprintString(ctx, node->polname);
  }

  if (node->qual != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->qual, node, "qual", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "qual");
  }

  if (node->relname != NULL) {
    _fingerprintString(ctx, "relname");
    _fingerprintString(ctx, node->relname);
  }

}

static void
_fingerprintSortGroupClause(FingerprintContext *ctx, const SortGroupClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "SortGroupClause");
  if (node->eqop != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->eqop);
    _fingerprintString(ctx, "eqop");
    _fingerprintString(ctx, buffer);
  }


  if (node->hashable) {    _fingerprintString(ctx, "hashable");
    _fingerprintString(ctx, "true");
  }


  if (node->nulls_first) {    _fingerprintString(ctx, "nulls_first");
    _fingerprintString(ctx, "true");
  }

  if (node->sortop != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->sortop);
    _fingerprintString(ctx, "sortop");
    _fingerprintString(ctx, buffer);
  }

  if (node->tleSortGroupRef != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->tleSortGroupRef);
    _fingerprintString(ctx, "tleSortGroupRef");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintGroupingSet(FingerprintContext *ctx, const GroupingSet *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "GroupingSet");
  if (node->content != NULL && node->content->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->content, node, "content", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "content");
  }
  if (node->kind != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->kind);
    _fingerprintString(ctx, "kind");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintWindowClause(FingerprintContext *ctx, const WindowClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "WindowClause");

  if (node->copiedOrder) {    _fingerprintString(ctx, "copiedOrder");
    _fingerprintString(ctx, "true");
  }

  if (node->endOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->endOffset, node, "endOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "endOffset");
  }
  if (node->frameOptions != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->frameOptions);
    _fingerprintString(ctx, "frameOptions");
    _fingerprintString(ctx, buffer);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  if (node->orderClause != NULL && node->orderClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->orderClause, node, "orderClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "orderClause");
  }
  if (node->partitionClause != NULL && node->partitionClause->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->partitionClause, node, "partitionClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "partitionClause");
  }

  if (node->refname != NULL) {
    _fingerprintString(ctx, "refname");
    _fingerprintString(ctx, node->refname);
  }

  if (node->startOffset != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->startOffset, node, "startOffset", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "startOffset");
  }
  if (node->winref != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->winref);
    _fingerprintString(ctx, "winref");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintFuncWithArgs(FingerprintContext *ctx, const FuncWithArgs *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FuncWithArgs");
  if (node->funcargs != NULL && node->funcargs->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcargs, node, "funcargs", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcargs");
  }
  if (node->funcname != NULL && node->funcname->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->funcname, node, "funcname", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "funcname");
  }
}

static void
_fingerprintAccessPriv(FingerprintContext *ctx, const AccessPriv *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "AccessPriv");
  if (node->cols != NULL && node->cols->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->cols, node, "cols", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "cols");
  }

  if (node->priv_name != NULL) {
    _fingerprintString(ctx, "priv_name");
    _fingerprintString(ctx, node->priv_name);
  }

}

static void
_fingerprintCreateOpClassItem(FingerprintContext *ctx, const CreateOpClassItem *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CreateOpClassItem");
  if (node->args != NULL && node->args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->args, node, "args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "args");
  }
  if (node->class_args != NULL && node->class_args->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->class_args, node, "class_args", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "class_args");
  }
  if (node->itemtype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->itemtype);
    _fingerprintString(ctx, "itemtype");
    _fingerprintString(ctx, buffer);
  }

  if (node->name != NULL && node->name->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->name, node, "name", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "name");
  }
  if (node->number != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->number);
    _fingerprintString(ctx, "number");
    _fingerprintString(ctx, buffer);
  }

  if (node->order_family != NULL && node->order_family->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->order_family, node, "order_family", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "order_family");
  }
  if (node->storedtype != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->storedtype, node, "storedtype", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "storedtype");
  }
}

static void
_fingerprintTableLikeClause(FingerprintContext *ctx, const TableLikeClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "TableLikeClause");
  if (node->options != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->options);
    _fingerprintString(ctx, "options");
    _fingerprintString(ctx, buffer);
  }

  if (node->relation != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->relation, node, "relation", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "relation");
  }
}

static void
_fingerprintFunctionParameter(FingerprintContext *ctx, const FunctionParameter *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "FunctionParameter");
  if (node->argType != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->argType, node, "argType", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "argType");
  }
  if (node->defexpr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->defexpr, node, "defexpr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "defexpr");
  }
  if (node->mode != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->mode);
    _fingerprintString(ctx, "mode");
    _fingerprintString(ctx, buffer);
  }


  if (node->name != NULL) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

}

static void
_fingerprintLockingClause(FingerprintContext *ctx, const LockingClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "LockingClause");
  if (node->lockedRels != NULL && node->lockedRels->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->lockedRels, node, "lockedRels", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "lockedRels");
  }
  if (node->strength != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->strength);
    _fingerprintString(ctx, "strength");
    _fingerprintString(ctx, buffer);
  }

  if (node->waitPolicy != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->waitPolicy);
    _fingerprintString(ctx, "waitPolicy");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintRowMarkClause(FingerprintContext *ctx, const RowMarkClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RowMarkClause");

  if (node->pushedDown) {    _fingerprintString(ctx, "pushedDown");
    _fingerprintString(ctx, "true");
  }

  if (node->rti != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->rti);
    _fingerprintString(ctx, "rti");
    _fingerprintString(ctx, buffer);
  }

  if (node->strength != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->strength);
    _fingerprintString(ctx, "strength");
    _fingerprintString(ctx, buffer);
  }

  if (node->waitPolicy != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->waitPolicy);
    _fingerprintString(ctx, "waitPolicy");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintXmlSerialize(FingerprintContext *ctx, const XmlSerialize *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "XmlSerialize");
  if (node->expr != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->expr, node, "expr", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "expr");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->typeName != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->typeName, node, "typeName", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "typeName");
  }
  if (node->xmloption != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->xmloption);
    _fingerprintString(ctx, "xmloption");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintWithClause(FingerprintContext *ctx, const WithClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "WithClause");
  if (node->ctes != NULL && node->ctes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctes, node, "ctes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctes");
  }
  // Intentionally ignoring node->location for fingerprinting

  if (node->recursive) {    _fingerprintString(ctx, "recursive");
    _fingerprintString(ctx, "true");
  }

}

static void
_fingerprintInferClause(FingerprintContext *ctx, const InferClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "InferClause");

  if (node->conname != NULL) {
    _fingerprintString(ctx, "conname");
    _fingerprintString(ctx, node->conname);
  }

  if (node->indexElems != NULL && node->indexElems->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->indexElems, node, "indexElems", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "indexElems");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
}

static void
_fingerprintOnConflictClause(FingerprintContext *ctx, const OnConflictClause *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "OnConflictClause");
  if (node->action != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->action);
    _fingerprintString(ctx, "action");
    _fingerprintString(ctx, buffer);
  }

  if (node->infer != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->infer, node, "infer", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "infer");
  }
  // Intentionally ignoring node->location for fingerprinting
  if (node->targetList != NULL && node->targetList->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->targetList, node, "targetList", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "targetList");
  }
  if (node->whereClause != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->whereClause, node, "whereClause", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "whereClause");
  }
}

static void
_fingerprintCommonTableExpr(FingerprintContext *ctx, const CommonTableExpr *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "CommonTableExpr");
  if (node->aliascolnames != NULL && node->aliascolnames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->aliascolnames, node, "aliascolnames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "aliascolnames");
  }
  if (node->ctecolcollations != NULL && node->ctecolcollations->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecolcollations, node, "ctecolcollations", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecolcollations");
  }
  if (node->ctecolnames != NULL && node->ctecolnames->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecolnames, node, "ctecolnames", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecolnames");
  }
  if (node->ctecoltypes != NULL && node->ctecoltypes->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecoltypes, node, "ctecoltypes", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecoltypes");
  }
  if (node->ctecoltypmods != NULL && node->ctecoltypmods->length > 0) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctecoltypmods, node, "ctecoltypmods", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctecoltypmods");
  }

  if (node->ctename != NULL) {
    _fingerprintString(ctx, "ctename");
    _fingerprintString(ctx, node->ctename);
  }

  if (node->ctequery != NULL) {
    FingerprintContext subCtx;
    _fingerprintInitForTokens(&subCtx);
    _fingerprintNode(&subCtx, node->ctequery, node, "ctequery", depth + 1);
    _fingerprintCopyTokens(&subCtx, ctx, "ctequery");
  }

  if (node->cterecursive) {    _fingerprintString(ctx, "cterecursive");
    _fingerprintString(ctx, "true");
  }

  if (node->cterefcount != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->cterefcount);
    _fingerprintString(ctx, "cterefcount");
    _fingerprintString(ctx, buffer);
  }

  // Intentionally ignoring node->location for fingerprinting
}

static void
_fingerprintRoleSpec(FingerprintContext *ctx, const RoleSpec *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "RoleSpec");
  // Intentionally ignoring node->location for fingerprinting

  if (node->rolename != NULL) {
    _fingerprintString(ctx, "rolename");
    _fingerprintString(ctx, node->rolename);
  }

  if (node->roletype != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->roletype);
    _fingerprintString(ctx, "roletype");
    _fingerprintString(ctx, buffer);
  }

}

static void
_fingerprintInlineCodeBlock(FingerprintContext *ctx, const InlineCodeBlock *node, const void *parent, const char *field_name, unsigned int depth)
{
  _fingerprintString(ctx, "InlineCodeBlock");

  if (node->langIsTrusted) {    _fingerprintString(ctx, "langIsTrusted");
    _fingerprintString(ctx, "true");
  }

  if (node->langOid != 0) {
    char buffer[50];
    sprintf(buffer, "%d", node->langOid);
    _fingerprintString(ctx, "langOid");
    _fingerprintString(ctx, buffer);
  }


  if (node->source_text != NULL) {
    _fingerprintString(ctx, "source_text");
    _fingerprintString(ctx, node->source_text);
  }

}

