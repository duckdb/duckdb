/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static PGRawStmt *
makeRawStmt(PGNode *stmt, int stmt_location)
{
	PGRawStmt    *rs = makeNode(PGRawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

/* Adjust a PGRawStmt to reflect that it doesn't run to the end of the string */
static void
updateRawStmtEnd(PGRawStmt *rs, int end_location)
{
	/*
	 * If we already set the length, don't change it.  This is for situations
	 * like "select foo ;; select bar" where the same statement will be last
	 * in the string for more than one semicolon.
	 */
	if (rs->stmt_len > 0)
		return;

	/* OK, update length of PGRawStmt */
	rs->stmt_len = end_location - rs->stmt_location;
}

static PGNode *
makeColumnRef(char *colname, PGList *indirection,
			  int location, core_yyscan_t yyscanner)
{
	/*
	 * Generate a PGColumnRef node, with an PGAIndirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the PGColumnRef node.
	 */
	PGColumnRef  *c = makeNode(PGColumnRef);
	int		nfields = 0;
	PGListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), PGAIndices))
		{
			PGAIndirection *i = makeNode(PGAIndirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to PGAIndirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (PGNode *) c;
			return (PGNode *) i;
		}
		else if (IsA(lfirst(l), PGAStar))
		{
			/* We only allow '*' at the end of a PGColumnRef */
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (PGNode *) c;
}

static PGNode *
makeTypeCast(PGNode *arg, PGTypeName *tpname, int trycast, int location)
{
	PGTypeCast *n = makeNode(PGTypeCast);
	n->arg = arg;
	n->typeName = tpname;
	n->tryCast = trycast;
	n->location = location;
	return (PGNode *) n;
}

static PGNode *
makeStringConst(char *str, int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGString;
	n->val.val.str = str;
	n->location = location;

	return (PGNode *)n;
}

static PGNode *
makeStringConstCast(char *str, int location, PGTypeName *tpname)
{
	PGNode *s = makeStringConst(str, location);

	return makeTypeCast(s, tpname, 0, -1);
}

static PGNode *
makeIntervalNode(char *str, int location, PGList *typmods) {
	PGIntervalConstant *n = makeNode(PGIntervalConstant);

	n->val_type = T_PGString;
	n->sval = str;
	n->location = location;
	n->typmods = typmods;

	return (PGNode *)n;

}

static PGNode *
makeIntervalNode(int val, int location, PGList *typmods) {
	PGIntervalConstant *n = makeNode(PGIntervalConstant);

	n->val_type = T_PGInteger;
	n->ival = val;
	n->location = location;
	n->typmods = typmods;

	return (PGNode *)n;
}

static PGNode *
makeIntervalNode(PGNode *arg, int location, PGList *typmods) {
	PGIntervalConstant *n = makeNode(PGIntervalConstant);

	n->val_type = T_PGAExpr;
	n->eval = arg;
	n->location = location;
	n->typmods = typmods;

	return (PGNode *)n;
}

static PGNode *
makeSampleSize(PGValue *sample_size, bool is_percentage) {
	PGSampleSize *n = makeNode(PGSampleSize);

	n->sample_size = *sample_size;
	n->is_percentage = is_percentage;

	return (PGNode *)n;
}

static PGNode *
makeSampleOptions(PGNode *sample_size, char *method, int *seed, int location) {
	PGSampleOptions *n = makeNode(PGSampleOptions);

	n->sample_size = sample_size;
	n->method = method;
	if (seed) {
		n->has_seed = true;
		n->seed = *seed;
	}
	n->location = location;

	return (PGNode *)n;
}

/* makeLimitPercent()
 * Make limit percent node
 */
static PGNode *
makeLimitPercent(PGNode *limit_percent) {
	PGLimitPercent *n = makeNode(PGLimitPercent);

	n->limit_percent = limit_percent;

	return (PGNode *)n;
}

static PGNode *
makeIntConst(int val, int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGInteger;
	n->val.val.ival = val;
	n->location = location;

	return (PGNode *)n;
}

static PGNode *
makeFloatConst(char *str, int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGFloat;
	n->val.val.str = str;
	n->location = location;

	return (PGNode *)n;
}

static PGNode *
makeBitStringConst(char *str, int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGBitString;
	n->val.val.str = str;
	n->location = location;

	return (PGNode *)n;
}

static PGNode *
makeNullAConst(int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGNull;
	n->location = location;

	return (PGNode *)n;
}

static PGNode *
makeAConst(PGValue *v, int location)
{
	PGNode *n;

	switch (v->type)
	{
		case T_PGFloat:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_PGInteger:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_PGString:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}

/* makeBoolAConst()
 * Create an PGAConst string node and put it inside a boolean cast.
 */
static PGNode *
makeBoolAConst(bool state, int location)
{
	PGAConst *n = makeNode(PGAConst);

	n->val.type = T_PGString;
	n->val.val.str = (state ? (char*) "t" : (char*) "f");
	n->location = location;

	return makeTypeCast((PGNode *)n, SystemTypeName("bool"), 0, -1);
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
static void
check_qualified_name(PGList *names, core_yyscan_t yyscanner)
{
	PGListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), PGString))
			parser_yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
static PGList *
check_func_name(PGList *names, core_yyscan_t yyscanner)
{
	PGListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), PGString))
			parser_yyerror("syntax error");
	}
	return names;
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
static PGList *
check_indirection(PGList *indirection, core_yyscan_t yyscanner)
{
	PGListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), PGAStar))
		{
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* makeParamRef
 * Creates a new PGParamRef node
 */
static PGNode* makeParamRef(int number, int location)
{
	PGParamRef *p = makeNode(PGParamRef);
	p->number = number;
	p->location = location;
	p->name = NULL;
	return (PGNode *) p;
}

/* makeNamedParamRef
 * Creates a new PGParamRef node
 */
static PGNode* makeNamedParamRef(char *name, int location)
{
	PGParamRef *p = (PGParamRef *)makeParamRef(0, location);
	p->name = name;
	return (PGNode *) p;
}


/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in PGSelectStmt productions.
 */
static void
insertSelectOptions(PGSelectStmt *stmt,
					PGList *sortClause, PGList *lockingClause,
					PGNode *limitOffset, PGNode *limitCount,
					PGWithClause *withClause,
					core_yyscan_t yyscanner)
{
	Assert(IsA(stmt, PGSelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed"),
					 parser_errposition(exprLocation((PGNode *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed"),
					 parser_errposition(exprLocation(limitOffset))));
		stmt->limitOffset = limitOffset;
	}
	if (limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed"),
					 parser_errposition(exprLocation(limitCount))));
		stmt->limitCount = limitCount;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed"),
					 parser_errposition(exprLocation((PGNode *) withClause))));
		stmt->withClause = withClause;
	}
}

static PGNode *
makeSetOp(PGSetOperation op, bool all, PGNode *larg, PGNode *rarg)
{
	PGSelectStmt *n = makeNode(PGSelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (PGSelectStmt *) larg;
	n->rarg = (PGSelectStmt *) rarg;
	return (PGNode *) n;
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
PGList *
SystemFuncName(const char *name)
{
	return list_make2(makeString(DEFAULT_SCHEMA), makeString(name));
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
PGTypeName *
SystemTypeName(const char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString(DEFAULT_SCHEMA),
											   makeString(name)));
}

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
static PGNode *
doNegate(PGNode *n, int location)
{
	if (IsA(n, PGAConst))
	{
		PGAConst *con = (PGAConst *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_PGInteger)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_PGFloat)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "-", NULL, n, location);
}

static void
doNegateFloat(PGValue *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, PGFloat));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
		v->val.str = psprintf("-%s", oldval);
}

static PGNode *
makeAndExpr(PGNode *lexpr, PGNode *rexpr, int location)
{
	PGNode	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, PGAExpr) &&
		   ((PGAExpr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((PGAExpr *) lexp)->lexpr;
	/* Flatten "a AND b AND c ..." to a single PGBoolExpr on sight */
	if (IsA(lexp, PGBoolExpr))
	{
		PGBoolExpr *blexpr = (PGBoolExpr *) lexp;

		if (blexpr->boolop == PG_AND_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (PGNode *) blexpr;
		}
	}
	return (PGNode *) makeBoolExpr(PG_AND_EXPR, list_make2(lexpr, rexpr), location);
}

static PGNode *
makeOrExpr(PGNode *lexpr, PGNode *rexpr, int location)
{
	PGNode	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, PGAExpr) &&
		   ((PGAExpr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((PGAExpr *) lexp)->lexpr;
	/* Flatten "a OR b OR c ..." to a single PGBoolExpr on sight */
	if (IsA(lexp, PGBoolExpr))
	{
		PGBoolExpr *blexpr = (PGBoolExpr *) lexp;

		if (blexpr->boolop == PG_OR_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (PGNode *) blexpr;
		}
	}
	return (PGNode *) makeBoolExpr(PG_OR_EXPR, list_make2(lexpr, rexpr), location);
}

static PGNode *
makeNotExpr(PGNode *expr, int location)
{
	return (PGNode *) makeBoolExpr(PG_NOT_EXPR, list_make1(expr), location);
}

/* Separate PGConstraint nodes from COLLATE clauses in a */
static void
SplitColQualList(PGList *qualList,
				 PGList **constraintList, PGCollateClause **collClause,
				 core_yyscan_t yyscanner)
{
	PGListCell   *cell;
	PGListCell   *prev;
	PGListCell   *next;

	*collClause = NULL;
	prev = NULL;
	for (cell = list_head(qualList); cell; cell = next)
	{
		PGNode   *n = (PGNode *) lfirst(cell);

		next = lnext(cell);
		if (IsA(n, PGConstraint))
		{
			/* keep it in list */
			prev = cell;
			continue;
		}
		if (IsA(n, PGCollateClause))
		{
			PGCollateClause *c = (PGCollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else
			elog(ERROR, "unexpected node type %d", (int) n->type);
		/* remove non-Constraint nodes from qualList */
		qualList = list_delete_cell(qualList, cell, prev);
	}
	*constraintList = qualList;
}

/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

/*----------
 * Recursive view transformation
 *
 * Convert
 *
 *     CREATE RECURSIVE VIEW relname (aliases) AS query
 *
 * to
 *
 *     CREATE VIEW relname (aliases) AS
 *         WITH RECURSIVE relname (aliases) AS (query)
 *         SELECT aliases FROM relname
 *
 * Actually, just the WITH ... part, which is then inserted into the original
 * view as the query.
 * ----------
 */
static PGNode *
makeRecursiveViewSelect(char *relname, PGList *aliases, PGNode *query)
{
	PGSelectStmt *s = makeNode(PGSelectStmt);
	PGWithClause *w = makeNode(PGWithClause);
	PGCommonTableExpr *cte = makeNode(PGCommonTableExpr);
	PGList	   *tl = NIL;
	PGListCell   *lc;

	/* create common table expression */
	cte->ctename = relname;
	cte->aliascolnames = aliases;
	cte->ctequery = query;
	cte->location = -1;

	/* create WITH clause and attach CTE */
	w->recursive = true;
	w->ctes = list_make1(cte);
	w->location = -1;

	/* create target list for the new SELECT from the alias list of the
	 * recursive view specification */
	foreach (lc, aliases)
	{
		PGResTarget *rt = makeNode(PGResTarget);

		rt->name = NULL;
		rt->indirection = NIL;
		rt->val = makeColumnRef(strVal(lfirst(lc)), NIL, -1, 0);
		rt->location = -1;

		tl = lappend(tl, rt);
	}

	/* create new SELECT combining WITH clause, target list, and fake FROM
	 * clause */
	s->withClause = w;
	s->targetList = tl;
	s->fromClause = list_make1(makeRangeVar(NULL, relname, -1));

	return (PGNode *) s;
}

/* parser_init()
 * Initialize to parse one query string
 */
void
parser_init(base_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;		/* in case grammar forgets to set it */
}

#undef yyparse
#undef yylex
#undef yyerror
#undef yylval
#undef yychar
#undef yydebug
#undef yynerrs
#undef yylloc

} // namespace duckdb_libpgquery