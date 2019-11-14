/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 *****************************************************************************/
IndexStmt:	CREATE opt_unique INDEX opt_concurrently opt_index_name
			ON relation_expr '(' index_params ')'
			opt_include opt_reloptions where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $5;
					n->relation = $7;
					n->indexParams = $9;
					n->indexIncludingParams = $11;
					n->options = $12;
					n->whereClause = $13;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					n->transformed = false;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS index_name
			ON relation_expr '(' index_params ')'
			opt_include opt_reloptions where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $8;
					n->relation = $10;
					n->indexParams = $12;
					n->indexIncludingParams = $14;
					n->options = $15;
					n->whereClause = $16;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					n->transformed = false;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;

opt_unique:
			UNIQUE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_concurrently:
			CONCURRENTLY							{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_index_name:
			index_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

index_name: ColId									{ $$ = $1; };

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
		;

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = $6;
					$$->nulls_ordering = $7;
				}
		;

opt_include:		INCLUDE '(' index_including_params ')'			{ $$ = $3; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

index_including_params:	index_elem						{ $$ = list_make1($1); }
			| index_including_params ',' index_elem		{ $$ = lappend($1, $3); }
		;

opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_class:	any_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
