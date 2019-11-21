/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/
IndexStmt:	CREATE_P opt_unique INDEX opt_concurrently opt_index_name
			ON qualified_name access_method_clause '(' index_params ')'
			opt_reloptions where_clause
				{
					PGIndexStmt *n = makeNode(PGIndexStmt);
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $5;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
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
					$$ = (PGNode *)n;
				}
			| CREATE_P opt_unique INDEX opt_concurrently IF_P NOT EXISTS index_name
			ON qualified_name access_method_clause '(' index_params ')'
			opt_reloptions where_clause
				{
					PGIndexStmt *n = makeNode(PGIndexStmt);
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $8;
					n->relation = $10;
					n->accessMethod = $11;
					n->indexParams = $13;
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
					$$ = (PGNode *)n;
				}
		;


access_method:
			ColId									{ $$ = $1; };


access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = (char*) DEFAULT_INDEX_TYPE; }
		;


opt_concurrently:
			CONCURRENTLY							{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;


opt_index_name:
			index_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;


opt_unique:
			UNIQUE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;
