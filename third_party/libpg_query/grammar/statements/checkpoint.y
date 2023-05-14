/*
 * Checkpoint statement
 */
CheckPointStmt:
			FORCE CHECKPOINT opt_col_id
				{
					PGCheckPointStmt *n = makeNode(PGCheckPointStmt);
					n->force = true;
					n->name = $3;
					$$ = (PGNode *)n;
				}
			| CHECKPOINT opt_col_id
				{
					PGCheckPointStmt *n = makeNode(PGCheckPointStmt);
					n->force = false;
					n->name = $2;
					$$ = (PGNode *)n;
				}
		;

opt_col_id:
	ColId 						{ $$ = $1; }
	| /* empty */ 				{ $$ = NULL; }
