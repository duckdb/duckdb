/*****************************************************************************
 *
 * PRAGMA stmt
 *
 *****************************************************************************/
PragmaStmt:
			PRAGMA_P ColId
				{
					PGPragmaStmt *n = makeNode(PGPragmaStmt);
					n->kind = PG_PRAGMA_TYPE_NOTHING;
					n->name = $2;
					$$ = (PGNode *)n;
				}
			| PRAGMA_P ColId '=' var_list
				{
					PGPragmaStmt *n = makeNode(PGPragmaStmt);
					n->kind = PG_PRAGMA_TYPE_ASSIGNMENT;
					n->name = $2;
					n->args = $4;
					$$ = (PGNode *)n;
				}
			| PRAGMA_P ColId '(' func_arg_list ')'
				{
					PGPragmaStmt *n = makeNode(PGPragmaStmt);
					n->kind = PG_PRAGMA_TYPE_CALL;
					n->name = $2;
					n->args = $4;
					$$ = (PGNode *)n;
				}
		;

