/*****************************************************************************
 *
 *		QUERY:
 *				PREPARE <plan_name> [(args, ...)] AS <query>
 *
 *****************************************************************************/
PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt
				{
					PGPrepareStmt *n = makeNode(PGPrepareStmt);
					n->name = $2;
					n->argtypes = $3;
					n->query = $5;
					$$ = (PGNode *) n;
				}
		;


prep_type_clause: '(' type_list ')'			{ $$ = $2; }
				| /* EMPTY */				{ $$ = NIL; }
		;

PreparableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt					/* by default all are $$=$1 */
		;
