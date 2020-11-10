/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:
			CREATE_P macro_alias qualified_name '(' func_arg_list ')' AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->name = $3;
					n->args = $5;
					n->function = $8;
					$$ = (PGNode *)n;
				}
			| CREATE_P macro_alias qualified_name '(' ')' AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->name = $3;
					n->args = NIL;
					n->function = $7;
					$$ = (PGNode *)n;
				}
 		;

macro_alias:
		FUNCTION
		| MACRO
	;
