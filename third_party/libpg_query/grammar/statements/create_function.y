/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:
			CREATE_P macro_alias qualified_name param_list AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->name = $3;
					n->args = $4;
					n->function = $6;
					$$ = (PGNode *)n;
				}
 		;

macro_alias:
		FUNCTION
		| MACRO
	;

param_list:
		'(' ')'
			{
				$$ = NIL;
			}
		| '(' func_arg_list ')'
			{
				$$ = $2;
			}
	;
