/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:

		CREATE_P macro_alias TABLE qualified_name param_list AS SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $4;
				n->params = $5;
				n->function = NULL;
				n->query = $7;
				$$ = (PGNode *)n;
			}
 		|

        /*
 		CREATE_P  macro_alias SCALAR qualified_name param_list AS b_expr
                	{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $4;
				n->params = $5;
				n->function = $7;
				n->query = NULL;
				$$ = (PGNode *)n;
                	}
                 |
          */

		CREATE_P macro_alias qualified_name param_list AS a_expr
                         {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $3;
				n->params = $4;
				n->function = $6;
				n->query = NULL;
				$$ = (PGNode *)n;
                         }

 		;



macro_alias:
		FUNCTION
		| MACRO


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
