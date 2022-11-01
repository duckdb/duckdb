/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
  *****************************************************************************/
 CreateFunctionStmt:
                /* the OptTemp is present but not used - to avoid conflicts with other CREATE_P Stmtatements */
		CREATE_P OptTemp macro_alias qualified_name param_list AS TABLE SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$4->relpersistence = $2;
				n->name = $4;
				n->params = $5;
				n->function = NULL;
				n->query = $8;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
 		|
 		CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name param_list AS TABLE SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->params = $8;
				n->function = NULL;
				n->query = $11;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				$$ = (PGNode *)n;

			}
		|
		CREATE_P OR REPLACE OptTemp macro_alias qualified_name param_list AS TABLE SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->params = $7;
				n->function = NULL;
				n->query = $10;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				$$ = (PGNode *)n;

			}
		|
		CREATE_P OptTemp macro_alias qualified_name param_list AS a_expr
                         {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$4->relpersistence = $2;
				n->name = $4;
				n->params = $5;
				n->function = $7;
				n->query = NULL;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
                         }
		|
		CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name param_list AS a_expr
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->params = $8;
				n->function = $10;
				n->query = NULL;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				$$ = (PGNode *)n;
			 }
		|
		CREATE_P OR REPLACE OptTemp macro_alias qualified_name param_list AS a_expr
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->params = $7;
				n->function = $9;
				n->query = NULL;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
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
