/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
  *****************************************************************************/
 CreateFunctionStmt:
                /* the OptTemp is present but not used - to avoid conflicts with other CREATE_P Stmtatements */
		CREATE_P OptTemp macro_alias qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$4->relpersistence = $2;
				n->name = $4;
				n->functions = $5;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
 		|
 		CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->functions = $8;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				$$ = (PGNode *)n;

			}
		|
		CREATE_P OR REPLACE OptTemp macro_alias qualified_name table_macro_list
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->functions = $7;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
		|
		CREATE_P OptTemp macro_alias qualified_name macro_definition_list
             {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$4->relpersistence = $2;
				n->name = $4;
				n->functions = $5;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
             }
		|
		CREATE_P OptTemp macro_alias IF_P NOT EXISTS qualified_name macro_definition_list
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$7->relpersistence = $2;
				n->name = $7;
				n->functions = $8;
				n->onconflict = PG_IGNORE_ON_CONFLICT;
				$$ = (PGNode *)n;
			 }
		|
		CREATE_P OR REPLACE OptTemp macro_alias qualified_name macro_definition_list
			 {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				$6->relpersistence = $4;
				n->name = $6;
				n->functions = $7;
				n->onconflict = PG_REPLACE_ON_CONFLICT;
				$$ = (PGNode *)n;
			 }
 		;

table_macro_definition:
		param_list AS TABLE select_no_parens
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $4;
				$$ = (PGNode *)n;
			}
	;

table_macro_definition_parens:
		param_list AS TABLE select_with_parens
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->query = $4;
				$$ = (PGNode *)n;
			}
	;

table_macro_list_internal:
		table_macro_definition_parens
			{
				$$ = list_make1($1);
			}
		| table_macro_list_internal ',' table_macro_definition_parens
			{
				$$ = lappend($1, $3);
			}
	;

table_macro_list:
		table_macro_definition
			{
				$$ = list_make1($1);
			}
		| table_macro_list_internal
	;

macro_definition:
		param_list AS a_expr
			{
				PGFunctionDefinition *n = makeNode(PGFunctionDefinition);
				n->params = $1;
				n->function = $3;
				$$ = (PGNode *)n;
			}
	;

macro_definition_list:  macro_definition
				{
					$$ = list_make1($1);
				}
			| macro_definition_list ',' macro_definition
				{
					$$ = lappend($1, $3);
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
