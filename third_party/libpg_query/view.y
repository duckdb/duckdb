/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 *
 *****************************************************************************/
stmt: ViewStmt
%type <ival> opt_check_option

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $8;
					n->replace = false;
					n->options = $6;
					n->withCheckOption = $9;
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->replace = true;
					n->options = $8;
					n->withCheckOption = $11;
					$$ = (Node *) n;
				}
		| CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $5;
					n->view->relpersistence = $2;
					n->aliases = $7;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $11);
					n->replace = false;
					n->options = $9;
					n->withCheckOption = $12;
					if (n->withCheckOption != NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@12)));
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $7;
					n->view->relpersistence = $4;
					n->aliases = $9;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $13);
					n->replace = true;
					n->options = $11;
					n->withCheckOption = $14;
					if (n->withCheckOption != NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@14)));
					$$ = (Node *) n;
				}
		;

opt_check_option:
		WITH CHECK OPTION				{ $$ = CASCADED_CHECK_OPTION; }
		| WITH CASCADED CHECK OPTION	{ $$ = CASCADED_CHECK_OPTION; }
		| WITH LOCAL CHECK OPTION		{ $$ = LOCAL_CHECK_OPTION; }
		| /* EMPTY */					{ $$ = NO_CHECK_OPTION; }
		;

opt_as:		AS										{}
			| /* EMPTY */							{}
		;
