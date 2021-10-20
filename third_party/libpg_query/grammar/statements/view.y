/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 *
 *****************************************************************************/
ViewStmt: CREATE_P OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					PGViewStmt *n = makeNode(PGViewStmt);
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $8;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					n->options = $6;
					n->withCheckOption = $9;
					$$ = (PGNode *) n;
				}
		| CREATE_P OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					PGViewStmt *n = makeNode(PGViewStmt);
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					n->options = $8;
					n->withCheckOption = $11;
					$$ = (PGNode *) n;
				}
		| CREATE_P OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					PGViewStmt *n = makeNode(PGViewStmt);
					n->view = $5;
					n->view->relpersistence = $2;
					n->aliases = $7;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $11);
					n->onconflict = PG_ERROR_ON_CONFLICT;
					n->options = $9;
					n->withCheckOption = $12;
					if (n->withCheckOption != PG_NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@12)));
					$$ = (PGNode *) n;
				}
		| CREATE_P OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					PGViewStmt *n = makeNode(PGViewStmt);
					n->view = $7;
					n->view->relpersistence = $4;
					n->aliases = $9;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $13);
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					n->options = $11;
					n->withCheckOption = $14;
					if (n->withCheckOption != PG_NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@14)));
					$$ = (PGNode *) n;
				}
		;


opt_check_option:
		WITH CHECK_P OPTION				{ $$ = CASCADED_CHECK_OPTION; }
		| WITH CASCADED CHECK_P OPTION	{ $$ = CASCADED_CHECK_OPTION; }
		| WITH LOCAL CHECK_P OPTION		{ $$ = PG_LOCAL_CHECK_OPTION; }
		| /* EMPTY */					{ $$ = PG_NO_CHECK_OPTION; }
		;
