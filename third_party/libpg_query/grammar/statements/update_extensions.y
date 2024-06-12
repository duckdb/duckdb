/*****************************************************************************
 *
 *		QUERY:
 *				PGUpdateExtensionsStmt (UPDATE EXTENSIONS)
 *
 *****************************************************************************/
UpdateExtensionsStmt: opt_with_clause UPDATE EXTENSIONS opt_column_list
				{
					PGUpdateExtensionsStmt *n = makeNode(PGUpdateExtensionsStmt);
					n->extensions = $4;

					if ($1) {
                          ereport(ERROR,
                                  (errcode(PG_ERRCODE_SYNTAX_ERROR),
                                   errmsg("Providing a with clause with an UPDATE EXTENSIONS statement is not allowed"),
                                   parser_errposition(@1)));
                          break;
                    }

					$$ = (PGNode *)n;
				}
		;
