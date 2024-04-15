/*****************************************************************************
 *
 *		QUERY:
 *				PGUpdateExtensionsStmt (UPDATE EXTENSIONS)
 *
 *****************************************************************************/
UpdateStmt: opt_with_clause UPDATE EXTENSIONS opt_column_list
				{
					PGUpdateExtensionsStmt *n = makeNode(PGUpdateExtensionsStmt);
					n->extensions = $4;
					$$ = (PGNode *)n;
				}
		;
