/*****************************************************************************
 *
 * Create Alias Statement
 *
 *****************************************************************************/
CreateAliasStmt:
                CREATE_P TYPE_P any_name FROM Typename
				{
					PGCreateAliasStmt *n = makeNode(PGCreateAliasStmt);
					n->typeName = $5;
					n->aliasname = $3;
					$$ = (PGNode *)n;
				}
		;
