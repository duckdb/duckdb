/*****************************************************************************
 *
 * Attach Statement
 *
 *****************************************************************************/
AttachStmt:
				ATTACH opt_database Sconst opt_database_alias
				{
					PGAttachStmt *n = makeNode(PGAttachStmt);
					n->path = $3;
					n->name = $4;
					$$ = (PGNode *)n;
				}
		;

opt_database:	DATABASE									{}
			| /*EMPTY*/								{}
		;


opt_database_alias:
			AS ColId									{ $$ = $2; }
			| /*EMPTY*/									{ $$ = NULL; }
		;
