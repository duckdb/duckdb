/*****************************************************************************
 *
 * Attach Statement
 *
 *****************************************************************************/
AttachStmt:
				ATTACH opt_database Sconst opt_database_alias copy_options
				{
					PGAttachStmt *n = makeNode(PGAttachStmt);
					n->path = $3;
					n->name = $4;
					n->options = $5;
					$$ = (PGNode *)n;
				}
		;

DetachStmt:
				DETACH opt_database IDENT
				{
					PGDetachStmt *n = makeNode(PGDetachStmt);
					n->missing_ok = false;
					n->db_name = $3;
					$$ = (PGNode *)n;
				}
			|	DETACH DATABASE IF_P EXISTS IDENT
				{
					PGDetachStmt *n = makeNode(PGDetachStmt);
					n->missing_ok = true;
					n->db_name = $5;
					$$ = (PGNode *)n;
				}
		;

opt_database:	DATABASE							{}
			| /*EMPTY*/								{}
		;

opt_database_alias:
			AS ColId									{ $$ = $2; }
			| /*EMPTY*/									{ $$ = NULL; }
		;

ident_name:	IDENT						{ $$ = list_make1(makeString($1)); }

ident_list:
			ident_name								{ $$ = list_make1($1); }
			| ident_list ',' ident_name				{ $$ = lappend($1, $3); }
		;
