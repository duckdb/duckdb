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
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
				| ATTACH IF_P NOT EXISTS opt_database Sconst opt_database_alias copy_options
				{
					PGAttachStmt *n = makeNode(PGAttachStmt);
					n->path = $6;
					n->name = $7;
					n->options = $8;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

DetachStmt:
				DETACH ColLabel
				{
					PGDetachStmt *n = makeNode(PGDetachStmt);
					n->missing_ok = false;
					n->db_name = $2;
					$$ = (PGNode *)n;
				}
			|	DETACH DATABASE ColLabel
				{
					PGDetachStmt *n = makeNode(PGDetachStmt);
					n->missing_ok = false;
					n->db_name = $3;
					$$ = (PGNode *)n;
				}
			|	DETACH DATABASE IF_P EXISTS ColLabel
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
