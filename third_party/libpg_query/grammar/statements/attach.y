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
				DETACH DATABASE any_name_list
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = PG_OBJECT_DATABASE;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = PG_DROP_RESTRICT;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			|	DETACH ident_list
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = PG_OBJECT_DATABASE;
					n->missing_ok = false;
					n->objects = $2;
					n->behavior = PG_DROP_RESTRICT;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			|	DETACH DATABASE IF_P EXISTS any_name_list
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = PG_OBJECT_DATABASE;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = PG_DROP_RESTRICT;
					n->concurrent = false;
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
