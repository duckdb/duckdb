/*****************************************************************************
 *
 * Create Type Statement
 *
 *****************************************************************************/
CreateTypeStmt:
                CREATE_P TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->kind = PG_NEWTYPE_ENUM;
					n->typeName = $3;
					n->vals = $7;
					$$ = (PGNode *)n;
				}
				| CREATE_P TYPE_P any_name AS ConstTypename
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->kind = PG_NEWTYPE_ALIAS;
					n->typeName = $3;
					n->ofType = $5;
					$$ = (PGNode *)n;
				}
		;

opt_enum_val_list:
		enum_val_list							{ $$ = $1; }
		| /*EMPTY*/							{ $$ = NIL; }
		;

enum_val_list:	Sconst
				{ $$ = list_make1(makeString($1)); }
			| enum_val_list ',' Sconst
				{ $$ = lappend($1, makeString($3)); }
		;
