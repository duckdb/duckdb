/*****************************************************************************
 *
 * Create Enum Statement
 *
 *****************************************************************************/
CreateEnumStmt:
                CREATE_P TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
				{
					PGCreateEnumStmt *n = makeNode(PGCreateEnumStmt);
					n->typeName = $3;
					n->vals = $7;
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
