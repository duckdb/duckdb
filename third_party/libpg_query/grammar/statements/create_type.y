/*****************************************************************************
 *
 * Create Type Statement
 *
 *****************************************************************************/
CreateTypeStmt:
				CREATE_P TYPE_P qualified_name AS ENUM_P select_with_parens
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->typeName = $3;
					n->kind = PG_NEWTYPE_ENUM;
					n->query = $6;
					n->vals = NULL;
					$$ = (PGNode *)n;
				}
				| CREATE_P TYPE_P qualified_name AS ENUM_P '(' opt_enum_val_list ')'
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->typeName = $3;
					n->kind = PG_NEWTYPE_ENUM;
					n->vals = $7;
					n->query = NULL;
					$$ = (PGNode *)n;
				}
				| CREATE_P TYPE_P qualified_name AS Typename
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->typeName = $3;
					n->query = NULL;
					auto name = std::string(reinterpret_cast<PGValue *>($5->names->tail->data.ptr_value)->val.str);
					if (name == "enum") {
						n->kind = PG_NEWTYPE_ENUM;
						n->vals = $5->typmods;
					} else {
						n->kind = PG_NEWTYPE_ALIAS;
						n->ofType = $5;
					}
					$$ = (PGNode *)n;
				}
				
		;



opt_enum_val_list:
			enum_val_list { $$ = $1;}
			|				{$$ = NIL;}
			;

enum_val_list: Sconst
				{
					$$ = list_make1(makeStringConst($1, @1));
				}
				| enum_val_list ',' Sconst
				{
					$$ = lappend($1, makeStringConst($3, @3));
				}
				;



