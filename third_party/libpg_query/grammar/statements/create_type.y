/*****************************************************************************
 *
 * Create Type Statement
 *
 *****************************************************************************/
CreateTypeStmt:
				CREATE_P OptTemp TYPE_P qualified_name AS create_type_value
				{
					PGCreateTypeStmt *n = (PGCreateTypeStmt *) $6;
					$4->relpersistence = $2;
					n->typeName = $4;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
				| CREATE_P OptTemp TYPE_P IF_P NOT EXISTS qualified_name AS create_type_value
				{
					PGCreateTypeStmt *n = (PGCreateTypeStmt *) $9;
					$7->relpersistence = $2;
					n->typeName = $7;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
				| CREATE_P OR REPLACE OptTemp TYPE_P qualified_name AS create_type_value
				{
					PGCreateTypeStmt *n = (PGCreateTypeStmt *) $8;
					$6->relpersistence = $4;
					n->typeName = $6;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

create_type_value:
	ENUM_P select_with_parens
	{
		PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
		n->kind = PG_NEWTYPE_ENUM;
		n->query = $2;
		n->vals = NULL;
		$$ = (PGNode *)n;
	}
	| ENUM_P '(' opt_enum_val_list ')'
	{
		PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
		n->kind = PG_NEWTYPE_ENUM;
		n->vals = $3;
		n->query = NULL;
		$$ = (PGNode *)n;
	}
	| Typename
	{
		PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
		n->query = NULL;
		auto name = std::string(reinterpret_cast<PGValue *>($1->names->tail->data.ptr_value)->val.str);
		if (name == "enum") {
			n->kind = PG_NEWTYPE_ENUM;
			n->vals = $1->typmods;
		} else {
			n->kind = PG_NEWTYPE_ALIAS;
			n->ofType = $1;
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



