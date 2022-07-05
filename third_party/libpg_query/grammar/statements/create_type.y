/*****************************************************************************
 *
 * Create Type Statement
 *
 *****************************************************************************/
CreateTypeStmt:
				CREATE_P TYPE_P any_name AS Typename
				{
					PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->typeName = $3;
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
