/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...]
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/
DropStmt:	DROP drop_type_any_name IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_any_name any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name_on_any_name name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP drop_type_name_on_any_name IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;


drop_type_any_name:
			TABLE									{ $$ = OBJECT_TABLE; }
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| COLLATION								{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| STATISTICS							{ $$ = OBJECT_STATISTIC_EXT; }
			| TEXT_P SEARCH PARSER					{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = OBJECT_TSCONFIGURATION; }
		;


drop_type_name:
			ACCESS METHOD							{ $$ = OBJECT_ACCESS_METHOD; }
			| EVENT TRIGGER							{ $$ = OBJECT_EVENT_TRIGGER; }
			| EXTENSION								{ $$ = OBJECT_EXTENSION; }
			| FOREIGN DATA_P WRAPPER				{ $$ = OBJECT_FDW; }
			| PUBLICATION							{ $$ = OBJECT_PUBLICATION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| SERVER								{ $$ = OBJECT_FOREIGN_SERVER; }
		;


any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;


opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;


drop_type_name_on_any_name:
			POLICY									{ $$ = OBJECT_POLICY; }
			| RULE									{ $$ = OBJECT_RULE; }
			| TRIGGER								{ $$ = OBJECT_TRIGGER; }
		;
