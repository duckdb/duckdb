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
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			| DROP drop_type_any_name any_name_list opt_drop_behavior
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			| DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (PGNode *)n;
				}
			| DROP drop_type_name_on_any_name name ON any_name opt_drop_behavior
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (PGNode *) n;
				}
			| DROP drop_type_name_on_any_name IF_P EXISTS name ON any_name opt_drop_behavior
				{
					PGDropStmt *n = makeNode(PGDropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (PGNode *) n;
				}
		;


drop_type_any_name:
			TABLE									{ $$ = PG_OBJECT_TABLE; }
			| SEQUENCE								{ $$ = PG_OBJECT_SEQUENCE; }
			| VIEW									{ $$ = PG_OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = PG_OBJECT_MATVIEW; }
			| INDEX									{ $$ = PG_OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = PG_OBJECT_FOREIGN_TABLE; }
			| COLLATION								{ $$ = PG_OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = PG_OBJECT_CONVERSION; }
			| STATISTICS							{ $$ = PG_OBJECT_STATISTIC_EXT; }
			| TEXT_P SEARCH PARSER					{ $$ = PG_OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = PG_OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = PG_OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = PG_OBJECT_TSCONFIGURATION; }
		;


drop_type_name:
			ACCESS METHOD							{ $$ = PG_OBJECT_ACCESS_METHOD; }
			| EVENT TRIGGER							{ $$ = PG_OBJECT_EVENT_TRIGGER; }
			| EXTENSION								{ $$ = PG_OBJECT_EXTENSION; }
			| FOREIGN DATA_P WRAPPER				{ $$ = PG_OBJECT_FDW; }
			| PUBLICATION							{ $$ = PG_OBJECT_PUBLICATION; }
			| SCHEMA								{ $$ = PG_OBJECT_SCHEMA; }
			| SERVER								{ $$ = PG_OBJECT_FOREIGN_SERVER; }
		;


any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;


opt_drop_behavior:
			CASCADE						{ $$ = PG_DROP_CASCADE; }
			| RESTRICT					{ $$ = PG_DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = PG_DROP_RESTRICT; /* default */ }
		;


drop_type_name_on_any_name:
			POLICY									{ $$ = PG_OBJECT_POLICY; }
			| RULE									{ $$ = PG_OBJECT_RULE; }
			| TRIGGER								{ $$ = PG_OBJECT_TRIGGER; }
		;
