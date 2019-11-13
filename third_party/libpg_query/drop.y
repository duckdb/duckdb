/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...]
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/
stmt: DropStmt
%type <objtype>	drop_type_any_name drop_type_name drop_type_name_on_any_name

DropStmt:	DROP drop_type_any_name IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_any_name any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = false;
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

/* object types taking any_name_list */
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

/* object types taking name_list */
drop_type_name:
			SCHEMA								{ $$ = OBJECT_SCHEMA; }
		;

/* object types attached to a table */
drop_type_name_on_any_name:
			POLICY									{ $$ = OBJECT_POLICY; }
			| RULE									{ $$ = OBJECT_RULE; }
		;
