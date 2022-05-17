/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX | SEQUENCE | VIEW | MATERIALIZED VIEW ] variations
 *
 * Note: we accept all subcommands for each of the five variants, and sort
 * out what's really legal at execution time.
 *****************************************************************************/
AlterTableStmt:
			ALTER TABLE relation_expr alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = PG_OBJECT_TABLE;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
		|	ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = PG_OBJECT_TABLE;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		|	ALTER INDEX qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = PG_OBJECT_INDEX;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
		|	ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = PG_OBJECT_INDEX;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		|	ALTER SEQUENCE qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = PG_OBJECT_SEQUENCE;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
		|	ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = PG_OBJECT_SEQUENCE;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		|	ALTER VIEW qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = PG_OBJECT_VIEW;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
		|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					PGAlterTableStmt *n = makeNode(PGAlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = PG_OBJECT_VIEW;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		;


alter_identity_column_option_list:
			alter_identity_column_option
				{ $$ = list_make1($1); }
			| alter_identity_column_option_list alter_identity_column_option
				{ $$ = lappend($1, $2); }
		;


alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3; }
			| DROP DEFAULT				{ $$ = NULL; }
		;


alter_identity_column_option:
			RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (PGNode *)$3, @1);
				}
			| SET SeqOptElem
				{
					if (strcmp($2->defname, "as") == 0 ||
						strcmp($2->defname, "restart") == 0 ||
						strcmp($2->defname, "owned_by") == 0)
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("sequence option \"%s\" not supported here", $2->defname),
								 parser_errposition(@2)));
					$$ = $2;
				}
			| SET GENERATED generated_when
				{
					$$ = makeDefElem("generated", (PGNode *) makeInteger($3), @1);
				}
		;


alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;


alter_table_cmd:
			/* ALTER TABLE <name> ADD <coldef> */
			ADD_P columnDef
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AddColumn;
					n->def = $2;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ADD IF NOT EXISTS <coldef> */
			| ADD_P IF_P NOT EXISTS columnDef
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AddColumn;
					n->def = $5;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
			| ADD_P COLUMN columnDef
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AddColumn;
					n->def = $3;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef> */
			| ADD_P COLUMN IF_P NOT EXISTS columnDef
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AddColumn;
					n->def = $6;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_DropNotNull;
					n->name = $3;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetNotNull;
					n->name = $3;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS SignedIconst
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetStatistics;
					n->name = $3;
					n->def = (PGNode *) makeInteger($6);
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId SET reloptions
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetOptions;
					n->name = $3;
					n->def = (PGNode *) $5;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> RESET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId RESET reloptions
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_ResetOptions;
					n->name = $3;
					n->def = (PGNode *) $5;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetStorage;
					n->name = $3;
					n->def = (PGNode *) makeString($6);
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED ... AS IDENTITY ... */
			| ALTER opt_column ColId ADD_P GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					PGConstraint *c = makeNode(PGConstraint);

					c->contype = PG_CONSTR_IDENTITY;
					c->generated_when = $6;
					c->options = $9;
					c->location = @5;

					n->subtype = PG_AT_AddIdentity;
					n->name = $3;
					n->def = (PGNode *) c;

					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET <sequence options>/RESET */
			| ALTER opt_column ColId alter_identity_column_option_list
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetIdentity;
					n->name = $3;
					n->def = (PGNode *) $4;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY */
			| ALTER opt_column ColId DROP IDENTITY_P
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = AT_DropIdentity;
					n->name = $3;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY IF EXISTS */
			| ALTER opt_column ColId DROP IDENTITY_P IF_P EXISTS
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = AT_DropIdentity;
					n->name = $3;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
			| DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ] [RESTRICT|CASCADE]
			 */
			| ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					PGColumnDef *def = makeNode(PGColumnDef);
					n->subtype = PG_AT_AlterColumnType;
					n->name = $3;
					n->def = (PGNode *) def;
					/* We only use these fields of the PGColumnDef node */
					def->typeName = $6;
					def->collClause = (PGCollateClause *) $7;
					def->raw_default = $8;
					def->location = @3;
					$$ = (PGNode *)n;
				}
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
			| ALTER opt_column ColId alter_generic_options
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AlterColumnGenericOptions;
					n->name = $3;
					n->def = (PGNode *) $4;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_AddConstraint;
					n->def = $2;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> ALTER CONSTRAINT ... */
			| ALTER CONSTRAINT name ConstraintAttributeSpec
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					PGConstraint *c = makeNode(PGConstraint);
					n->subtype = PG_AT_AlterConstraint;
					n->def = (PGNode *) c;
					c->contype = PG_CONSTR_FOREIGN; /* others not supported, yet */
					c->conname = $3;
					processCASbits($4, @4, "ALTER CONSTRAINT statement",
									&c->deferrable,
									&c->initdeferred,
									NULL, NULL, yyscanner);
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
			| VALIDATE CONSTRAINT name
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_ValidateConstraint;
					n->name = $3;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_DropConstraint;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> SET LOGGED  */
			| SET LOGGED
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetLogged;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> SET UNLOGGED  */
			| SET UNLOGGED
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetUnLogged;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET reloptions
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_SetRelOptions;
					n->def = (PGNode *)$2;
					$$ = (PGNode *)n;
				}
			/* ALTER TABLE <name> RESET (...) */
			| RESET reloptions
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_ResetRelOptions;
					n->def = (PGNode *)$2;
					$$ = (PGNode *)n;
				}
			| alter_generic_options
				{
					PGAlterTableCmd *n = makeNode(PGAlterTableCmd);
					n->subtype = PG_AT_GenericOptions;
					n->def = (PGNode *)$1;
					$$ = (PGNode *) n;
				}
		;


alter_using:
			USING a_expr				{ $$ = $2; }
			| /* EMPTY */				{ $$ = NULL; }
		;


alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = PG_DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = PG_DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended(NULL, $2, NULL, DEFELEM_DROP, @2);
				}
		;


alter_table_cmds:
			alter_table_cmd							{ $$ = list_make1($1); }
			| alter_table_cmds ',' alter_table_cmd	{ $$ = lappend($1, $3); }
		;


alter_generic_options:
			OPTIONS	'(' alter_generic_option_list ')'		{ $$ = $3; }
		;


opt_set_data: SET DATA_P							{ $$ = 1; }
			| SET									{ $$ = 0; }
			| /*EMPTY*/								{ $$ = 0; }
		;
