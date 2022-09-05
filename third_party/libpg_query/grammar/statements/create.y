/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *****************************************************************************/
CreateStmt:	CREATE_P OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptWith OnCommitOption
				{
					PGCreateStmt *n = makeNode(PGCreateStmt);
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $8;
					n->oncommit = $9;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		| CREATE_P OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptWith
			OnCommitOption
				{
					PGCreateStmt *n = makeNode(PGCreateStmt);
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $11;
					n->oncommit = $12;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		| CREATE_P OR REPLACE OptTemp TABLE qualified_name '('
			OptTableElementList ')' OptWith
			OnCommitOption
				{
					PGCreateStmt *n = makeNode(PGCreateStmt);
					$6->relpersistence = $4;
					n->relation = $6;
					n->tableElts = $8;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $10;
					n->oncommit = $11;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;


ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 parser_errposition(@2)));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties"),
								 parser_errposition(@2)));
					$$ = newspec;
				}
		;


def_arg:	func_type						{ $$ = (PGNode *)$1; }
			| reserved_keyword				{ $$ = (PGNode *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (PGNode *)$1; }
			| NumericOnly					{ $$ = (PGNode *)$1; }
			| Sconst						{ $$ = (PGNode *)makeString($1); }
			| NONE							{ $$ = (PGNode *)makeString(pstrdup($1)); }
		;


OptParenthesizedSeqOptList: '(' SeqOptList ')'		{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


generic_option_arg:
				Sconst				{ $$ = (PGNode *) makeString($1); }
		;


key_action:
			NO ACTION					{ $$ = PG_FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = PG_FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = PG_FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = PG_FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = PG_FKCONSTR_ACTION_SETDEFAULT; }
		;


ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					PGConstraint *n = castNode(PGConstraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (PGNode *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE any_name
				{
					/*
					 * Note: the PGCollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					PGCollateClause *n = makeNode(PGCollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (PGNode *) n;
				}
		;


ColConstraintElem:
			NOT NULL_P
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_NOTNULL;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| NULL_P
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_NULL;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| UNIQUE opt_definition
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					$$ = (PGNode *)n;
				}
			| PRIMARY KEY opt_definition
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					$$ = (PGNode *)n;
				}
			| CHECK_P '(' a_expr ')' opt_no_inherit
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->skip_validation = false;
					n->initially_valid = true;
					$$ = (PGNode *)n;
				}
			| USING COMPRESSION name
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_COMPRESSION;
					n->location = @1;
					n->compression_name = $3;
					$$ = (PGNode *)n;
				}
			| DEFAULT b_expr
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (PGNode *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (PGNode *)n;
				}
		;

GeneratedColumnType:
			VIRTUAL { $$ = PG_CONSTR_GENERATED_VIRTUAL; }
			| STORED { $$ = PG_CONSTR_GENERATED_STORED; }
			;

opt_GeneratedColumnType:
			GeneratedColumnType { $$ = $1; }
			| /* EMPTY */ { $$ = PG_CONSTR_GENERATED_VIRTUAL; }
			;

GeneratedConstraintElem:
			GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_IDENTITY;
					n->generated_when = $2;
					n->options = $5;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| GENERATED generated_when AS '(' a_expr ')' opt_GeneratedColumnType
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = $7;
					n->generated_when = $2;
					n->raw_expr = $5;
					n->cooked_expr = NULL;
					n->location = @1;

					/*
					 * Can't do this in the grammar because of shift/reduce
					 * conflicts.  (IDENTITY allows both ALWAYS and BY
					 * DEFAULT, but generated columns only allow ALWAYS.)  We
					 * can also give a more useful error message and location.
					 */
					if ($2 != PG_ATTRIBUTE_IDENTITY_ALWAYS)
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("for a generated column, GENERATED ALWAYS must be specified"),
								 parser_errposition(@2)));

					$$ = (PGNode *)n;
				}
			| AS '(' a_expr ')' opt_GeneratedColumnType
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = $5;
					n->generated_when = PG_ATTRIBUTE_IDENTITY_ALWAYS;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->location = @1;
					$$ = (PGNode *)n;
				}
	    ;


generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;


key_update: ON UPDATE key_action		{ $$ = $3; }
		;


key_actions:
			key_update
				{ $$ = ($1 << 8) | (PG_FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (PG_FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (PG_FKCONSTR_ACTION_NOACTION << 8) | (PG_FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = PG_ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = PG_ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = PG_ONCOMMIT_NOOP; }
		;


reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;


opt_no_inherit:	NO INHERIT							{  $$ = true; }
			| /* EMPTY */							{  $$ = false; }
		;


TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					PGConstraint *n = castNode(PGConstraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (PGNode *) n;
				}
			| ConstraintElem						{ $$ = $1; }
		;


TableLikeOption:
				COMMENTS			{ $$ = PG_CREATE_TABLE_LIKE_COMMENTS; }
				| CONSTRAINTS		{ $$ = PG_CREATE_TABLE_LIKE_CONSTRAINTS; }
				| DEFAULTS			{ $$ = PG_CREATE_TABLE_LIKE_DEFAULTS; }
				| IDENTITY_P		{ $$ = PG_CREATE_TABLE_LIKE_IDENTITY; }
				| INDEXES			{ $$ = PG_CREATE_TABLE_LIKE_INDEXES; }
				| STATISTICS		{ $$ = PG_CREATE_TABLE_LIKE_STATISTICS; }
				| STORAGE			{ $$ = PG_CREATE_TABLE_LIKE_STORAGE; }
				| ALL				{ $$ = PG_CREATE_TABLE_LIKE_ALL; }
		;



reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;


ExistingIndex:   USING INDEX index_name				{ $$ = $3; }
		;


ConstraintAttr:
			DEFERRABLE
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| NOT DEFERRABLE
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| INITIALLY DEFERRED
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (PGNode *)n;
				}
			| INITIALLY IMMEDIATE
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (PGNode *)n;
				}
		;



OptWith:
			WITH reloptions				{ $$ = $2; }
			| WITH OIDS					{ $$ = list_make1(makeDefElem("oids", (PGNode *) makeInteger(true), @1)); }
			| WITHOUT OIDS				{ $$ = list_make1(makeDefElem("oids", (PGNode *) makeInteger(false), @1)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;


definition: '(' def_list ')'						{ $$ = $2; }
		;


TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = 0; }
		;


generic_option_name:
				ColLabel			{ $$ = $1; }
		;


ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;



columnDef:	ColId Typename ColQualList
				{
					PGColumnDef *n = makeNode(PGColumnDef);
					n->category = COL_STANDARD;
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($3, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (PGNode *)n;
			}
			|
			ColId opt_Typename GeneratedConstraintElem ColQualList
				{
					PGColumnDef *n = makeNode(PGColumnDef);
					n->category = COL_GENERATED;
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					// merge the constraints with the generated column constraint
					auto constraints = $4;
					if (constraints) {
					    constraints = lappend(constraints, $3);
					} else {
					    constraints = list_make1($3);
					}
					SplitColQualList(constraints, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (PGNode *)n;
			}
		;


def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;


index_name: ColId									{ $$ = $1; };


TableElement:
			columnDef							{ $$ = $1; }
			| TableLikeClause					{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;


def_elem:	ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (PGNode *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;


opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


OptTableElementList:
			TableElementList					{ $$ = $1; }
			| TableElementList ','					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;


columnElem: ColId
				{
					$$ = (PGNode *) makeString($1);
				}
		;


opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;


key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;


reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (PGNode *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
			| ColLabel '.' ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (PGNode *) $5,
											 PG_DEFELEM_UNSPEC, @1);
				}
			| ColLabel '.' ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, PG_DEFELEM_UNSPEC, @1);
				}
		;


columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnList_opt_comma:
			columnList								{ $$ = $1; }
			| columnList ','						{ $$ = $1; }
		;


func_type:	Typename								{ $$ = $1; }
			| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->setof = true;
					$$->location = @2;
				}
		;


ConstraintElem:
			CHECK_P '(' a_expr ')' ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (PGNode *)n;
				}
			| UNIQUE '(' columnList_opt_comma ')' opt_definition
				ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->options = $5;
					n->indexname = NULL;
					processCASbits($6, @6, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (PGNode *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (PGNode *)n;
				}
			| PRIMARY KEY '(' columnList_opt_comma ')' opt_definition
				ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->options = $6;
					n->indexname = NULL;
					processCASbits($7, @7, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (PGNode *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (PGNode *)n;
				}
			| FOREIGN KEY '(' columnList_opt_comma ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					PGConstraint *n = makeNode(PGConstraint);
					n->contype = PG_CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (PGNode *)n;
				}
		;


TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;


key_match:  MATCH FULL
			{
				$$ = PG_FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 parser_errposition(@1)));
				$$ = PG_FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = PG_FKCONSTR_MATCH_SIMPLE;
			}
		| /*EMPTY*/
			{
				$$ = PG_FKCONSTR_MATCH_SIMPLE;
			}
		;


TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					PGTableLikeClause *n = makeNode(PGTableLikeClause);
					n->relation = $2;
					n->options = $3;
					$$ = (PGNode *)n;
				}
		;


OptTemp:	TEMPORARY					{ $$ = PG_RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = PG_RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = PG_RELPERSISTENCE_TEMP; }
			| LOCAL TEMP				{ $$ = PG_RELPERSISTENCE_TEMP; }
			| GLOBAL TEMPORARY
				{
					ereport(PGWARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = PG_RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					ereport(PGWARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = PG_RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = PG_RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;


generated_when:
			ALWAYS			{ $$ = PG_ATTRIBUTE_IDENTITY_ALWAYS; }
			| BY DEFAULT	{ $$ = ATTRIBUTE_IDENTITY_BY_DEFAULT; }
		;
