/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS PGSelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/
CreateAsStmt:
		CREATE_P OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = PG_OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->onconflict = PG_ERROR_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($7);
					$$ = (PGNode *) ctas;
				}
		| CREATE_P OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
					ctas->query = $9;
					ctas->into = $7;
					ctas->relkind = PG_OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->onconflict = PG_IGNORE_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$7->rel->relpersistence = $2;
					$7->skipData = !($10);
					$$ = (PGNode *) ctas;
				}
		| CREATE_P OR REPLACE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
					ctas->query = $8;
					ctas->into = $6;
					ctas->relkind = PG_OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->onconflict = PG_REPLACE_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$6->rel->relpersistence = $4;
					$6->skipData = !($9);
					$$ = (PGNode *) ctas;
				}
		;


opt_with_data:
			WITH DATA_P								{ $$ = true; }
			| WITH NO DATA_P						{ $$ = false; }
			| /*EMPTY*/								{ $$ = true; }
		;


create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption
				{
					$$ = makeNode(PGIntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->viewQuery = NULL;
					$$->skipData = false;		/* might get changed later */
				}
		;
