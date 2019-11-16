/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS SelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/
CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($7);
					$$ = (Node *) ctas;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $9;
					ctas->into = $7;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the IntoClause */
					$7->rel->relpersistence = $2;
					$7->skipData = !($10);
					$$ = (Node *) ctas;
				}
		;


opt_with_data:
			WITH DATA_P								{ $$ = TRUE; }
			| WITH NO DATA_P						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = TRUE; }
		;


create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->viewQuery = NULL;
					$$->skipData = false;		/* might get changed later */
				}
		;
