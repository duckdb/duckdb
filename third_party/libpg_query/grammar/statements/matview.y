/*****************************************************************************
 *
 *		QUERY :
 *				CREATE MATERIALIZED VIEW relname AS SelectStmt
 *
 *****************************************************************************/
CreateMatViewStmt:
		CREATE_P OptTemp MATERIALIZED VIEW create_as_target AS SelectStmt opt_with_data
				{
					PGCreateMatViewStmt *ctas = makeNode(PGCreateMatViewStmt);
					ctas->query = $7;
					ctas->into = $5;
					ctas->relkind = PG_OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->onconflict = PG_ERROR_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$5->rel->relpersistence = $2;
					$5->skipData = !($8);
					$$ = (PGNode *) ctas;
				}
		| CREATE_P OptTemp MATERIALIZED VIEW IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
				{
					PGCreateMatViewStmt *ctas = makeNode(PGCreateMatViewStmt);
					ctas->query = $10;
					ctas->into = $8;
					ctas->relkind = PG_OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->onconflict = PG_IGNORE_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$8->rel->relpersistence = $2;
					$8->skipData = !($11);
					$$ = (PGNode *) ctas;
				}
		| CREATE_P OR REPLACE OptTemp MATERIALIZED VIEW create_as_target AS SelectStmt opt_with_data
				{
					PGCreateMatViewStmt *ctas = makeNode(PGCreateMatViewStmt);
					ctas->query = $9;
					ctas->into = $7;
					ctas->relkind = PG_OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->onconflict = PG_REPLACE_ON_CONFLICT;
					/* cram additional flags into the PGIntoClause */
					$7->rel->relpersistence = $4;
					$7->skipData = !($10);
					$$ = (PGNode *) ctas;
				}
		;
