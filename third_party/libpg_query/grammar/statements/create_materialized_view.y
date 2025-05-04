/*****************************************************************************
 *
 *		QUERY :
 *				CREATE MATERIALIZED VIEW materialized_view_name AS PGSelectStmt [ WITH [NO] DATA ]
 *
 *****************************************************************************/
CreateMaterializedViewStmt:
		CREATE_P MATERIALIZED VIEW create_as_target AS SelectStmt opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
                    ctas->query = $6;
                    ctas->into = $4;
                    ctas->relkind = PG_OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->onconflict = PG_ERROR_ON_CONFLICT;
					$4->skipData = !($7);
					$$ = (PGNode *) ctas;
				}
		| CREATE_P OR REPLACE MATERIALIZED VIEW create_as_target AS SelectStmt opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
                    ctas->query = $8;
                    ctas->into = $6;
                    ctas->relkind = PG_OBJECT_MATVIEW;
                    ctas->is_select_into = false;
                    ctas->onconflict = PG_REPLACE_ON_CONFLICT;
                    $6->skipData = !($9);
                    $$ = (PGNode *) ctas;
				}
		;
