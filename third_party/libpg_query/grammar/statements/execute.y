/*****************************************************************************
 *
 * EXECUTE <plan_name> [(params, ...)]
 * CREATE TABLE <name> AS EXECUTE <plan_name> [(params, ...)]
 *
 *****************************************************************************/
ExecuteStmt: EXECUTE name execute_param_clause
				{
					PGExecuteStmt *n = makeNode(PGExecuteStmt);
					n->name = $2;
					n->params = $3;
					$$ = (PGNode *) n;
				}
			| CREATE_P OptTemp TABLE create_as_target AS
				EXECUTE name execute_param_clause opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
					PGExecuteStmt *n = makeNode(PGExecuteStmt);
					n->name = $7;
					n->params = $8;
					ctas->query = (PGNode *) n;
					ctas->into = $4;
					ctas->relkind = PG_OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = false;
					/* cram additional flags into the PGIntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($9);
					$$ = (PGNode *) ctas;
				}
			| CREATE_P OptTemp TABLE IF_P NOT EXISTS create_as_target AS
				EXECUTE name execute_param_clause opt_with_data
				{
					PGCreateTableAsStmt *ctas = makeNode(PGCreateTableAsStmt);
					PGExecuteStmt *n = makeNode(PGExecuteStmt);
					n->name = $10;
					n->params = $11;
					ctas->query = (PGNode *) n;
					ctas->into = $7;
					ctas->relkind = PG_OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the PGIntoClause */
					$7->rel->relpersistence = $2;
					$7->skipData = !($12);
					$$ = (PGNode *) ctas;
				}
		;


execute_param_clause: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
					;
