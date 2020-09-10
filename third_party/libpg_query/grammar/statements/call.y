/*****************************************************************************
 *
 * CALL <proc_name> [(params, ...)]
 *
 *****************************************************************************/
CallStmt: CALL_P func_application
				{
					PGCallStmt *n = makeNode(PGCallStmt);
					n->func = $2;
					$$ = (PGNode *) n;
				}
		;
