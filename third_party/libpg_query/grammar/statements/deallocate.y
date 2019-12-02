/*****************************************************************************
 *
 *		QUERY:
 *				DEALLOCATE [PREPARE] <plan_name>
 *
 *****************************************************************************/
DeallocateStmt: DEALLOCATE name
					{
						PGDeallocateStmt *n = makeNode(PGDeallocateStmt);
						n->name = $2;
						$$ = (PGNode *) n;
					}
				| DEALLOCATE PREPARE name
					{
						PGDeallocateStmt *n = makeNode(PGDeallocateStmt);
						n->name = $3;
						$$ = (PGNode *) n;
					}
				| DEALLOCATE ALL
					{
						PGDeallocateStmt *n = makeNode(PGDeallocateStmt);
						n->name = NULL;
						$$ = (PGNode *) n;
					}
				| DEALLOCATE PREPARE ALL
					{
						PGDeallocateStmt *n = makeNode(PGDeallocateStmt);
						n->name = NULL;
						$$ = (PGNode *) n;
					}
		;
