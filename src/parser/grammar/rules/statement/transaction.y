/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/
TransactionStmt:
			ABORT_P opt_transaction opt_transaction_chain
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					n->chain = $3;
					$$ = (Node *)n;
				}
			| BEGIN_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					$$ = (Node *)n;
				}
			| START TRANSACTION
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction opt_transaction_chain
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					n->chain = $3;
					$$ = (Node *)n;
				}
			| END_P opt_transaction opt_transaction_chain
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					n->chain = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction opt_transaction_chain
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					n->chain = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->savepoint_name = $5;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->savepoint_name = $4;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

opt_transaction_chain:
			AND CHAIN		{ $$ = true; }
			| AND NO CHAIN	{ $$ = false; }
			| /* EMPTY */	{ $$ = false; }
		;
