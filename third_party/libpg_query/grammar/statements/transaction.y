TransactionStmt:
			ABORT_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (PGNode *)n;
				}
			| BEGIN_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_BEGIN;
					$$ = (PGNode *)n;
				}
			| START opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_START;
					$$ = (PGNode *)n;
				}
			| COMMIT opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (PGNode *)n;
				}
			| END_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (PGNode *)n;
				}
			| ROLLBACK opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (PGNode *)n;
				}
		;


opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;
