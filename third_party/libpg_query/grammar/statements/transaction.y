TransactionStmt:
			ABORT_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_ROLLBACK;
					n->options = NIL;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
			| BEGIN_P opt_transaction opt_transaction_type
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_BEGIN;
					n->transaction_type = $3;
					$$ = (PGNode *)n;
				}
			| START opt_transaction opt_transaction_type
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_START;
					n->transaction_type = $3;
					$$ = (PGNode *)n;
				}
			| COMMIT opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_COMMIT;
					n->options = NIL;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
			| END_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_COMMIT;
					n->options = NIL;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
			| ROLLBACK opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_ROLLBACK;
					n->options = NIL;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
		;


opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

opt_transaction_type:
			  READ_P ONLY							{ $$ = PG_TRANS_TYPE_READ_ONLY; }
			| READ_P WRITE_P						{ $$ = PG_TRANS_TYPE_READ_WRITE; }
			| /*EMPTY*/								{ $$ = PG_TRANS_TYPE_DEFAULT; }
		;
