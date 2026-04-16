TransactionStmt:
			ABORT_P opt_transaction
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_ROLLBACK;
					n->options = NIL;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
			| BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_BEGIN;
					n->options = $3;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
					$$ = (PGNode *)n;
				}
			| START opt_transaction transaction_mode_list_or_empty
				{
					PGTransactionStmt *n = makeNode(PGTransactionStmt);
					n->kind = PG_TRANS_STMT_START;
					n->options = $3;
					n->transaction_type = PG_TRANS_TYPE_DEFAULT;
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

transaction_mode_item:
			ISOLATION LEVEL iso_level
				{ $$ = makeDefElem("transaction_isolation",
								   makeStringConst($3, @3), @1); }
			| READ_P ONLY
				{ $$ = makeDefElem("transaction_read_only",
								   makeIntConst(true, @1), @1); }
			| READ_P WRITE_P
				{ $$ = makeDefElem("transaction_read_only",
								   makeIntConst(false, @1), @1); }
			| DEFERRABLE
				{ $$ = makeDefElem("transaction_deferrable",
								   makeIntConst(true, @1), @1); }
			| NOT DEFERRABLE
				{ $$ = makeDefElem("transaction_deferrable",
								   makeIntConst(false, @1), @1); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
				{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
				{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
				{ $$ = lappend($1, $2); }
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /* EMPTY */
				{ $$ = NIL; }
		;

iso_level:
			  READ_P UNCOMMITTED					{ $$ = "read uncommitted"; }
			| READ_P COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ_P						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;
