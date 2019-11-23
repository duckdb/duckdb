/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/
AlterSeqStmt:
			ALTER SEQUENCE qualified_name SeqOptList
				{
					PGAlterSeqStmt *n = makeNode(PGAlterSeqStmt);
					n->sequence = $3;
					n->options = $4;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
				{
					PGAlterSeqStmt *n = makeNode(PGAlterSeqStmt);
					n->sequence = $5;
					n->options = $6;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}

		;


SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;


opt_with:	WITH									{}
			| WITH_LA								{}
			| /*EMPTY*/								{}
		;


NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '+' FCONST						{ $$ = makeFloat($2); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;


SeqOptElem: AS SimpleTypename
				{
					$$ = makeDefElem("as", (PGNode *)$2, @1);
				}
			| CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (PGNode *)$2, @1);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (PGNode *)makeInteger(true), @1);
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (PGNode *)makeInteger(false), @1);
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (PGNode *)$3, @1);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (PGNode *)$2, @1);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (PGNode *)$2, @1);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (PGNode *)$3, @1);
				}
			| SEQUENCE NAME_P any_name
				{
					/* not documented, only used by pg_dump */
					$$ = makeDefElem("sequence_name", (PGNode *)$3, @1);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (PGNode *)$3, @1);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (PGNode *)$3, @1);
				}
		;


opt_by:		BY				{}
			| /* empty */	{}
	  ;


SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;
