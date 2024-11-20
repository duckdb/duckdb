/* Property Graph Query (PGQ) is the main novelty in SQL:2023
 *
 * this parser is rather complete, but the following features
 * are not supported:
 * - simplified path syntax
 * - undirected edges (~) mostly a GQL feature anyway.
 *   Because SQL/PGQ does not have a way to even define such edges.
 * - ALTER PROPERTY GRAPH.
 *   In DuckPGQ property graphs are like views, so drop and recreate
 * - DEFAULT LABEL in CREATE PROPERTY GRAPH
 *   as DuckDB requires all element tables to have unique labels.
 * - the % wildcard while MATCH-ing elements, used for testing if an
 *   element has a label (DuckDB requires elements to have a label)
 *
 * there are also three language extensions:
 * - COST(var_name_ident) in a COLUMNS() list of GRAPH_TABLE:
 *   var_name_ident must be a path variable. This returns path cost.
 * - ELEMENT_ID(var_name_ident) in a COLUMNS() list of GRAPH_TABLE:
 *   var_name_ident must be a path variable. This returns path cost.
 * - LABEL global_label IN col LABEL label_list
 *   in CREATE PROPERTY GRAPH. In addition to global_label, which
 *   all elements in the table get, column 'col' contains an integer
 *   interpreted as bitmap. The element also has the x-th label of
 *   the label_list iff the x-th bit of that integer is set to 1.
 *   This allows flexible LABEL memberships from a single table.
 *
 * COST and ELEMENT_ID will be parsed as a_expr expressions in the column
 * list and we treat them differently in the transformation.
 *
 * The constraint that a label occurs only in one element table of a
 * property graph will be enforced in DuckPGQ to avoid having to scan
 * multiple tables for a labeltest.
 *
 * Not all parsed functionality may be implemented -- raising errors
 * in such cases is arranged in the semantic checks
 */

 /* Column identifier for SQL/PGQ --- names that can be column, table, etc names.
  */
 PGQ_IDENT:		IDENT									{ $$ = $1; }
 			| unreserved_keyword					{ $$ = pstrdup($1); }
 			| pgq_col_name_keyword						{ $$ = pstrdup($1); }
 		;

/* -------------------------------
 * DROP PROPERTY GRAPH Statement
 * -------------------------------
 */
DropPropertyGraphStmt:
		DROP PROPERTY GRAPH qualified_name opt_drop_behavior
			{
				PGDropPropertyGraphStmt *n = makeNode(PGDropPropertyGraphStmt);
				n->name = $4;
				n->behavior = $5;
				n->missing_ok = false;
				$$ = (PGNode *)n;
			}
        |
        DROP PROPERTY GRAPH IF_P EXISTS qualified_name opt_drop_behavior
        			{
        				PGDropPropertyGraphStmt *n = makeNode(PGDropPropertyGraphStmt);
        				n->name = $6;
        				n->behavior = $7;
        				n->missing_ok = true;
        				$$ = (PGNode *)n;
        			}
		;

/* -------------------------------
 * CREATE PROPERTY GRAPH Statement
 * -------------------------------
 */
VertexOrNode:
		VERTEX
	|
		NODE
		;

EdgeOrRelationship:
		EDGE
	|
		RELATIONSHIP
		;

EdgeTablesClauseOptional:
    EdgeOrRelationship TABLES '(' EdgeTableDefinition EdgeTableDefinitionList ')'   { $$ = $5?lappend($5,$4):list_make1($4); }
    |
    /* EMPTY */                                                                     { $$ = NULL; }


CreatePropertyGraphStmt:
		CREATE_P PROPERTY GRAPH qualified_name
		VertexOrNode TABLES '(' VertexTableDefinition VertexTableDefinitionList ')'
		EdgeTablesClauseOptional
			{
				PGCreatePropertyGraphStmt *n = makeNode(PGCreatePropertyGraphStmt);
				n->name = $4;
				n->vertex_tables = $9?lappend($9,$8):list_make1($8);
				n->edge_tables = $11;
				n->onconflict = PG_ERROR_ON_CONFLICT;
				$$ = (PGNode *)n;
			}
		;
		|
		CREATE_P OR REPLACE PROPERTY GRAPH qualified_name
        		VertexOrNode TABLES '(' VertexTableDefinition VertexTableDefinitionList ')'
        		EdgeTablesClauseOptional
        			{
        				PGCreatePropertyGraphStmt *n = makeNode(PGCreatePropertyGraphStmt);
        				n->name = $6;
        				n->vertex_tables = $11?lappend($11,$10):list_make1($10);
        				n->edge_tables = $13;
        				n->onconflict = PG_REPLACE_ON_CONFLICT;
        				$$ = (PGNode *)n;
        			}
        		;


VertexTableDefinitionList:
		',' VertexTableDefinition 
		VertexTableDefinitionList	{ $$ = $3?lappend($3,$2):list_make1($2); }
	|
		/* EMPTY */					{ $$ = NULL; }
        ;

KeySpecification:
		'(' name_list ')'			{ $$ = $2; }
		;

KeyReference:
		KEY KeySpecification REFERENCES qualified_name '(' name_list ')'
			{
				/* Case where both KEY (id) and REFERENCES (id) are provided */
				PGKeyReference *key_ref = makeNode(PGKeyReference);
				key_ref->key_columns = $2;
				key_ref->ref_table = $4;
				key_ref->ref_columns = $6;
				$$ = (PGNode *) key_ref;
			}
	|
		qualified_name
			{
				/* Case where neither KEY (id) nor REFERENCES (id) are provided */
				PGKeyReference *key_ref = makeNode(PGKeyReference);
				key_ref->key_columns = NULL;
				key_ref->ref_table = $1;
				key_ref->ref_columns = NULL;
				$$ = (PGNode *) key_ref;
			}
		;

LabelList:
        PGQ_IDENT                   { $$ = list_make1(makeString($1)); }
    |   LabelList ',' PGQ_IDENT     { $$ = lappend($1, makeString($3)); }
    ;

LabelOptional:
    LABEL PGQ_IDENT { $$ = $2; }
    | /* EMPTY */   { $$ = NULL; }
;

Discriminator:
		IN_P qualified_name '(' LabelList ')'			
			{ 
				PGPropertyGraphTable *n = makeNode(PGPropertyGraphTable);
				n->discriminator = $2; /* a BIGINT column with 64 bits to set detailed label membership */
				n->labels = $4; /* there is a list of up to 64 labels */
				$$ = (PGNode*) n;
			}
	|
		/* EMPTY */
			{ 
				PGPropertyGraphTable *n = makeNode(PGPropertyGraphTable);
				n->discriminator = NULL; /* no discriminator */
				n->labels = NULL; /* no list, just the single staring PGQ_IDENT */
				$$ = (PGNode*) n;
			}
        ;

VertexTableDefinition:
		/* qualified name is an BIGINT column with 64 bits: a maximum of 64 labels can be set */
		QualifiednameOptionalAs PropertiesClause LabelOptional Discriminator
			{
				PGPropertyGraphTable *n = (PGPropertyGraphTable*) $4;
				n->table = $1;
				n->properties = $2;
				/* Xth label in list is set iff discriminator Xth-bit==1 */
				if (n->labels) n->labels = lappend(n->labels,makeString($3));
				else n->labels = list_make1(makeString($3));
				n->is_vertex_table = true;
				$$ = (PGNode *) n;
			}
		;

EdgeTableDefinitionList:
		',' EdgeTableDefinition	EdgeTableDefinitionList 	
									{ $$ = $3?lappend($3,$2):list_make1($2); }
	|
		/* EMPTY */					{ $$ = NULL; }
		;

EdgeTableDefinition:
		QualifiednameOptionalAs
		SOURCE KeyReference
		DESTINATION KeyReference
		PropertiesClause LabelOptional Discriminator
			{
				PGPropertyGraphTable *n = (PGPropertyGraphTable*) $8;
				n->table = $1;
				n->is_vertex_table = false;
				PGKeyReference *src_key_ref = (PGKeyReference *) $3;
                n->src_fk = src_key_ref->key_columns;
                n->src_name = src_key_ref->ref_table;
                n->src_pk = src_key_ref->ref_columns;
                PGKeyReference *dst_key_ref = (PGKeyReference *) $5;
				n->dst_fk = dst_key_ref->key_columns;
				n->dst_name = dst_key_ref->ref_table;
				n->dst_pk = dst_key_ref->ref_columns;
				n->properties = $6;
				/* Handle labels and discriminator as before */
				if (n->labels) n->labels = lappend(n->labels, makeString($7));
				else n->labels = list_make1(makeString($7));
				$$ = (PGNode *) n;
			}
		;

AreOptional:
		ARE
	|
		/* EMPTY */
		;

IdentOptionalAs:
		PGQ_IDENT					    { $$ = list_make2(makeString($1), makeString($1)); }
	|
		PGQ_IDENT AS PGQ_IDENT			{ $$ = list_make2(makeString($1), makeString($3)); }
        ;

QualifiednameOptionalAs:
        qualified_name                  { $$ = list_make2($1, makeString("")); }
        |
        qualified_name AS PGQ_IDENT     { $$ = list_make2($1, makeString($3)); }
        ;

PropertiesList:
		IdentOptionalAs			{ $$ = list_make1($1); }
	|
		PropertiesList ','
		IdentOptionalAs			{ $$ = lappend($1, $3); }
	    ;

ExceptOptional:
		EXCEPT '(' PropertiesList ')'
								{ $$ = $3; }
	|
		/* EMPTY */				{ $$ = NULL; }
		;

PropertiesSpec:
		AreOptional ALL COLUMNS ExceptOptional
			{ 
				$$ = list_make1(list_make2(makeString("*"), makeString("*")));
				if ($4) $$ = list_concat($$,$4); 
			}
	|
		'(' PropertiesList ')'	{ $$ = $2; }
	    ;

PropertiesClause:
		NO PROPERTIES			{ $$ = NULL; }
	|
		PROPERTIES PropertiesSpec
								{ $$ = $2; }
	|
		/* EMPTY */				{ $$ = list_make1(list_make2(makeString("*"), makeString("*"))); }
        ;

/* ------------------------------
 * GRAPH_TABLE clause (aka MATCH)
 * ------------------------------
 */

GraphTableWhereOptional:
		WHERE pgq_expr 			{ $$ = $2; }
	|
		/* EMPTY */				{ $$ = NULL; }
		;

GraphTableNameOptional:
        qualified_name          { $$ = $1; }
    |
        /* EMPTY */				{ $$ = NULL; }
        ;

ColumnsOptional:
        COLUMNS '(' target_list_opt_comma ')' { $$ = $3; }
    |
        /* EMPTY */
            {
                PGAStar *star = makeNode(PGAStar);
                $$ = list_make1(star);
            }
    ;


GraphTableStmt:
		'(' PGQ_IDENT MATCH PathPatternList KeepOptional GraphTableWhereOptional
		ColumnsOptional ')' GraphTableNameOptional
			{
				PGMatchClause *n = makeNode(PGMatchClause);
				n->pg_name = $2;
				n->paths = $4;
				if ($5) {
					/* we massage away 'keep' functionality immediately */
					PGPathPattern *keep = (PGPathPattern*) $5;
					PGListCell *list = list_head(n->paths);
					while(list) {
						PGPathPattern *p = (PGPathPattern*) lfirst(list);
						PGList *backup = p->path;
						*p = *keep; /* copy path spec into all paths */
						p->path = backup; /* restore */
						list = lnext(list);
					}
				}
				n->where_clause = $6;
				n->columns = $7;
				n->graph_table = $9;
				$$ = (PGNode *) n;
			}
		;

KeepOptional:
		KEEP PathPrefix				{ $$ = $2; }
		|
		/* EMPTY */					{ $$ = NULL; }
		;

PathOrPathsOptional:
		PATH | PATHS | /* EMPTY */
		;

GroupOrGroupsOptional:
		GROUP_P 					{ $$ = 1; }
	|
		GROUPS						{ $$ = 1; }
	|
		/* EMPTY */					{ $$ = 0; }
		;

PathVariableOptional:
		PGQ_IDENT '='					{ $$ = $1; }
	|
		/* EMPTY */					{ $$ = NULL;}
	    ;

PathModeOptional:
		WALK PathOrPathsOptional  	{ $$ = PG_PATHMODE_WALK; }
	|
		TRAIL PathOrPathsOptional	{ $$ = PG_PATHMODE_TRAIL; }
	|
		SIMPLE PathOrPathsOptional	{ $$ = PG_PATHMODE_SIMPLE; }
	|
		ACYCLIC PathOrPathsOptional	{ $$ = PG_PATHMODE_ACYCLIC; }
	|
		PathOrPathsOptional			{ $$ = PG_PATHMODE_WALK; }
	    ;

TopKOptional:
		ICONST						{ $$ = $1; }
	|
		/* EMPTY */					{ $$ = 0; }
		;

PathPrefix:
		ANY SHORTEST PathModeOptional
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = false;
				n->group = false;
				n->shortest = true;
				n->mode = (PGPathMode) $3;
				n->topk = 1;
				$$ = (PGNode*) n;
			}
	|
		SHORTEST ICONST PathModeOptional GroupOrGroupsOptional
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = false;
				n->group = $4;
				n->shortest = true;
				n->mode = (PGPathMode) $3;
				n->topk = $2;
				$$ = (PGNode*) n;
			}
	|
		ALL SHORTEST PathModeOptional
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = true;
				n->group = false;
				n->shortest = true;
				n->mode = (PGPathMode) $3;
				n->topk = 0;
				$$ = (PGNode*) n;
			}
	|
		ALL PathModeOptional
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = true;
				n->group = false;
				n->shortest = false;
				n->mode = (PGPathMode) $2;
				n->topk = 0;
				$$ = (PGNode*) n;
			}
	|
		ANY TopKOptional PathModeOptional
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = false;
				n->group = false;
				n->shortest = false;
				n->mode = (PGPathMode) $3;
				n->topk = $2;
				$$ = (PGNode*) n;
			}
	|
		/* EMPTY */
			{
				PGPathPattern *n = makeNode(PGPathPattern);
				n->path = NULL;
				n->all = true;
				n->group = false;
				n->shortest = false;
				n->mode = PG_PATHMODE_WALK;
				n->topk = 0;
				$$ = (PGNode*) n;
			}
		;


PathPatternList:
		PathPattern					{ $$ = list_make1($1); }
	|
		PathPatternList ','
		PathPattern					{ $$ = lappend($1, $3); }
		;

PathPattern:
    PathVariableOptional PathPrefix PathConcatenation
    {
        PGPathPattern *n = (PGPathPattern*) $2;
        PGList *l = (PGList *) $3;

        /* Check if the list is not empty and retrieve the first element */
        if (l != NULL && list_length(l) > 0) {
            PGNode *node = (PGNode *) lfirst(list_head(l));

            $$ = (PGNode*) n;

            /* Check if the node is a PGSubPath and not NULL */
            if ($1 == NULL) {
                n->path = $3;
            } else if (list_length(l) == 1 && node != NULL && node->type == T_PGSubPath && !((PGSubPath*)node)->path_var) {
                PGSubPath *p = (PGSubPath*) node;
                p->path_var = $1;
                $$ = (PGNode*) p;
            }
            /* If the node is not a PGSubPath or the node is NULL, create a new subpath */
            else {
                PGSubPath *p = makeNode(PGSubPath);
                p->mode = n->mode;
                p->lower = p->upper = p->single_bind = 1;
                p->path_var = $1;
                p->path = $3;
                n->path = list_make1(p);
            }
        } else {
            /* Handle the case where the list is NULL or empty */
            $$ = (PGNode*) n; /* Or appropriate fallback */
        }
    }
;

PatternUnion:
		'|'							{ $$ = 0; }
	|
		'|' '+' '|'					{ $$ = 1; }
		;

KleeneQuantifierOptional:
		ICONST						{ $$ = $1; }
	|
		/* EMPTY */					{ $$ = -1; }
		;


KleeneOptional:
		'*'
			{
				PGSubPath *n = makeNode(PGSubPath);
				n->single_bind = 0;
				n->lower = 0;
				n->upper = (1<<30);
				$$ = (PGNode*) n;
			}
	|
		'+'
			{
				PGSubPath *n = makeNode(PGSubPath);
				n->single_bind = 0;
				n->lower = 1;
				n->upper = (1<<30);
				$$ = (PGNode*) n;
			}
	|
		'?'
			{
				PGSubPath *n = makeNode(PGSubPath);
				n->single_bind = 1;
				n->lower = 0;
				n->upper = 1;
				$$ = (PGNode*) n;
			}
	|
		'{' KleeneQuantifierOptional ',' KleeneQuantifierOptional '}'
			{
				PGSubPath *n = makeNode(PGSubPath);
				n->single_bind = 0;
				n->lower = ($2>=0)?$2:0;
				n->upper = ($4>=0)?$4:(1<<30);
				$$ = (PGNode*) n;
			}
	|
		/* EMPTY */
			{
				PGSubPath *n = makeNode(PGSubPath);
				n->single_bind = 1;
				n->lower = 1;
				n->upper = 1;
				$$ = (PGNode*) n;
			}
    ;

CostNum:
		ICONST						{ $$ = $1; }
	|
		FCONST						{ $$ = atof($1); }
		;

CostDefault:
		DEFAULT CostNum				{ $$ = $2; }
	|
		/* EMPTY */ 				{ $$ = NULL; }
		;

CostOptional:
		COST b_expr CostDefault 
			{
				PGPathInfo *n = makeNode(PGPathInfo);
				PGAConst *d = (PGAConst*) $3;
				n->cost_expr = $2;
				n->default_value = d?((d->val.type == T_PGInteger)?
					((double) d->val.val.ival):strtod(d->val.val.str,NULL)):1;
				$$ = (PGNode*) n;
			}
	|
		/* EMPTY */
			{
				PGPathInfo *n = makeNode(PGPathInfo);
				n->cost_expr = NULL;
				n->default_value = 1;
				$$ = (PGNode*) n;
			}
		;

SubPath:
		PathVariableOptional PathModeOptional PathConcatenation GraphTableWhereOptional  CostOptional
			{
				PGPathInfo *n = (PGPathInfo*) $5;
				n->var_name = $1;
				n->mode = (PGPathMode) $2;
				n->path = $3;
				n->where_clause = $4;
				$$ = (PGNode*) n;
			}
		;

EnclosedSubPath:
		'[' SubPath ']' KleeneOptional
			{
				PGSubPath *p = (PGSubPath*) $4;
				p->path = list_make1($2);
				$$ = (PGNode*) p;
			}
		;

PathElement:
		VertexPattern 				{ $$ = $1; }
	|
		EdgePattern					{ $$ = $1; }
	;

PathSequence:
		EnclosedSubPath PathSequence	
			{
				PGSubPath *n = (PGSubPath*) $1;
				PGPathInfo *i = (PGPathInfo*) n->path;
				PGList* p = (PGList*) i->path;

				if (i->var_name == NULL && i->mode <= PG_PATHMODE_WALK &&
					i->where_clause == NULL && i->cost_expr == NULL)
				{
					/* there is no need for a SubPath */
					$$ = $2?list_concat(p,$2):p;
				} else {
        			n->path_var = i->var_name;
					n->mode = i->mode;
					n->path = p;
					n->where_clause = i->where_clause;
					n->cost_expr = i->cost_expr;
					n->default_value = i->default_value;
					$$ = list_make1(n);
					if ($2) $$ = list_concat($$,$2);
				}
			}
	|
		PathElement PathSequence	{ $$ = $1?list_concat($1,$2):$2; }
	|
		/* EMPTY*/					{ $$ = NULL; }
		;

PathConcatenation:
		PathSequence 				{ $$ = $1; }
	|
		PathSequence PatternUnion PathSequence 
			{
				PGPathUnion *n = makeNode(PGPathUnion);
				n->multiset = $2;
				n->path1 = $1;
				n->path2 = $3;
				$$ = list_make1(n);
			}
	    ;

OrLabelExpression:
		LabelExpression { $$ = $1; }
	|
		LabelExpression '|' OrLabelExpression 
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = "|";
				n->left = (PGLabelTest*) $1;
				n->right = (PGLabelTest*) $3;
				$$ = (PGNode*) n;
			}
	    ;

AndLabelExpression:
		LabelExpression { $$ = $1; }
	|
		LabelExpression '&' AndLabelExpression 
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = "|";
				n->left = (PGLabelTest*) $1;
				n->right = (PGLabelTest*) $3;
				$$ = (PGNode*) n;
			}
	    ;

ComposedLabelExpression:
		LabelExpression	{ $$ = $1; }
	|
		LabelExpression '|' OrLabelExpression 
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = "|";
				n->left = (PGLabelTest*) $1;
				n->right = (PGLabelTest*) $3;
				$$ = (PGNode*) n;
			}
	|
		LabelExpression '&' AndLabelExpression 
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = "&";
				n->left = (PGLabelTest*) $1;
				n->right = (PGLabelTest*) $3;
				$$ = (PGNode*) n;
			}
		;

LabelExpression:
		PGQ_IDENT
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = $1;
				n->left = n->right = NULL;
				$$ = (PGNode*) n;
			}
	|
		'!' LabelExpression
			{
				PGLabelTest *n = makeNode(PGLabelTest);
				n->name = "!";
				n->left = (PGLabelTest*) $2;
				n->right = NULL;
				$$ = (PGNode*) n;
			}
	|
		'(' ComposedLabelExpression ')' { $$ = $2; }
		;

LabelExpressionOptional:
		IsOrColon LabelExpression 	{ $$ = $2; }
	|
		/* EMPTY */					{ $$ = NULL; }
        ;

IsOrColon:
		IS
	|
		':'
        ;

VariableOptional:
		PGQ_IDENT 						{ $$ = $1; }
	|
		/* EMPTY */					{ $$ = NULL;}
	    ;

FullElementSpec:
        VariableOptional LabelExpressionOptional GraphTableWhereOptional CostOptional
			{
				PGPathInfo *n = (PGPathInfo*) $4;
				n->var_name = $1;
				n->where_clause = $3;
				n->label_expr = (PGLabelTest*) $2;
				$$ = (PGNode*) n;
			}
		;

/* we allow spaces inside the arrows */
Arrow:
        '-'
            {   $$ = "-"; }
    |
        '<' '-'
            {   $$ = "<-";  }
    |
        LAMBDA_ARROW
            {   $$ = "->"; }
    |
        '-' '>'
            {   $$ = "->"; }
    |
        '<' LAMBDA_ARROW
            {    $$ = "<->";  }
    |
        '<' '-' '>'
            {    $$ = "<->";  }
    |
        Op
            {   /* DDB lexer may concatenate an arrow with + or * into an "operator" */
                char *op = $1, *ok = NULL;
                /* only <-, <->, -, -> are ok */
                if (op[0] == '<') op++; /* also accept <-> */
                if (op[0] == '-') {
                    ok = op + 1  + (op[1] == '>');
                }
                /* it may optionally be followed by a single * or + */
                if (!ok || (ok[0] && ((ok[0] != '*' && ok[0] != '+') || ok[1]))) {
                    char msg[128];
                    snprintf(msg, 128, "PGQ expected an arrow instead of %s operator.", $1);
                    parser_yyerror(msg);
                }
                $$ = $1;
            }
        ;

ArrowLeft:
        '-' '['
            {   $$ = "-"; }
    |
        '<' '-' '['
            {   $$ = "<-";  }
        ;

ArrowKleeneOptional:
        Arrow KleeneOptional
            {
                PGSubPath *p = (PGSubPath*) $2;
                char *op = $1;
                int len = strlen(op);
                int plus = (op[len-1] == '+');
                int star = (op[len-1] == '*');
                if (plus || star) { /* + or * was glued to the end of the arrow */
                    if (!p->single_bind || p->lower != 1 || p-> upper != 1) {
                        parser_yyerror("PGQ cannot accept + or * followed by another quantifier.");
                    } else {
                        p->single_bind = 0;
                        p->lower = plus;
                        p->upper = (1<<30);
                    }
                }
                p->path = (PGList*) op; /* return the arrow temporarily in 'path'.. */
                $$ = (PGNode*) p;
            }
        ;

EdgePattern:
        ArrowLeft FullElementSpec ']' ArrowKleeneOptional
            {
                PGSubPath *p = (PGSubPath*) $4;
                char *left = $1;
                char *dash = (char*) p->path;
                PGPathInfo* i = (PGPathInfo*) $2;
                PGPathElement *n = makeNode(PGPathElement);
                if (dash[0] == '<') { /* ArrowKleeneOptional accepts <- but that is not ok here */
                    parser_yyerror("PGQ cannot accept < after ] edge pattern closing.");
                }
                n->match_type = (dash[1] == '>')?
                                    ((left[0] == '<')?PG_MATCH_EDGE_LEFT_RIGHT:PG_MATCH_EDGE_RIGHT):
                                    ((left[0] == '<')?PG_MATCH_EDGE_LEFT:PG_MATCH_EDGE_ANY);
                n->element_var = i->var_name;
                n->label_expr = i->label_expr;
                $$ = list_make1(n);
                if (i->where_clause || i->cost_expr || p->lower != 1 || !p->single_bind) {
                    /* return a subpath consisting of one edge (element) */
                    p->where_clause = i->where_clause;
                    p->cost_expr = i->cost_expr;
                    p->default_value = i->default_value;
                    p->path = $$;
                    p->path_var = NULL;
                    $$ = list_make1(p);
                }
            }
    |
        ArrowKleeneOptional
            {
                PGSubPath *p = (PGSubPath*) $1;
                char *left = (char*) p->path;
                PGPathElement *n = makeNode(PGPathElement);;
                char *dash = left + (left[0] == '<');
                n->label_expr = NULL;
                n->element_var = NULL;
                n->match_type = (dash[1] == '>')?
                                   ((left[0] == '<')?PG_MATCH_EDGE_LEFT_RIGHT:PG_MATCH_EDGE_RIGHT):
                                   ((left[0] == '<')?PG_MATCH_EDGE_LEFT:PG_MATCH_EDGE_ANY);
                $$ = list_make1(n);
                if (p->lower != 1 || !p->single_bind) {
                    /* return a subpath consisting of one edge (element) */
                    p->path = $$;
                    p->path_var = NULL;
                    $$ = list_make1(p);
                }
            }
        ;

VertexPattern:
        '(' FullElementSpec')'
			{
				PGPathElement *n = makeNode(PGPathElement);
				PGPathInfo* i = (PGPathInfo*) $2;

				n->element_var = i->var_name;
				n->label_expr = i->label_expr;
				n->match_type = PG_MATCH_VERTEX;
				$$ = list_make1(n);
				if (i->where_clause || i->cost_expr) {
					PGSubPath *p = makeNode(PGSubPath);
					p->mode = PG_PATHMODE_NONE;
					p->lower = p->upper = p->single_bind = 1;
					p->where_clause = i->where_clause;
					p->cost_expr = i->cost_expr;
					p->default_value = i->default_value;
					p->path = $$;
					p->path_var = NULL;
					$$ = list_make1(p);
				}
			}
		;

/* a copy of a_expr from select.y, but removing:
 * 2282                         | qual_Op b_expr                                        %prec Op
 * 2284                         | b_expr qual_Op                                        %prec POSTFIXOP
 * 2186                         | a_expr subquery_Op sub_type select_with_parens        %prec Op
 * 2197  						| pgq_expr subquery_Op sub_type '(' pgq_expr ')'		%prec Op
 * 2204                         | DEFAULT
 * 2218                         | COLUMNS '(' '*' opt_except_list opt_replace_list ')'
 * 2227                         | COLUMNS '(' Sconst ')'
 * and replacing a_expr into pgq_expr
 */
pgq_expr:		c_expr									{ $$ = $1; }
			| pgq_expr TYPECAST Typename
					{ $$ = makeTypeCast($1, $3, 0, @2); }
			| pgq_expr COLLATE any_name
				{
					PGCollateClause *n = makeNode(PGCollateClause);
					n->arg = $1;
					n->collname = $3;
					n->location = @2;
					$$ = (PGNode *) n;
				}
			| pgq_expr AT TIME ZONE pgq_expr			%prec AT
				{
					$$ = (PGNode *) makeFuncCall(SystemFuncName("timezone"),
											   list_make2($5, $1),
											   @2);
				}
		/*
		 * These operators must be called out explicitly in order to make use
		 * of bison's automatic operator-precedence handling.  All other
		 * operator names are handled by the generic productions using "Op",
		 * below; and all those operators will have the same precedence.
		 *
		 * If you add more explicitly-known operators, be sure to add them
		 * also to b_expr and to the MathOp list below.
		 */
			| '+' pgq_expr					%prec UMINUS
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "+", NULL, $2, @1); }
			| '-' pgq_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| pgq_expr '+' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "+", $1, $3, @2); }
			| pgq_expr '-' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "-", $1, $3, @2); }
			| pgq_expr '*' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "*", $1, $3, @2); }
			| pgq_expr '/' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "/", $1, $3, @2); }
			| pgq_expr '%' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "%", $1, $3, @2); }
			| pgq_expr '^' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "^", $1, $3, @2); }
			| pgq_expr POWER_OF pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "**", $1, $3, @2); }
			| pgq_expr '<' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "<", $1, $3, @2); }
			| pgq_expr '>' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, ">", $1, $3, @2); }
			| pgq_expr '=' pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "=", $1, $3, @2); }
			| pgq_expr LESS_EQUALS pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "<=", $1, $3, @2); }
			| pgq_expr GREATER_EQUALS pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, ">=", $1, $3, @2); }
			| pgq_expr NOT_EQUALS pgq_expr
				{ $$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "<>", $1, $3, @2); }

			| pgq_expr qual_Op pgq_expr				%prec Op
				{ $$ = (PGNode *) makeAExpr(PG_AEXPR_OP, $2, $1, $3, @2); }
			| pgq_expr AND pgq_expr
				{ $$ = makeAndExpr($1, $3, @2); }
			| pgq_expr OR pgq_expr
				{ $$ = makeOrExpr($1, $3, @2); }
			| NOT pgq_expr
				{ $$ = makeNotExpr($2, @1); }
			| NOT_LA pgq_expr						%prec NOT
				{ $$ = makeNotExpr($2, @1); }
			| pgq_expr GLOB pgq_expr %prec GLOB
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_GLOB, "~~~",
												   $1, $3, @2);
				}
			| pgq_expr LIKE pgq_expr
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_LIKE, "~~",
												   $1, $3, @2);
				}
			| pgq_expr LIKE pgq_expr ESCAPE pgq_expr					%prec LIKE
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make3($1, $3, $5),
											   @2);
					$$ = (PGNode *) n;
				}
			| pgq_expr NOT_LA LIKE pgq_expr							%prec NOT_LA
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_LIKE, "!~~",
												   $1, $4, @2);
				}
			| pgq_expr NOT_LA LIKE pgq_expr ESCAPE pgq_expr			%prec NOT_LA
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("not_like_escape"),
											   list_make3($1, $4, $6),
											   @2);
					$$ = (PGNode *) n;
				}
			| pgq_expr ILIKE pgq_expr
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_ILIKE, "~~*",
												   $1, $3, @2);
				}
			| pgq_expr ILIKE pgq_expr ESCAPE pgq_expr					%prec ILIKE
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("ilike_escape"),
											   list_make3($1, $3, $5),
											   @2);
					$$ = (PGNode *) n;
				}
			| pgq_expr NOT_LA ILIKE pgq_expr						%prec NOT_LA
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_ILIKE, "!~~*",
												   $1, $4, @2);
				}
			| pgq_expr NOT_LA ILIKE pgq_expr ESCAPE pgq_expr			%prec NOT_LA
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("not_ilike_escape"),
											   list_make3($1, $4, $6),
											   @2);
					$$ = (PGNode *) n;
				}

			| pgq_expr SIMILAR TO pgq_expr							%prec SIMILAR
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, makeNullAConst(-1)),
											   @2);
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_SIMILAR, "~",
												   $1, (PGNode *) n, @2);
				}
			| pgq_expr SIMILAR TO pgq_expr ESCAPE pgq_expr			%prec SIMILAR
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_SIMILAR, "~",
												   $1, (PGNode *) n, @2);
				}
			| pgq_expr NOT_LA SIMILAR TO pgq_expr					%prec NOT_LA
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, makeNullAConst(-1)),
											   @2);
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_SIMILAR, "!~",
												   $1, (PGNode *) n, @2);
				}
			| pgq_expr NOT_LA SIMILAR TO pgq_expr ESCAPE pgq_expr		%prec NOT_LA
				{
					PGFuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, $7),
											   @2);
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_SIMILAR, "!~",
												   $1, (PGNode *) n, @2);
				}

			/* PGNullTest clause
			 * Define SQL-style Null test clause.
			 * Allow two forms described in the standard:
			 *	a IS NULL
			 *	a IS NOT NULL
			 * Allow two SQL extensions
			 *	a ISNULL
			 *	a NOTNULL
			 */
			| pgq_expr IS NULL_P							%prec IS
				{
					PGNullTest *n = makeNode(PGNullTest);
					n->arg = (PGExpr *) $1;
					n->nulltesttype = PG_IS_NULL;
					n->location = @2;
					$$ = (PGNode *)n;
				}
			| pgq_expr ISNULL
				{
					PGNullTest *n = makeNode(PGNullTest);
					n->arg = (PGExpr *) $1;
					n->nulltesttype = PG_IS_NULL;
					n->location = @2;
					$$ = (PGNode *)n;
				}
			| pgq_expr IS NOT NULL_P						%prec IS
				{
					PGNullTest *n = makeNode(PGNullTest);
					n->arg = (PGExpr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (PGNode *)n;
				}
			| pgq_expr NOT NULL_P
				{
					PGNullTest *n = makeNode(PGNullTest);
					n->arg = (PGExpr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (PGNode *)n;
				}
			| pgq_expr NOTNULL
				{
					PGNullTest *n = makeNode(PGNullTest);
					n->arg = (PGExpr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (PGNode *)n;
				}
			| pgq_expr LAMBDA_ARROW pgq_expr
			{
				PGLambdaFunction *n = makeNode(PGLambdaFunction);
				n->lhs = $1;
				n->rhs = $3;
				n->location = @2;
				$$ = (PGNode *) n;
			}
			| pgq_expr DOUBLE_ARROW pgq_expr %prec Op
			{
							$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "->>", $1, $3, @2);
			}
			| row OVERLAPS row
				{
					if (list_length($1) != 2)
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on left side of OVERLAPS expression"),
								 parser_errposition(@1)));
					if (list_length($3) != 2)
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on right side of OVERLAPS expression"),
								 parser_errposition(@3)));
					$$ = (PGNode *) makeFuncCall(SystemFuncName("overlaps"),
											   list_concat($1, $3),
											   @2);
				}
			| pgq_expr IS TRUE_P							%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = PG_IS_TRUE;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS NOT TRUE_P						%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS FALSE_P							%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = IS_FALSE;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS NOT FALSE_P						%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS UNKNOWN							%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = IS_UNKNOWN;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS NOT UNKNOWN						%prec IS
				{
					PGBooleanTest *b = makeNode(PGBooleanTest);
					b->arg = (PGExpr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					b->location = @2;
					$$ = (PGNode *)b;
				}
			| pgq_expr IS DISTINCT FROM pgq_expr			%prec IS
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| pgq_expr IS NOT DISTINCT FROM pgq_expr		%prec IS
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| pgq_expr IS OF '(' type_list ')'			%prec IS
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OF, "=", $1, (PGNode *) $5, @2);
				}
			| pgq_expr IS NOT OF '(' type_list ')'		%prec IS
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_OF, "<>", $1, (PGNode *) $6, @2);
				}
			| pgq_expr BETWEEN opt_asymmetric b_expr AND pgq_expr		%prec BETWEEN
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_BETWEEN,
												   "BETWEEN",
												   $1,
												   (PGNode *) list_make2($4, $6),
												   @2);
				}
			| pgq_expr NOT_LA BETWEEN opt_asymmetric b_expr AND pgq_expr %prec NOT_LA
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_NOT_BETWEEN,
												   "NOT BETWEEN",
												   $1,
												   (PGNode *) list_make2($5, $7),
												   @2);
				}
			| pgq_expr BETWEEN SYMMETRIC b_expr AND pgq_expr			%prec BETWEEN
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_BETWEEN_SYM,
												   "BETWEEN SYMMETRIC",
												   $1,
												   (PGNode *) list_make2($4, $6),
												   @2);
				}
			| pgq_expr NOT_LA BETWEEN SYMMETRIC b_expr AND pgq_expr		%prec NOT_LA
				{
					$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_NOT_BETWEEN_SYM,
												   "NOT BETWEEN SYMMETRIC",
												   $1,
												   (PGNode *) list_make2($5, $7),
												   @2);
				}
			| pgq_expr IN_P in_expr
				{
					/* in_expr returns a PGSubLink or a list of pgq_exprs */
					if (IsA($3, PGSubLink))
					{
						/* generate foo = ANY (subquery) */
						PGSubLink *n = (PGSubLink *) $3;
						n->subLinkType = PG_ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						$$ = (PGNode *)n;
					}
					else
					{
						/* generate scalar IN expression */
						$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| pgq_expr NOT_LA IN_P in_expr						%prec NOT_LA
				{
					/* in_expr returns a PGSubLink or a list of pgq_exprs */
					if (IsA($4, PGSubLink))
					{
						/* generate NOT (foo = ANY (subquery)) */
						/* Make an = ANY node */
						PGSubLink *n = (PGSubLink *) $4;
						n->subLinkType = PG_ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						/* Stick a NOT on top; must have same parse location */
						$$ = makeNotExpr((PGNode *) n, @2);
					}
					else
					{
						/* generate scalar NOT IN expression */
						$$ = (PGNode *) makeSimpleAExpr(PG_AEXPR_IN, "<>", $1, $4, @2);
					}
				}
		;
