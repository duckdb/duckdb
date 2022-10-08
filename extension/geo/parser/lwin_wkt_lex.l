%top{
/* The lexer */

#include <stdio.h>
#include <string.h>
#include "liblwgeom/lwin_wkt.hpp"
#include "parser/lwin_wkt_parse.hpp"
}
%{

static YY_BUFFER_STATE wkt_yy_buf_state;

/*
* Handle errors due to unexpected junk in WKT strings.
*/
static void wkt_lexer_unknown()
{
	/* Set the global error state */
	global_parser_result.errcode = PARSER_ERROR_OTHER;
	global_parser_result.message = parser_error_messages[PARSER_ERROR_OTHER];
	global_parser_result.errlocation = wkt_yylloc.last_column;
}

/*
* This macro is magically run after a rule is found but before the main
* action is run. We use it to update the parse location information
* so we can report on where things fail. Also optionally to dump
* debugging info.
*/
#define YY_USER_ACTION do { \
	wkt_yylloc.first_line = wkt_yylloc.last_line = yylineno; \
	wkt_yylloc.first_column = wkt_yylloc.last_column; \
	wkt_yylloc.last_column += yyleng; \
	} while (0);


%}

%option prefix="wkt_yy"
%option nounput
%option noinput
%option never-interactive
%option noyywrap
/* Suppress the default implementations. */
%option noyyalloc noyyrealloc noyyfree

%%

-?(([0-9]+\.?)|([0-9]*\.?[0-9]+)([eE][-+]?[0-9]+)?)[ \,\)\t\n\r] {
	wkt_yylval.doublevalue = atof(wkt_yytext);
	yyless(wkt_yyleng-1);
	return DOUBLE_TOK;
	}

SRID=-?[0-9]+ {
	wkt_yylval.integervalue = wkt_lexer_read_srid(wkt_yytext);
	return SRID_TOK;
	}

GEOMETRYCOLLECTION { return COLLECTION_TOK; }
MULTISURFACE { return MSURFACE_TOK; }
MULTIPOLYGON { return MPOLYGON_TOK; }
MULTICURVE { return MCURVE_TOK; }
MULTILINESTRING { return MLINESTRING_TOK; }
MULTIPOINT { return MPOINT_TOK; }
CURVEPOLYGON { return CURVEPOLYGON_TOK; }
POLYGON { return POLYGON_TOK; }
COMPOUNDCURVE { return COMPOUNDCURVE_TOK; }
CIRCULARSTRING { return CIRCULARSTRING_TOK; }
LINESTRING { return LINESTRING_TOK; }
POLYHEDRALSURFACE { return POLYHEDRALSURFACE_TOK; }
TRIANGLE { return TRIANGLE_TOK; }
TIN { return TIN_TOK; }
POINT { return POINT_TOK; }
EMPTY { return EMPTY_TOK; }

Z|M|ZM {
	wkt_yylval.stringvalue = wkt_yytext;
	return DIMENSIONALITY_TOK;
	}

\( { return LBRACKET_TOK; }
\) { return RBRACKET_TOK; }
,  { return COMMA_TOK; }
\; { return SEMICOLON_TOK; }

[ \t\n\r]+ { /* ignore whitespace */ }

. { /* Error out and stop parsing on unknown/unexpected characters */
	wkt_lexer_unknown();
	yyterminate();
	}

%%

void *wkt_yyalloc (yy_size_t  size )
{
	return (void *) lwalloc( size );
}

void *wkt_yyrealloc  (void * ptr, yy_size_t  size )
{
	return (void *) lwrealloc( (char *) ptr, size );
}

void wkt_yyfree (void * ptr )
{
	lwfree( (char *) ptr );	/* see wkt_yyrealloc() for (char *) cast */
}

/*
* Set up the lexer!
*/
void wkt_lexer_init(char *src)
{
	yy_init_globals();
	wkt_yy_buf_state = wkt_yy_scan_string(src);
}

/*
* Clean up the lexer!
*/
void wkt_lexer_close()
{
	wkt_yy_delete_buffer(wkt_yy_buf_state);
}

