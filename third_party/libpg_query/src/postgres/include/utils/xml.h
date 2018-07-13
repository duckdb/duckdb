/*-------------------------------------------------------------------------
 *
 * xml.h
 *	  Declarations for XML data type support.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/xml.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XML_H
#define XML_H

#include "fmgr.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"

typedef struct varlena xmltype;

typedef enum
{
	XML_STANDALONE_YES,
	XML_STANDALONE_NO,
	XML_STANDALONE_NO_VALUE,
	XML_STANDALONE_OMITTED
}	XmlStandaloneType;

typedef enum
{
	XMLBINARY_BASE64,
	XMLBINARY_HEX
}	XmlBinaryType;

typedef enum
{
	PG_XML_STRICTNESS_LEGACY,	/* ignore errors unless function result
								 * indicates error condition */
	PG_XML_STRICTNESS_WELLFORMED,		/* ignore non-parser messages */
	PG_XML_STRICTNESS_ALL		/* report all notices/warnings/errors */
} PgXmlStrictness;

/* struct PgXmlErrorContext is private to xml.c */
typedef struct PgXmlErrorContext PgXmlErrorContext;

#define DatumGetXmlP(X)		((xmltype *) PG_DETOAST_DATUM(X))
#define XmlPGetDatum(X)		PointerGetDatum(X)

#define PG_GETARG_XML_P(n)	DatumGetXmlP(PG_GETARG_DATUM(n))
#define PG_RETURN_XML_P(x)	PG_RETURN_POINTER(x)

extern Datum xml_in(PG_FUNCTION_ARGS);
extern Datum xml_out(PG_FUNCTION_ARGS);
extern Datum xml_recv(PG_FUNCTION_ARGS);
extern Datum xml_send(PG_FUNCTION_ARGS);
extern Datum xmlcomment(PG_FUNCTION_ARGS);
extern Datum xmlconcat2(PG_FUNCTION_ARGS);
extern Datum texttoxml(PG_FUNCTION_ARGS);
extern Datum xmltotext(PG_FUNCTION_ARGS);
extern Datum xmlvalidate(PG_FUNCTION_ARGS);
extern Datum xpath(PG_FUNCTION_ARGS);
extern Datum xpath_exists(PG_FUNCTION_ARGS);
extern Datum xmlexists(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed_document(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed_content(PG_FUNCTION_ARGS);

extern Datum table_to_xml(PG_FUNCTION_ARGS);
extern Datum query_to_xml(PG_FUNCTION_ARGS);
extern Datum cursor_to_xml(PG_FUNCTION_ARGS);
extern Datum table_to_xmlschema(PG_FUNCTION_ARGS);
extern Datum query_to_xmlschema(PG_FUNCTION_ARGS);
extern Datum cursor_to_xmlschema(PG_FUNCTION_ARGS);
extern Datum table_to_xml_and_xmlschema(PG_FUNCTION_ARGS);
extern Datum query_to_xml_and_xmlschema(PG_FUNCTION_ARGS);

extern Datum schema_to_xml(PG_FUNCTION_ARGS);
extern Datum schema_to_xmlschema(PG_FUNCTION_ARGS);
extern Datum schema_to_xml_and_xmlschema(PG_FUNCTION_ARGS);

extern Datum database_to_xml(PG_FUNCTION_ARGS);
extern Datum database_to_xmlschema(PG_FUNCTION_ARGS);
extern Datum database_to_xml_and_xmlschema(PG_FUNCTION_ARGS);

extern void pg_xml_init_library(void);
extern PgXmlErrorContext *pg_xml_init(PgXmlStrictness strictness);
extern void pg_xml_done(PgXmlErrorContext *errcxt, bool isError);
extern bool pg_xml_error_occurred(PgXmlErrorContext *errcxt);
extern void xml_ereport(PgXmlErrorContext *errcxt, int level, int sqlcode,
			const char *msg);

extern xmltype *xmlconcat(List *args);
extern xmltype *xmlelement(XmlExprState *xmlExpr, ExprContext *econtext);
extern xmltype *xmlparse(text *data, XmlOptionType xmloption, bool preserve_whitespace);
extern xmltype *xmlpi(char *target, text *arg, bool arg_is_null, bool *result_is_null);
extern xmltype *xmlroot(xmltype *data, text *version, int standalone);
extern bool xml_is_document(xmltype *arg);
extern text *xmltotext_with_xmloption(xmltype *data, XmlOptionType xmloption_arg);
extern char *escape_xml(const char *str);

extern char *map_sql_identifier_to_xml_name(char *ident, bool fully_escaped, bool escape_period);
extern char *map_xml_name_to_sql_identifier(char *name);
extern char *map_sql_value_to_xml_value(Datum value, Oid type, bool xml_escape_strings);

extern int	xmlbinary;			/* XmlBinaryType, but int for guc enum */

extern int	xmloption;			/* XmlOptionType, but int for guc enum */

#endif   /* XML_H */
