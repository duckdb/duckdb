#include "pg_query_json.h"

#include "postgres.h"

#include <ctype.h>

#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"

static void _outNode(StringInfo str, const void *obj);

#include "pg_query_json_helper.c"

#define WRITE_NODE_FIELD(fldname) \
	if (true) { \
		 appendStringInfo(str, "\"" CppAsString(fldname) "\": "); \
	     _outNode(str, &node->fldname); \
		 appendStringInfo(str, ", "); \
  	}

#define WRITE_NODE_FIELD_WITH_TYPE(fldname, typename) \
	if (true) { \
		 appendStringInfo(str, "\"" CppAsString(fldname) "\": {"); \
	   	 _out##typename(str, (const typename *) &node->fldname); \
		 removeTrailingDelimiter(str); \
 		 appendStringInfo(str, "}}, "); \
	}

#define WRITE_NODE_PTR_FIELD(fldname) \
	if (node->fldname != NULL) { \
		 appendStringInfo(str, "\"" CppAsString(fldname) "\": "); \
		 _outNode(str, node->fldname); \
		 appendStringInfo(str, ", "); \
	}

#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfo(str, "\"" CppAsString(fldname) "\": "), \
	 _outBitmapset(str, node->fldname), \
	 appendStringInfo(str, ", "))


static void
_outList(StringInfo str, const List *node)
{
	const ListCell *lc;

	// Simple lists are frequent structures - we don't make them into full nodes to avoid super-verbose output
	appendStringInfoChar(str, '[');

	foreach(lc, node)
	{
		_outNode(str, lfirst(lc));

		if (lnext(lc))
			appendStringInfoString(str, ", ");
	}

	appendStringInfoChar(str, ']');
}

static void
_outIntList(StringInfo str, const List *node)
{
	const ListCell *lc;

	WRITE_NODE_TYPE("IntList");
	appendStringInfo(str, "\"items\": ");
	appendStringInfoChar(str, '[');

	foreach(lc, node)
	{
		appendStringInfo(str, " %d", lfirst_int(lc));

		if (lnext(lc))
			appendStringInfoString(str, ", ");
	}

	appendStringInfoChar(str, ']');
	appendStringInfo(str, ", ");
}

static void
_outOidList(StringInfo str, const List *node)
{
	const ListCell *lc;

	WRITE_NODE_TYPE("OidList");
	appendStringInfo(str, "\"items\": ");
	appendStringInfoChar(str, '[');

	foreach(lc, node)
	{
		appendStringInfo(str, " %u", lfirst_oid(lc));

		if (lnext(lc))
			appendStringInfoString(str, ", ");
	}

	appendStringInfoChar(str, ']');
	appendStringInfo(str, ", ");
}

static void
_outBitmapset(StringInfo str, const Bitmapset *bms)
{
	Bitmapset	*tmpset;
	int			x;

	appendStringInfoChar(str, '[');
	/*appendStringInfoChar(str, 'b');*/
	tmpset = bms_copy(bms);
	while ((x = bms_first_member(tmpset)) >= 0)
		appendStringInfo(str, "%d, ", x);
	bms_free(tmpset);
	removeTrailingDelimiter(str);
	appendStringInfoChar(str, ']');
}

static void
_outInteger(StringInfo str, const Value *node)
{
	WRITE_NODE_TYPE("Integer");
	appendStringInfo(str, "\"ival\": %ld, ", node->val.ival);
}

static void
_outFloat(StringInfo str, const Value *node)
{
	WRITE_NODE_TYPE("Float");
	appendStringInfo(str, "\"str\": ");
	_outToken(str, node->val.str);
	appendStringInfo(str, ", ");
}

static void
_outString(StringInfo str, const Value *node)
{
	WRITE_NODE_TYPE("String");
	appendStringInfo(str, "\"str\": ");
	_outToken(str, node->val.str);
	appendStringInfo(str, ", ");
}

static void
_outBitString(StringInfo str, const Value *node)
{
	WRITE_NODE_TYPE("BitString");
	appendStringInfo(str, "\"str\": ");
	_outToken(str, node->val.str);
	appendStringInfo(str, ", ");
}

static void
_outNull(StringInfo str, const Value *node)
{
	WRITE_NODE_TYPE("Null");
}

#include "pg_query_json_defs.c"

static void
_outNode(StringInfo str, const void *obj)
{
	if (obj == NULL)
	{
		appendStringInfoString(str, "null");
	}
	else if (IsA(obj, List))
	{
		_outList(str, obj);
	}
	else
	{
		appendStringInfoChar(str, '{');
		switch (nodeTag(obj))
		{
			case T_Integer:
				_outInteger(str, obj);
				break;
			case T_Float:
				_outFloat(str, obj);
				break;
			case T_String:
				_outString(str, obj);
				break;
			case T_BitString:
				_outBitString(str, obj);
				break;
			case T_Null:
				_outNull(str, obj);
				break;
			case T_IntList:
				_outIntList(str, obj);
				break;
			case T_OidList:
				_outOidList(str, obj);
				break;

			#include "pg_query_json_conds.c"

			default:
				elog(WARNING, "could not dump unrecognized node type: %d",
					 (int) nodeTag(obj));

				appendStringInfo(str, "}");
				return;
		}
		removeTrailingDelimiter(str);
		appendStringInfo(str, "}}");
	}
}

char *
pg_query_nodes_to_json(const void *obj)
{
	StringInfoData str;

	initStringInfo(&str);

	if (obj == NULL) /* Make sure we generate valid JSON for empty queries */
		appendStringInfoString(&str, "[]");
	else
		_outNode(&str, obj);

	return str.data;
}
