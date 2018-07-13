#include "lib/stringinfo.h"

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, "\"" nodelabel "\": {")

/* Write an integer field */
#define WRITE_INT_FIELD(fldname) \
	if (node->fldname != 0) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": %d, ", node->fldname); \
	}

#define WRITE_INT_VALUE(fldname, value) \
	if (value != 0) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": %d, ", value); \
	}

/* Write an unsigned integer field */
#define WRITE_UINT_FIELD(fldname) \
	if (node->fldname != 0) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": %u, ", node->fldname); \
	}

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	if (node->fldname != 0) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": %ld, ", node->fldname); \
	}

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	if (node->fldname != 0) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": \"%c\", ", node->fldname); \
	}

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname) \
	appendStringInfo(str, "\"" CppAsString(fldname) "\": %d, ", \
					 (int) node->fldname)

/* Write a float field */
#define WRITE_FLOAT_FIELD(fldname) \
	appendStringInfo(str, "\"" CppAsString(fldname) "\": %f, ", node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	if (node->fldname) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": %s, ", \
					 	booltostr(node->fldname)); \
	}

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	if (node->fldname != NULL) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": "); \
	 	_outToken(str, node->fldname); \
	 	appendStringInfo(str, ", "); \
	}

#define WRITE_STRING_VALUE(fldname, value) \
	if (true) { \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": "); \
	 	_outToken(str, value); \
	 	appendStringInfo(str, ", "); \
	}


#define booltostr(x)	((x) ? "true" : "false")

static void
removeTrailingDelimiter(StringInfo str)
{
	if (str->len >= 2 && str->data[str->len - 2] == ',' && str->data[str->len - 1] == ' ') {
		str->len -= 2;
		str->data[str->len] = '\0';
	} else if (str->len >= 1 && str->data[str->len - 1] == ',') {
		str->len -= 1;
		str->data[str->len] = '\0';
	}
}

static void
_outToken(StringInfo buf, const char *str)
{
	if (str == NULL)
	{
		appendStringInfoString(buf, "null");
		return;
	}

	// copied directly from https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/json.c#L2428
	const char *p;

	appendStringInfoCharMacro(buf, '"');
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '\b':
				appendStringInfoString(buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(buf, "\\f");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(buf, "\\\\");
				break;
			default:
				if ((unsigned char) *p < ' ')
					appendStringInfo(buf, "\\u%04x", (int) *p);
				else
					appendStringInfoCharMacro(buf, *p);
				break;
		}
	}
	appendStringInfoCharMacro(buf, '"');
}
