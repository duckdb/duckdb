// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
*
*   Copyright (C) 1999-2014, International Business Machines
*   Corporation and others.  All Rights Reserved.
*
*******************************************************************************
*   file name:  uniset_props.cpp
*   encoding:   UTF-8
*   tab size:   8 (not used)
*   indentation:4
*
*   created on: 2004aug25
*   created by: Markus W. Scherer
*
*   Character property dependent functions moved here from uniset.cpp
*/

#include "unicode/utypes.h"
#include "unicode/uniset.h"
#include "unicode/parsepos.h"
#include "unicode/uchar.h"
#include "unicode/uscript.h"
#include "unicode/symtable.h"
#include "unicode/uset.h"
#include "unicode/locid.h"
#include "unicode/brkiter.h"
#include "uset_imp.h"
#include "ruleiter.h"
#include "cmemory.h"
#include "ucln_cmn.h"
#include "util.h"
#include "uvector.h"
#include "uprops.h"
#include "propname.h"
#include "normalizer2impl.h"
#include "uinvchar.h"
#include "uprops.h"
#include "charstr.h"
#include "cstring.h"
#include "mutex.h"
#include "umutex.h"
#include "uassert.h"
#include "hash.h"

U_NAMESPACE_USE

// Define UChar constants using hex for EBCDIC compatibility
// Used #define to reduce private static exports and memory access time.
#define uniset_props_SET_OPEN        ((UChar)0x005B) /*[*/
#define uniset_props_SET_CLOSE       ((UChar)0x005D) /*]*/
#define uniset_props_HYPHEN          ((UChar)0x002D) /*-*/
#define uniset_props_COMPLEMENT      ((UChar)0x005E) /*^*/
#define uniset_props_COLON           ((UChar)0x003A) /*:*/
#define uniset_props_BACKSLASH       ((UChar)0x005C) /*\*/
#define uniset_props_INTERSECTION    ((UChar)0x0026) /*&*/
#define uniset_props_UPPER_U         ((UChar)0x0055) /*U*/
#define uniset_props_LOWER_U         ((UChar)0x0075) /*u*/
#define uniset_props_OPEN_BRACE      ((UChar)123)    /*{*/
#define uniset_props_CLOSE_BRACE     ((UChar)125)    /*}*/
#define uniset_props_UPPER_P         ((UChar)0x0050) /*P*/
#define uniset_props_LOWER_P         ((UChar)0x0070) /*p*/
#define uniset_props_UPPER_N         ((UChar)78)     /*N*/
#define uniset_props_EQUALS          ((UChar)0x003D) /*=*/

//static const UChar POSIX_OPEN[]  = { uniset_props_SET_OPEN,uniset_props_COLON,0 };  // "[:"
static const UChar uniset_props_POSIX_CLOSE[] = { uniset_props_COLON,uniset_props_SET_CLOSE,0 };  // ":]"
//static const UChar PERL_OPEN[]   = { uniset_props_BACKSLASH,uniset_props_LOWER_P,0 }; // "\\p"
//static const UChar PERL_CLOSE[]  = { uniset_props_CLOSE_BRACE,0 };    // "}"
//static const UChar NAME_OPEN[]   = { uniset_props_BACKSLASH,uniset_props_UPPER_N,0 };  // "\\N"
static const UChar uniset_props_HYPHEN_RIGHT_BRACE[] = {uniset_props_HYPHEN,uniset_props_SET_CLOSE,0}; /*-]*/

// Special property set IDs
static const char uniset_props_ANY[]   = "uniset_props_ANY";   // [\u0000-\U0010FFFF]
static const char uniset_props_ASCII[] = "uniset_props_ASCII"; // [\u0000-\u007F]
static const char uniset_props_ASSIGNED[] = "Assigned"; // [:^Cn:]

// Unicode name property alias
#define uniset_props_NAME_PROP "na"
#define uniset_props_NAME_PROP_LENGTH 2

/**
 * Delimiter string used in patterns to close a category reference:
 * ":]".  Example: "[:Lu:]".
 */
//static const UChar CATEGORY_CLOSE[] = {uniset_props_COLON, uniset_props_SET_CLOSE, 0x0000}; /* ":]" */

// Cached sets ------------------------------------------------------------- ***

U_CDECL_BEGIN
static UBool U_CALLCONV uset_cleanup();

static UnicodeSet *uni32Singleton;
static icu::UInitOnce uni32InitOnce = U_INITONCE_INITIALIZER;

/**
 * Cleanup function for UnicodeSet
 */
static UBool U_CALLCONV uset_cleanup(void) {
    delete uni32Singleton;
    uni32Singleton = NULL;
    uni32InitOnce.reset();
    return TRUE;
}

U_CDECL_END

U_NAMESPACE_BEGIN

namespace {

// Cache some sets for other services -------------------------------------- ***
void U_CALLCONV createUni32Set(UErrorCode &errorCode) {
    U_ASSERT(uni32Singleton == NULL);
    uni32Singleton = new UnicodeSet(UNICODE_STRING_SIMPLE("[:age=3.2:]"), errorCode);
    if(uni32Singleton==NULL) {
        errorCode=U_MEMORY_ALLOCATION_ERROR;
    } else {
        uni32Singleton->freeze();
    }
    ucln_common_registerCleanup(UCLN_COMMON_USET, uset_cleanup);
}

// helper functions for matching of pattern syntax pieces ------------------ ***
// these functions are parallel to the PERL_OPEN etc. strings above

// using these functions is not only faster than UnicodeString::compare() and
// caseCompare(), but they also make UnicodeSet work for simple patterns when
// no Unicode properties data is available - when caseCompare() fails

static inline UBool
isPerlOpen(const UnicodeString &pattern, int32_t pos) {
    UChar c;
    return pattern.charAt(pos)==uniset_props_BACKSLASH && ((c=pattern.charAt(pos+1))==uniset_props_LOWER_P || c==uniset_props_UPPER_P);
}

/*static inline UBool
isPerlClose(const UnicodeString &pattern, int32_t pos) {
    return pattern.charAt(pos)==uniset_props_CLOSE_BRACE;
}*/

static inline UBool
isNameOpen(const UnicodeString &pattern, int32_t pos) {
    return pattern.charAt(pos)==uniset_props_BACKSLASH && pattern.charAt(pos+1)==uniset_props_UPPER_N;
}

static inline UBool
isPOSIXOpen(const UnicodeString &pattern, int32_t pos) {
    return pattern.charAt(pos)==uniset_props_SET_OPEN && pattern.charAt(pos+1)==uniset_props_COLON;
}

/*static inline UBool
isPOSIXClose(const UnicodeString &pattern, int32_t pos) {
    return pattern.charAt(pos)==uniset_props_COLON && pattern.charAt(pos+1)==uniset_props_SET_CLOSE;
}*/

// TODO memory debugging provided inside uniset.cpp
// could be made available here but probably obsolete with use of modern
// memory leak checker tools
#ifndef _dbgct
#define _dbgct(me)
#endif

}  // namespace

U_CFUNC UnicodeSet *
uniset_getUnicode32Instance(UErrorCode &errorCode) {
    umtx_initOnce(uni32InitOnce, &createUni32Set, errorCode);
    return uni32Singleton;
}

//----------------------------------------------------------------
// Constructors &c
//----------------------------------------------------------------

/**
 * Constructs a set from the given pattern, optionally ignoring
 * white space.  See the class description for the syntax of the
 * pattern language.
 * @param pattern a string specifying what characters are in the set
 */
UnicodeSet::UnicodeSet(const UnicodeString& pattern,
                       UErrorCode& status) {
    applyPattern(pattern, status);
    _dbgct(this);
}

//----------------------------------------------------------------
// Public API
//----------------------------------------------------------------

UnicodeSet& UnicodeSet::applyPattern(const UnicodeString& pattern,
                                     UErrorCode& status) {
    // Equivalent to
    //   return applyPattern(pattern, USET_IGNORE_SPACE, NULL, status);
    // but without dependency on closeOver().
    ParsePosition pos(0);
    applyPatternIgnoreSpace(pattern, pos, NULL, status);
    if (U_FAILURE(status)) return *this;

    int32_t i = pos.getIndex();
    // Skip over trailing whitespace
    ICU_Utility::skipWhitespace(pattern, i, TRUE);
    if (i != pattern.length()) {
        status = U_ILLEGAL_ARGUMENT_ERROR;
    }
    return *this;
}

void
UnicodeSet::applyPatternIgnoreSpace(const UnicodeString& pattern,
                                    ParsePosition& pos,
                                    const SymbolTable* symbols,
                                    UErrorCode& status) {
    if (U_FAILURE(status)) {
        return;
    }
    if (isFrozen()) {
        status = U_NO_WRITE_PERMISSION;
        return;
    }
    // Need to build the pattern in a temporary string because
    // _applyPattern calls add() etc., which set pat to empty.
    UnicodeString rebuiltPat;
    RuleCharacterIterator chars(pattern, symbols, pos);
    applyPattern(chars, symbols, rebuiltPat, USET_IGNORE_SPACE, NULL, 0, status);
    if (U_FAILURE(status)) return;
    if (chars.inVariable()) {
        // syntaxError(chars, "Extra chars in variable value");
        status = U_MALFORMED_SET;
        return;
    }
    setPattern(rebuiltPat);
}

/**
 * Return true if the given position, in the given pattern, appears
 * to be the start of a UnicodeSet pattern.
 */
UBool UnicodeSet::resemblesPattern(const UnicodeString& pattern, int32_t pos) {
    return ((pos+1) < pattern.length() &&
            pattern.charAt(pos) == (UChar)91/*[*/) ||
        resemblesPropertyPattern(pattern, pos);
}

//----------------------------------------------------------------
// Implementation: Pattern parsing
//----------------------------------------------------------------

namespace {

/**
 * A small all-inline class to manage a UnicodeSet pointer.  Add
 * operator->() etc. as needed.
 */
class UnicodeSetPointer {
    UnicodeSet* p;
public:
    inline UnicodeSetPointer() : p(0) {}
    inline ~UnicodeSetPointer() { delete p; }
    inline UnicodeSet* pointer() { return p; }
    inline UBool allocate() {
        if (p == 0) {
            p = new UnicodeSet();
        }
        return p != 0;
    }
};

constexpr int32_t MAX_DEPTH = 100;

}  // namespace

/**
 * Parse the pattern from the given RuleCharacterIterator.  The
 * iterator is advanced over the parsed pattern.
 * @param chars iterator over the pattern characters.  Upon return
 * it will be advanced to the first character after the parsed
 * pattern, or the end of the iteration if all characters are
 * parsed.
 * @param symbols symbol table to use to parse and dereference
 * variables, or null if none.
 * @param rebuiltPat the pattern that was parsed, rebuilt or
 * copied from the input pattern, as appropriate.
 * @param options a bit mask of zero or more of the following:
 * IGNORE_SPACE, CASE.
 */
void UnicodeSet::applyPattern(RuleCharacterIterator& chars,
                              const SymbolTable* symbols,
                              UnicodeString& rebuiltPat,
                              uint32_t options,
                              UnicodeSet& (UnicodeSet::*caseClosure)(int32_t attribute),
                              int32_t depth,
                              UErrorCode& ec) {
    if (U_FAILURE(ec)) return;
    if (depth > MAX_DEPTH) {
        ec = U_ILLEGAL_ARGUMENT_ERROR;
        return;
    }

    // Syntax characters: [ ] ^ - & { }

    // Recognized special forms for chars, sets: c-c s-s s&s

    int32_t opts = RuleCharacterIterator::PARSE_VARIABLES |
                   RuleCharacterIterator::PARSE_ESCAPES;
    if ((options & USET_IGNORE_SPACE) != 0) {
        opts |= RuleCharacterIterator::SKIP_WHITESPACE;
    }

    UnicodeString patLocal, buf;
    UBool usePat = FALSE;
    UnicodeSetPointer scratch;
    RuleCharacterIterator::Pos backup;

    // mode: 0=before [, 1=between [...], 2=after ]
    // lastItem: 0=none, 1=char, 2=set
    int8_t lastItem = 0, mode = 0;
    UChar32 lastChar = 0;
    UChar op = 0;

    UBool invert = FALSE;

    clear();

    while (mode != 2 && !chars.atEnd()) {
        U_ASSERT((lastItem == 0 && op == 0) ||
                 (lastItem == 1 && (op == 0 || op == uniset_props_HYPHEN /*'-'*/)) ||
                 (lastItem == 2 && (op == 0 || op == uniset_props_HYPHEN /*'-'*/ ||
                                    op == uniset_props_INTERSECTION /*'&'*/)));

        UChar32 c = 0;
        UBool literal = FALSE;
        UnicodeSet* nested = 0; // alias - do not delete

        // -------- Check for property pattern

        // setMode: 0=none, 1=unicodeset, 2=propertypat, 3=preparsed
        int8_t setMode = 0;
        if (resemblesPropertyPattern(chars, opts)) {
            setMode = 2;
        }

        // -------- Parse '[' of opening delimiter OR nested set.
        // If there is a nested set, use `setMode' to define how
        // the set should be parsed.  If the '[' is part of the
        // opening delimiter for this pattern, parse special
        // strings "[", "[^", "[-", and "[^-".  Check for stand-in
        // characters representing a nested set in the symbol
        // table.

        else {
            // Prepare to backup if necessary
            chars.getPos(backup);
            c = chars.next(opts, literal, ec);
            if (U_FAILURE(ec)) return;

            if (c == 0x5B /*'['*/ && !literal) {
                if (mode == 1) {
                    chars.setPos(backup); // backup
                    setMode = 1;
                } else {
                    // Handle opening '[' delimiter
                    mode = 1;
                    patLocal.append((UChar) 0x5B /*'['*/);
                    chars.getPos(backup); // prepare to backup
                    c = chars.next(opts, literal, ec);
                    if (U_FAILURE(ec)) return;
                    if (c == 0x5E /*'^'*/ && !literal) {
                        invert = TRUE;
                        patLocal.append((UChar) 0x5E /*'^'*/);
                        chars.getPos(backup); // prepare to backup
                        c = chars.next(opts, literal, ec);
                        if (U_FAILURE(ec)) return;
                    }
                    // Fall through to handle special leading '-';
                    // otherwise restart loop for nested [], \p{}, etc.
                    if (c == uniset_props_HYPHEN /*'-'*/) {
                        literal = TRUE;
                        // Fall through to handle literal '-' below
                    } else {
                        chars.setPos(backup); // backup
                        continue;
                    }
                }
            } else if (symbols != 0) {
                const UnicodeFunctor *m = symbols->lookupMatcher(c);
                if (m != 0) {
                    const UnicodeSet *ms = dynamic_cast<const UnicodeSet *>(m);
                    if (ms == NULL) {
                        ec = U_MALFORMED_SET;
                        return;
                    }
                    // casting away const, but `nested' won't be modified
                    // (important not to modify stored set)
                    nested = const_cast<UnicodeSet*>(ms);
                    setMode = 3;
                }
            }
        }

        // -------- Handle a nested set.  This either is inline in
        // the pattern or represented by a stand-in that has
        // previously been parsed and was looked up in the symbol
        // table.

        if (setMode != 0) {
            if (lastItem == 1) {
                if (op != 0) {
                    // syntaxError(chars, "Char expected after operator");
                    ec = U_MALFORMED_SET;
                    return;
                }
                add(lastChar, lastChar);
                _appendToPat(patLocal, lastChar, FALSE);
                lastItem = 0;
                op = 0;
            }

            if (op == uniset_props_HYPHEN /*'-'*/ || op == uniset_props_INTERSECTION /*'&'*/) {
                patLocal.append(op);
            }

            if (nested == 0) {
                // lazy allocation
                if (!scratch.allocate()) {
                    ec = U_MEMORY_ALLOCATION_ERROR;
                    return;
                }
                nested = scratch.pointer();
            }
            switch (setMode) {
            case 1:
                nested->applyPattern(chars, symbols, patLocal, options, caseClosure, depth + 1, ec);
                break;
            case 2:
                chars.skipIgnored(opts);
                nested->applyPropertyPattern(chars, patLocal, ec);
                if (U_FAILURE(ec)) return;
                break;
            case 3: // `nested' already parsed
                nested->_toPattern(patLocal, FALSE);
                break;
            }

            usePat = TRUE;

            if (mode == 0) {
                // Entire pattern is a category; leave parse loop
                *this = *nested;
                mode = 2;
                break;
            }

            switch (op) {
            case uniset_props_HYPHEN: /*'-'*/
                removeAll(*nested);
                break;
            case uniset_props_INTERSECTION: /*'&'*/
                retainAll(*nested);
                break;
            case 0:
                addAll(*nested);
                break;
            }

            op = 0;
            lastItem = 2;

            continue;
        }

        if (mode == 0) {
            // syntaxError(chars, "Missing '['");
            ec = U_MALFORMED_SET;
            return;
        }

        // -------- Parse special (syntax) characters.  If the
        // current character is not special, or if it is escaped,
        // then fall through and handle it below.

        if (!literal) {
            switch (c) {
            case 0x5D /*']'*/:
                if (lastItem == 1) {
                    add(lastChar, lastChar);
                    _appendToPat(patLocal, lastChar, FALSE);
                }
                // Treat final trailing '-' as a literal
                if (op == uniset_props_HYPHEN /*'-'*/) {
                    add(op, op);
                    patLocal.append(op);
                } else if (op == uniset_props_INTERSECTION /*'&'*/) {
                    // syntaxError(chars, "Trailing '&'");
                    ec = U_MALFORMED_SET;
                    return;
                }
                patLocal.append((UChar) 0x5D /*']'*/);
                mode = 2;
                continue;
            case uniset_props_HYPHEN /*'-'*/:
                if (op == 0) {
                    if (lastItem != 0) {
                        op = (UChar) c;
                        continue;
                    } else {
                        // Treat final trailing '-' as a literal
                        add(c, c);
                        c = chars.next(opts, literal, ec);
                        if (U_FAILURE(ec)) return;
                        if (c == 0x5D /*']'*/ && !literal) {
                            patLocal.append(uniset_props_HYPHEN_RIGHT_BRACE, 2);
                            mode = 2;
                            continue;
                        }
                    }
                }
                // syntaxError(chars, "'-' not after char or set");
                ec = U_MALFORMED_SET;
                return;
            case uniset_props_INTERSECTION /*'&'*/:
                if (lastItem == 2 && op == 0) {
                    op = (UChar) c;
                    continue;
                }
                // syntaxError(chars, "'&' not after set");
                ec = U_MALFORMED_SET;
                return;
            case 0x5E /*'^'*/:
                // syntaxError(chars, "'^' not after '['");
                ec = U_MALFORMED_SET;
                return;
            case 0x7B /*'{'*/:
                if (op != 0) {
                    // syntaxError(chars, "Missing operand after operator");
                    ec = U_MALFORMED_SET;
                    return;
                }
                if (lastItem == 1) {
                    add(lastChar, lastChar);
                    _appendToPat(patLocal, lastChar, FALSE);
                }
                lastItem = 0;
                buf.truncate(0);
                {
                    UBool ok = FALSE;
                    while (!chars.atEnd()) {
                        c = chars.next(opts, literal, ec);
                        if (U_FAILURE(ec)) return;
                        if (c == 0x7D /*'}'*/ && !literal) {
                            ok = TRUE;
                            break;
                        }
                        buf.append(c);
                    }
                    if (buf.length() < 1 || !ok) {
                        // syntaxError(chars, "Invalid multicharacter string");
                        ec = U_MALFORMED_SET;
                        return;
                    }
                }
                // We have new string. Add it to set and continue;
                // we don't need to drop through to the further
                // processing
                add(buf);
                patLocal.append((UChar) 0x7B /*'{'*/);
                _appendToPat(patLocal, buf, FALSE);
                patLocal.append((UChar) 0x7D /*'}'*/);
                continue;
            case SymbolTable::SYMBOL_REF:
                //         symbols  nosymbols
                // [a-$]   error    error (ambiguous)
                // [a$]    anchor   anchor
                // [a-$x]  var "x"* literal '$'
                // [a-$.]  error    literal '$'
                // *We won't get here in the case of var "x"
                {
                    chars.getPos(backup);
                    c = chars.next(opts, literal, ec);
                    if (U_FAILURE(ec)) return;
                    UBool anchor = (c == 0x5D /*']'*/ && !literal);
                    if (symbols == 0 && !anchor) {
                        c = SymbolTable::SYMBOL_REF;
                        chars.setPos(backup);
                        break; // literal '$'
                    }
                    if (anchor && op == 0) {
                        if (lastItem == 1) {
                            add(lastChar, lastChar);
                            _appendToPat(patLocal, lastChar, FALSE);
                        }
                        add(U_ETHER);
                        usePat = TRUE;
                        patLocal.append((UChar) SymbolTable::SYMBOL_REF);
                        patLocal.append((UChar) 0x5D /*']'*/);
                        mode = 2;
                        continue;
                    }
                    // syntaxError(chars, "Unquoted '$'");
                    ec = U_MALFORMED_SET;
                    return;
                }
            default:
                break;
            }
        }

        // -------- Parse literal characters.  This includes both
        // escaped chars ("\u4E01") and non-syntax characters
        // ("a").

        switch (lastItem) {
        case 0:
            lastItem = 1;
            lastChar = c;
            break;
        case 1:
            if (op == uniset_props_HYPHEN /*'-'*/) {
                if (lastChar >= c) {
                    // Don't allow redundant (a-a) or empty (b-a) ranges;
                    // these are most likely typos.
                    // syntaxError(chars, "Invalid range");
                    ec = U_MALFORMED_SET;
                    return;
                }
                add(lastChar, c);
                _appendToPat(patLocal, lastChar, FALSE);
                patLocal.append(op);
                _appendToPat(patLocal, c, FALSE);
                lastItem = 0;
                op = 0;
            } else {
                add(lastChar, lastChar);
                _appendToPat(patLocal, lastChar, FALSE);
                lastChar = c;
            }
            break;
        case 2:
            if (op != 0) {
                // syntaxError(chars, "Set expected after operator");
                ec = U_MALFORMED_SET;
                return;
            }
            lastChar = c;
            lastItem = 1;
            break;
        }
    }

    if (mode != 2) {
        // syntaxError(chars, "Missing ']'");
        ec = U_MALFORMED_SET;
        return;
    }

    chars.skipIgnored(opts);

    /**
     * Handle global flags (invert, case insensitivity).  If this
     * pattern should be compiled case-insensitive, then we need
     * to close over case BEFORE uniset_props_COMPLEMENTING.  This makes
     * patterns like /[^abc]/i work.
     */
    if ((options & USET_CASE_INSENSITIVE) != 0) {
        (this->*caseClosure)(USET_CASE_INSENSITIVE);
    }
    else if ((options & USET_ADD_CASE_MAPPINGS) != 0) {
        (this->*caseClosure)(USET_ADD_CASE_MAPPINGS);
    }
    if (invert) {
        complement();
    }

    // Use the rebuilt pattern (patLocal) only if necessary.  Prefer the
    // generated pattern.
    if (usePat) {
        rebuiltPat.append(patLocal);
    } else {
        _generatePattern(rebuiltPat, FALSE);
    }
    if (isBogus() && U_SUCCESS(ec)) {
        // We likely ran out of memory. AHHH!
        ec = U_MEMORY_ALLOCATION_ERROR;
    }
}

//----------------------------------------------------------------
// Property set implementation
//----------------------------------------------------------------

namespace {

static UBool numericValueFilter(UChar32 ch, void* context) {
    return u_getNumericValue(ch) == *(double*)context;
}

static UBool generalCategoryMaskFilter(UChar32 ch, void* context) {
    int32_t value = *(int32_t*)context;
    return (U_GET_GC_MASK((UChar32) ch) & value) != 0;
}

static UBool versionFilter(UChar32 ch, void* context) {
    static const UVersionInfo none = { 0, 0, 0, 0 };
    UVersionInfo v;
    u_charAge(ch, v);
    UVersionInfo* version = (UVersionInfo*)context;
    return uprv_memcmp(&v, &none, sizeof(v)) > 0 && uprv_memcmp(&v, version, sizeof(v)) <= 0;
}

typedef struct {
    UProperty prop;
    int32_t value;
} IntPropertyContext;

static UBool intPropertyFilter(UChar32 ch, void* context) {
    IntPropertyContext* c = (IntPropertyContext*)context;
    return u_getIntPropertyValue((UChar32) ch, c->prop) == c->value;
}

static UBool scriptExtensionsFilter(UChar32 ch, void* context) {
    return uscript_hasScript(ch, *(UScriptCode*)context);
}

}  // namespace

/**
 * Generic filter-based scanning code for UCD property UnicodeSets.
 */
void UnicodeSet::applyFilter(UnicodeSet::Filter filter,
                             void* context,
                             const UnicodeSet* inclusions,
                             UErrorCode &status) {
    if (U_FAILURE(status)) return;

    // Logically, walk through all Unicode characters, noting the start
    // and end of each range for which filter.contain(c) is
    // true.  Add each range to a set.
    //
    // To improve performance, use an inclusions set which
    // encodes information about character ranges that are known
    // to have identical properties.
    // inclusions contains the first characters of
    // same-value ranges for the given property.

    clear();

    UChar32 startHasProperty = -1;
    int32_t limitRange = inclusions->getRangeCount();

    for (int j=0; j<limitRange; ++j) {
        // get current range
        UChar32 start = inclusions->getRangeStart(j);
        UChar32 end = inclusions->getRangeEnd(j);

        // for all the code points in the range, process
        for (UChar32 ch = start; ch <= end; ++ch) {
            // only add to this UnicodeSet on inflection points --
            // where the hasProperty value changes to false
            if ((*filter)(ch, context)) {
                if (startHasProperty < 0) {
                    startHasProperty = ch;
                }
            } else if (startHasProperty >= 0) {
                add(startHasProperty, ch-1);
                startHasProperty = -1;
            }
        }
    }
    if (startHasProperty >= 0) {
        add((UChar32)startHasProperty, (UChar32)0x10FFFF);
    }
    if (isBogus() && U_SUCCESS(status)) {
        // We likely ran out of memory. AHHH!
        status = U_MEMORY_ALLOCATION_ERROR;
    }
}

namespace {

static UBool mungeCharName(char* dst, const char* src, int32_t dstCapacity) {
    /* Note: we use ' ' in compiler code page */
    int32_t j = 0;
    char ch;
    --dstCapacity; /* make room for term. zero */
    while ((ch = *src++) != 0) {
        if (ch == ' ' && (j==0 || (j>0 && dst[j-1]==' '))) {
            continue;
        }
        if (j >= dstCapacity) return FALSE;
        dst[j++] = ch;
    }
    if (j > 0 && dst[j-1] == ' ') --j;
    dst[j] = 0;
    return TRUE;
}

}  // namespace

//----------------------------------------------------------------
// Property set API
//----------------------------------------------------------------

#define FAIL(ec) UPRV_BLOCK_MACRO_BEGIN { \
    ec=U_ILLEGAL_ARGUMENT_ERROR; \
    return *this; \
} UPRV_BLOCK_MACRO_END

UnicodeSet&
UnicodeSet::applyIntPropertyValue(UProperty prop, int32_t value, UErrorCode& ec) {
    if (U_FAILURE(ec) || isFrozen()) { return *this; }
    if (prop == UCHAR_GENERAL_CATEGORY_MASK) {
        const UnicodeSet* inclusions = CharacterProperties::getInclusionsForProperty(prop, ec);
        applyFilter(generalCategoryMaskFilter, &value, inclusions, ec);
    } else if (prop == UCHAR_SCRIPT_EXTENSIONS) {
        const UnicodeSet* inclusions = CharacterProperties::getInclusionsForProperty(prop, ec);
        UScriptCode script = (UScriptCode)value;
        applyFilter(scriptExtensionsFilter, &script, inclusions, ec);
    } else if (0 <= prop && prop < UCHAR_BINARY_LIMIT) {
        if (value == 0 || value == 1) {
            const USet *set = u_getBinaryPropertySet(prop, &ec);
            if (U_FAILURE(ec)) { return *this; }
            copyFrom(*UnicodeSet::fromUSet(set), TRUE);
            if (value == 0) {
                complement();
            }
        } else {
            clear();
        }
    } else if (UCHAR_INT_START <= prop && prop < UCHAR_INT_LIMIT) {
        const UnicodeSet* inclusions = CharacterProperties::getInclusionsForProperty(prop, ec);
        IntPropertyContext c = {prop, value};
        applyFilter(intPropertyFilter, &c, inclusions, ec);
    } else {
        ec = U_ILLEGAL_ARGUMENT_ERROR;
    }
    return *this;
}

UnicodeSet&
UnicodeSet::applyPropertyAlias(const UnicodeString& prop,
                               const UnicodeString& value,
                               UErrorCode& ec) {
    if (U_FAILURE(ec) || isFrozen()) return *this;

    // prop and value used to be converted to char * using the default
    // converter instead of the invariant conversion.
    // This should not be necessary because all Unicode property and value
    // names use only invariant characters.
    // If there are any variant characters, then we won't find them anyway.
    // Checking first avoids assertion failures in the conversion.
    if( !uprv_isInvariantUString(prop.getBuffer(), prop.length()) ||
        !uprv_isInvariantUString(value.getBuffer(), value.length())
    ) {
        FAIL(ec);
    }
    CharString pname, vname;
    pname.appendInvariantChars(prop, ec);
    vname.appendInvariantChars(value, ec);
    if (U_FAILURE(ec)) return *this;

    UProperty p;
    int32_t v;
    UBool invert = FALSE;

    if (value.length() > 0) {
        p = u_getPropertyEnum(pname.data());
        if (p == UCHAR_INVALID_CODE) FAIL(ec);

        // Treat gc as gcm
        if (p == UCHAR_GENERAL_CATEGORY) {
            p = UCHAR_GENERAL_CATEGORY_MASK;
        }

        if ((p >= UCHAR_BINARY_START && p < UCHAR_BINARY_LIMIT) ||
            (p >= UCHAR_INT_START && p < UCHAR_INT_LIMIT) ||
            (p >= UCHAR_MASK_START && p < UCHAR_MASK_LIMIT)) {
            v = u_getPropertyValueEnum(p, vname.data());
            if (v == UCHAR_INVALID_CODE) {
                // Handle numeric CCC
                if (p == UCHAR_CANONICAL_COMBINING_CLASS ||
                    p == UCHAR_TRAIL_CANONICAL_COMBINING_CLASS ||
                    p == UCHAR_LEAD_CANONICAL_COMBINING_CLASS) {
                    char* end;
                    double val = uprv_strtod(vname.data(), &end);
                    // Anything between 0 and 255 is valid even if unused.
                    // Cast double->int only after range check.
                    // We catch NaN here because comparing it with both 0 and 255 will be false
                    // (as are all comparisons with NaN).
                    if (*end != 0 || !(0 <= val && val <= 255) ||
                            (v = (int32_t)val) != val) {
                        // non-integral value or outside 0..255, or trailing junk
                        FAIL(ec);
                    }
                } else {
                    FAIL(ec);
                }
            }
        }

        else {

            switch (p) {
            case UCHAR_NUMERIC_VALUE:
                {
                    char* end;
                    double val = uprv_strtod(vname.data(), &end);
                    if (*end != 0) {
                        FAIL(ec);
                    }
                    applyFilter(numericValueFilter, &val,
                                CharacterProperties::getInclusionsForProperty(p, ec), ec);
                    return *this;
                }
            case UCHAR_NAME:
                {
                    // Must munge name, since u_charFromName() does not do
                    // 'loose' matching.
                    char buf[128]; // it suffices that this be > uprv_getMaxCharNameLength
                    if (!mungeCharName(buf, vname.data(), sizeof(buf))) FAIL(ec);
                    UChar32 ch = u_charFromName(U_EXTENDED_CHAR_NAME, buf, &ec);
                    if (U_SUCCESS(ec)) {
                        clear();
                        add(ch);
                        return *this;
                    } else {
                        FAIL(ec);
                    }
                }
            case UCHAR_UNICODE_1_NAME:
                // ICU 49 deprecates the Unicode_1_Name property APIs.
                FAIL(ec);
            case UCHAR_AGE:
                {
                    // Must munge name, since u_versionFromString() does not do
                    // 'loose' matching.
                    char buf[128];
                    if (!mungeCharName(buf, vname.data(), sizeof(buf))) FAIL(ec);
                    UVersionInfo version;
                    u_versionFromString(version, buf);
                    applyFilter(versionFilter, &version,
                                CharacterProperties::getInclusionsForProperty(p, ec), ec);
                    return *this;
                }
            case UCHAR_SCRIPT_EXTENSIONS:
                v = u_getPropertyValueEnum(UCHAR_SCRIPT, vname.data());
                if (v == UCHAR_INVALID_CODE) {
                    FAIL(ec);
                }
                // fall through to calling applyIntPropertyValue()
                break;
            default:
                // p is a non-binary, non-enumerated property that we
                // don't support (yet).
                FAIL(ec);
            }
        }
    }

    else {
        // value is empty.  Interpret as General Category, Script, or
        // Binary property.
        p = UCHAR_GENERAL_CATEGORY_MASK;
        v = u_getPropertyValueEnum(p, pname.data());
        if (v == UCHAR_INVALID_CODE) {
            p = UCHAR_SCRIPT;
            v = u_getPropertyValueEnum(p, pname.data());
            if (v == UCHAR_INVALID_CODE) {
                p = u_getPropertyEnum(pname.data());
                if (p >= UCHAR_BINARY_START && p < UCHAR_BINARY_LIMIT) {
                    v = 1;
                } else if (0 == uprv_comparePropertyNames(uniset_props_ANY, pname.data())) {
                    set(MIN_VALUE, MAX_VALUE);
                    return *this;
                } else if (0 == uprv_comparePropertyNames(uniset_props_ASCII, pname.data())) {
                    set(0, 0x7F);
                    return *this;
                } else if (0 == uprv_comparePropertyNames(uniset_props_ASSIGNED, pname.data())) {
                    // [:Assigned:]=[:^Cn:]
                    p = UCHAR_GENERAL_CATEGORY_MASK;
                    v = U_GC_CN_MASK;
                    invert = TRUE;
                } else {
                    FAIL(ec);
                }
            }
        }
    }

    applyIntPropertyValue(p, v, ec);
    if(invert) {
        complement();
    }

    if (isBogus() && U_SUCCESS(ec)) {
        // We likely ran out of memory. AHHH!
        ec = U_MEMORY_ALLOCATION_ERROR;
    }
    return *this;
}

//----------------------------------------------------------------
// Property set patterns
//----------------------------------------------------------------

/**
 * Return true if the given position, in the given pattern, appears
 * to be the start of a property set pattern.
 */
UBool UnicodeSet::resemblesPropertyPattern(const UnicodeString& pattern,
                                           int32_t pos) {
    // Patterns are at least 5 characters long
    if ((pos+5) > pattern.length()) {
        return FALSE;
    }

    // Look for an opening [:, [:^, \p, or \P
    return isPOSIXOpen(pattern, pos) || isPerlOpen(pattern, pos) || isNameOpen(pattern, pos);
}

/**
 * Return true if the given iterator appears to point at a
 * property pattern.  Regardless of the result, return with the
 * iterator unchanged.
 * @param chars iterator over the pattern characters.  Upon return
 * it will be unchanged.
 * @param iterOpts RuleCharacterIterator options
 */
UBool UnicodeSet::resemblesPropertyPattern(RuleCharacterIterator& chars,
                                           int32_t iterOpts) {
    // NOTE: literal will always be FALSE, because we don't parse escapes.
    UBool result = FALSE, literal;
    UErrorCode ec = U_ZERO_ERROR;
    iterOpts &= ~RuleCharacterIterator::PARSE_ESCAPES;
    RuleCharacterIterator::Pos pos;
    chars.getPos(pos);
    UChar32 c = chars.next(iterOpts, literal, ec);
    if (c == 0x5B /*'['*/ || c == 0x5C /*'\\'*/) {
        UChar32 d = chars.next(iterOpts & ~RuleCharacterIterator::SKIP_WHITESPACE,
                               literal, ec);
        result = (c == 0x5B /*'['*/) ? (d == 0x3A /*':'*/) :
                 (d == 0x4E /*'N'*/ || d == 0x70 /*'p'*/ || d == 0x50 /*'P'*/);
    }
    chars.setPos(pos);
    return result && U_SUCCESS(ec);
}

/**
 * Parse the given property pattern at the given parse position.
 */
UnicodeSet& UnicodeSet::applyPropertyPattern(const UnicodeString& pattern,
                                             ParsePosition& ppos,
                                             UErrorCode &ec) {
    int32_t pos = ppos.getIndex();

    UBool posix = FALSE; // true for [:pat:], false for \p{pat} \P{pat} \N{pat}
    UBool isName = FALSE; // true for \N{pat}, o/w false
    UBool invert = FALSE;

    if (U_FAILURE(ec)) return *this;

    // Minimum length is 5 characters, e.g. \p{L}
    if ((pos+5) > pattern.length()) {
        FAIL(ec);
    }

    // On entry, ppos should point to one of the following locations:
    // Look for an opening [:, [:^, \p, or \P
    if (isPOSIXOpen(pattern, pos)) {
        posix = TRUE;
        pos += 2;
        pos = ICU_Utility::skipWhitespace(pattern, pos);
        if (pos < pattern.length() && pattern.charAt(pos) == uniset_props_COMPLEMENT) {
            ++pos;
            invert = TRUE;
        }
    } else if (isPerlOpen(pattern, pos) || isNameOpen(pattern, pos)) {
        UChar c = pattern.charAt(pos+1);
        invert = (c == uniset_props_UPPER_P);
        isName = (c == uniset_props_UPPER_N);
        pos += 2;
        pos = ICU_Utility::skipWhitespace(pattern, pos);
        if (pos == pattern.length() || pattern.charAt(pos++) != uniset_props_OPEN_BRACE) {
            // Syntax error; "\p" or "\P" not followed by "{"
            FAIL(ec);
        }
    } else {
        // Open delimiter not seen
        FAIL(ec);
    }

    // Look for the matching close delimiter, either :] or }
    int32_t close;
    if (posix) {
      close = pattern.indexOf(uniset_props_POSIX_CLOSE, 2, pos);
    } else {
      close = pattern.indexOf(uniset_props_CLOSE_BRACE, pos);
    }
    if (close < 0) {
        // Syntax error; close delimiter missing
        FAIL(ec);
    }

    // Look for an '=' sign.  If this is present, we will parse a
    // medium \p{gc=Cf} or long \p{GeneralCategory=Format}
    // pattern.
    int32_t equals = pattern.indexOf(uniset_props_EQUALS, pos);
    UnicodeString propName, valueName;
    if (equals >= 0 && equals < close && !isName) {
        // Equals seen; parse medium/long pattern
        pattern.extractBetween(pos, equals, propName);
        pattern.extractBetween(equals+1, close, valueName);
    }

    else {
        // Handle case where no '=' is seen, and \N{}
        pattern.extractBetween(pos, close, propName);

        // Handle \N{name}
        if (isName) {
            // This is a little inefficient since it means we have to
            // parse uniset_props_NAME_PROP back to UCHAR_NAME even though we already
            // know it's UCHAR_NAME.  If we refactor the API to
            // support args of (UProperty, char*) then we can remove
            // uniset_props_NAME_PROP and make this a little more efficient.
            valueName = propName;
            propName = UnicodeString(uniset_props_NAME_PROP, uniset_props_NAME_PROP_LENGTH, US_INV);
        }
    }

    applyPropertyAlias(propName, valueName, ec);

    if (U_SUCCESS(ec)) {
        if (invert) {
            complement();
        }

        // Move to the limit position after the close delimiter if the
        // parse succeeded.
        ppos.setIndex(close + (posix ? 2 : 1));
    }

    return *this;
}

/**
 * Parse a property pattern.
 * @param chars iterator over the pattern characters.  Upon return
 * it will be advanced to the first character after the parsed
 * pattern, or the end of the iteration if all characters are
 * parsed.
 * @param rebuiltPat the pattern that was parsed, rebuilt or
 * copied from the input pattern, as appropriate.
 */
void UnicodeSet::applyPropertyPattern(RuleCharacterIterator& chars,
                                      UnicodeString& rebuiltPat,
                                      UErrorCode& ec) {
    if (U_FAILURE(ec)) return;
    UnicodeString pattern;
    chars.lookahead(pattern);
    ParsePosition pos(0);
    applyPropertyPattern(pattern, pos, ec);
    if (U_FAILURE(ec)) return;
    if (pos.getIndex() == 0) {
        // syntaxError(chars, "Invalid property pattern");
        ec = U_MALFORMED_SET;
        return;
    }
    chars.jumpahead(pos.getIndex());
    rebuiltPat.append(pattern, 0, pos.getIndex());
}

U_NAMESPACE_END
