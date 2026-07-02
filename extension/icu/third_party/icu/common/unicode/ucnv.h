/**
 * Converter option for EBCDIC SBCS or mixed-SBCS/DBCS (stateful) codepages.
 * Swaps Unicode mappings for EBCDIC LF and NL codes, as used on
 * S/390 (z/OS) Unix System Services (Open Edition).
 * For example, ucnv_open("ibm-1047,swaplfnl", &errorCode);
 * See convrtrs.txt.
 *
 * @see ucnv_open
 * @stable ICU 2.4
 */
#define UCNV_SWAP_LFNL_OPTION_STRING ",swaplfnl"
