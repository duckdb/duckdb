/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - log_min_messages
 * - client_min_messages
 * - check_function_bodies
 *--------------------------------------------------------------------
 */

/*--------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration scheme, including SET
 * command, configuration file, and command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2015, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

#include "access/commit_ts.h"
#include "access/gin.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "commands/vacuum.h"
#include "commands/variable.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "libpq/auth.h"
#include "libpq/be-fsstubs.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "postmaster/walwriter.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/dsm_impl.h"
#include "storage/standby.h"
#include "storage/fd.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/predicate.h"
#include "tcop/tcopprot.h"
#include "tsearch/ts_cache.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/tzparser.h"
#include "utils/xml.h"

#ifndef PG_KRB_SRVTAB
#define PG_KRB_SRVTAB ""
#endif

#define CONFIG_FILENAME "postgresql.conf"
#define HBA_FILENAME	"pg_hba.conf"
#define IDENT_FILENAME	"pg_ident.conf"

#ifdef EXEC_BACKEND
#define CONFIG_EXEC_PARAMS "global/config_exec_params"
#define CONFIG_EXEC_PARAMS_NEW "global/config_exec_params.new"
#endif

/*
 * Precision with which REAL type guc values are to be printed for GUC
 * serialization.
 */
#define REALTYPE_PRECISION 17

/* XXX these should appear in other modules' header files */
extern bool Log_disconnections;
extern int	CommitDelay;
extern int	CommitSiblings;
extern char *default_tablespace;
extern char *temp_tablespaces;
extern bool ignore_checksum_failure;
extern bool synchronize_seqscans;

#ifdef TRACE_SYNCSCAN
extern bool trace_syncscan;
#endif
#ifdef DEBUG_BOUNDED_SORT
extern bool optimize_bounded_sort;
#endif



/* global variables for check hook support */




static void do_serialize(char **destptr, Size *maxbytes, const char *fmt,...) pg_attribute_printf(3, 4);

static void set_config_sourcefile(const char *name, char *sourcefile,
					  int sourceline);
static bool call_bool_check_hook(struct config_bool * conf, bool *newval,
					 void **extra, GucSource source, int elevel);
static bool call_int_check_hook(struct config_int * conf, int *newval,
					void **extra, GucSource source, int elevel);
static bool call_real_check_hook(struct config_real * conf, double *newval,
					 void **extra, GucSource source, int elevel);
static bool call_string_check_hook(struct config_string * conf, char **newval,
					   void **extra, GucSource source, int elevel);
static bool call_enum_check_hook(struct config_enum * conf, int *newval,
					 void **extra, GucSource source, int elevel);

static bool check_log_destination(char **newval, void **extra, GucSource source);
static void assign_log_destination(const char *newval, void *extra);

#ifdef HAVE_SYSLOG

#else
static int	syslog_facility = 0;
#endif

static void assign_syslog_facility(int newval, void *extra);
static void assign_syslog_ident(const char *newval, void *extra);
static void assign_session_replication_role(int newval, void *extra);
static bool check_temp_buffers(int *newval, void **extra, GucSource source);
static bool check_bonjour(bool *newval, void **extra, GucSource source);
static bool check_ssl(bool *newval, void **extra, GucSource source);
static bool check_stage_log_stats(bool *newval, void **extra, GucSource source);
static bool check_log_stats(bool *newval, void **extra, GucSource source);
static bool check_canonical_path(char **newval, void **extra, GucSource source);
static bool check_timezone_abbreviations(char **newval, void **extra, GucSource source);
static void assign_timezone_abbreviations(const char *newval, void *extra);
static void pg_timezone_abbrev_initialize(void);
static const char *show_archive_command(void);
static void assign_tcp_keepalives_idle(int newval, void *extra);
static void assign_tcp_keepalives_interval(int newval, void *extra);
static void assign_tcp_keepalives_count(int newval, void *extra);
static const char *show_tcp_keepalives_idle(void);
static const char *show_tcp_keepalives_interval(void);
static const char *show_tcp_keepalives_count(void);
static bool check_maxconnections(int *newval, void **extra, GucSource source);
static bool check_max_worker_processes(int *newval, void **extra, GucSource source);
static bool check_autovacuum_max_workers(int *newval, void **extra, GucSource source);
static bool check_autovacuum_work_mem(int *newval, void **extra, GucSource source);
static bool check_effective_io_concurrency(int *newval, void **extra, GucSource source);
static void assign_effective_io_concurrency(int newval, void *extra);
static void assign_pgstat_temp_directory(const char *newval, void *extra);
static bool check_application_name(char **newval, void **extra, GucSource source);
static void assign_application_name(const char *newval, void *extra);
static bool check_cluster_name(char **newval, void **extra, GucSource source);
static const char *show_unix_socket_permissions(void);
static const char *show_log_file_mode(void);

/* Private functions in guc-file.l that need to be called from guc.c */
static ConfigVariable *ProcessConfigFileInternal(GucContext context,
						  bool applySettings, int elevel);


/*
 * Options for enum values defined in this module.
 *
 * NOTE! Option values may not contain double quotes!
 */



/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level)
 */














#ifdef HAVE_SYSLOG
#else
#endif







/*
 * Although only "on", "off", and "safe_encoding" are documented, we
 * accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", and "partition" are documented, we
 * accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", "remote_write", and "local" are documented, we
 * accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", "try" are documented, we accept all the likely
 * variants of "on" and "off".
 */


/*
 * Options for enum values stored in other modules
 */
extern const struct config_enum_entry wal_level_options[];
extern const struct config_enum_entry archive_mode_options[];
extern const struct config_enum_entry sync_method_options[];
extern const struct config_enum_entry dynamic_shared_memory_options[];

/*
 * GUC option variables that are exported from this module
 */









		/* this is sort of all three
												 * above together */




__thread bool		check_function_bodies = true;







__thread int			log_min_messages = WARNING;

__thread int			client_min_messages = NOTICE;























/*
 * SSL renegotiation was been removed in PostgreSQL 9.5, but we tolerate it
 * being set to zero (meaning never renegotiate) for backward compatibility.
 * This avoids breaking compatibility with clients that have never supported
 * renegotiation and therefore always try to zero it.
 */


/*
 * This really belongs in pg_shmem.c, but is defined here so that it doesn't
 * need to be duplicated in all the different implementations of pg_shmem.c.
 */


/*
 * These variables are all dummies that don't do anything, except in some
 * cases provide the value for SHOW to display.  The real state is elsewhere
 * and is kept in sync by assign_hooks.
 */




























/* should be static, but commands/variable.c needs to get at this */



/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */


/*
 * Displayable names for source types (enum GucSource)
 *
 * Note: these strings are deliberately not localized.
 */


/*
 * Displayable names for the groupings defined in enum config_group
 */


/*
 * Displayable names for GUC variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */


/*
 * Unit conversion tables.
 *
 * There are two tables, one for memory units, and another for time units.
 * For each supported conversion from one unit to another, we have an entry
 * in the table.
 *
 * To keep things simple, and to avoid intermediate-value overflows,
 * conversions are never chained.  There needs to be a direct conversion
 * between all units (of the same type).
 *
 * The conversions from each base unit must be kept in order from greatest
 * to smallest unit; convert_from_base_unit() relies on that.  (The order of
 * the base units does not matter.)
 */
#define MAX_UNIT_LEN		3	/* length of longest recognized unit string */

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* GUC_UNIT_XXX */
	int			multiplier;		/* If positive, multiply the value with this
								 * for unit -> base_unit conversion.  If
								 * negative, divide (with the absolute value) */
} unit_conversion;

/* Ensure that the constants in the tables don't overflow or underflow */
#if BLCKSZ < 1024 || BLCKSZ > (1024*1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024*1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_SEG_SIZE < (1024*1024) || XLOG_BLCKSZ > (1024*1024*1024)
#error XLOG_SEG_SIZE must be between 1MB and 1GB
#endif









/*
 * Contents of GUC tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *	  and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *	  details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *	  applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *	  appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GUC_LIST option you must edit pg_dumpall.c to ensure
 *	  it is not single quoted at dump time.
 */


/******** option records follow ********/

#ifdef USE_ASSERT_CHECKING
#else
#endif
#ifdef BTREE_BUILD_STATS
#endif
#ifdef LOCK_DEBUG
#endif
#ifdef TRACE_SORT
#endif
#ifdef TRACE_SYNCSCAN
#endif
#ifdef DEBUG_BOUNDED_SORT
#endif
#ifdef WAL_DEBUG
#endif
#ifdef HAVE_INT64_TIMESTAMP
#else
#endif


#ifdef LOCK_DEBUG
#endif
#ifdef USE_PREFETCH
#else
#endif





#ifdef HAVE_UNIX_SOCKETS
#else
#endif
#ifdef USE_SSL
#else
#endif
#ifdef USE_SSL
#else
#endif


#ifdef HAVE_SYSLOG
#else
#endif

/******** end of options list ********/


/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name
 * should be mapped to a new one only if the new variable has very similar
 * semantics to the old.
 */



/*
 * Actual lookup of variables is done through this single, sorted array.
 */


/* Current number of variables contained in the vector */


/* Vector capacity */



			/* TRUE if need to do commit/abort work */

	/* TRUE to enable GUC_REPORT */

	/* 1 when in main transaction */


static int	guc_var_compare(const void *a, const void *b);
static int	guc_name_compare(const char *namea, const char *nameb);
static void InitializeGUCOptionsFromEnvironment(void);
static void InitializeOneGUCOption(struct config_generic * gconf);
static void push_old_value(struct config_generic * gconf, GucAction action);
static void ReportGUCOption(struct config_generic * record);
static void reapply_stacked_values(struct config_generic * variable,
					   struct config_string * pHolder,
					   GucStack *stack,
					   const char *curvalue,
					   GucContext curscontext, GucSource cursource);
static void ShowGUCConfigOption(const char *name, DestReceiver *dest);
static void ShowAllGUCConfig(DestReceiver *dest);
static char *_ShowOption(struct config_generic * record, bool use_units);
static bool validate_option_array_item(const char *name, const char *value,
						   bool skipIfNoPermissions);
static void write_auto_conf_file(int fd, const char *filename, ConfigVariable *head_p);
static void replace_auto_config_value(ConfigVariable **head_p, ConfigVariable **tail_p,
						  const char *name, const char *value);


/*
 * Some infrastructure for checking malloc/strdup/realloc calls
 */







/*
 * Detect whether strval is referenced anywhere in a GUC string item
 */


/*
 * Support for assigning to a field of a string GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */


/*
 * Detect whether an "extra" struct is referenced anywhere in a GUC item
 */


/*
 * Support for assigning to an "extra" field of a GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */


/*
 * Support for copying a variable's active value into a stack entry.
 * The "extra" field associated with the active value is copied, too.
 *
 * NB: be sure stringval and extra fields of a new stack entry are
 * initialized to NULL before this is used, else we'll try to free() them.
 */


/*
 * Support for discarding a no-longer-needed value in a stack entry.
 * The "extra" field associated with the stack entry is cleared, too.
 */



/*
 * Fetch the sorted array pointer (exported for help_config.c's use ONLY)
 */



/*
 * Build the sorted array.  This is split out so that it could be
 * re-executed after startup (eg, we could allow loadable modules to
 * add vars, and then we'd need to re-sort).
 */


/*
 * Add a new GUC variable to the list of known variables. The
 * list is expanded if needed.
 */


/*
 * Create and add a placeholder variable for a custom variable name.
 */


/*
 * Look up option NAME.  If it exists, return a pointer to its record,
 * else return NULL.  If create_placeholders is TRUE, we'll create a
 * placeholder record for a valid-looking custom variable name.
 */



/*
 * comparator for qsorting and bsearching guc_variables array
 */


/*
 * the bare comparison function for GUC names
 */



/*
 * Initialize GUC options during program startup.
 *
 * Note that we cannot read the config file yet, since we have not yet
 * processed command-line switches.
 */


/*
 * Assign any GUC values that can come from the server's environment.
 *
 * This is called from InitializeGUCOptions, and also from ProcessConfigFile
 * to deal with the possibility that a setting has been removed from
 * postgresql.conf and should now get a value from the environment.
 * (The latter is a kludge that should probably go away someday; if so,
 * fold this back into InitializeGUCOptions.)
 */


/*
 * Initialize one GUC option variable to its compiled-in default.
 *
 * Note: the reason for calling check_hooks is not that we think the boot_val
 * might fail, but that the hooks might wish to compute an "extra" struct.
 */



/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success; on failure, prints a suitable error message
 * to stderr and returns false.
 */



/*
 * Reset all options to their saved default values (implements RESET ALL)
 */



/*
 * push_old_value
 *		Push previous state during transactional assignment to a GUC variable.
 */



/*
 * Do GUC processing at main transaction start.
 */


/*
 * Enter a new nesting level for GUC values.  This is called at subtransaction
 * start, and when entering a function that has proconfig settings, and in
 * some other places where we want to set GUC variables transiently.
 * NOTE we must not risk error here, else subtransaction start will be unhappy.
 */


/*
 * Do GUC processing at transaction or subtransaction commit or abort, or
 * when exiting a function that has proconfig settings, or when undoing a
 * transient assignment to some GUC variables.  (The name is thus a bit of
 * a misnomer; perhaps it should be ExitGUCNestLevel or some such.)
 * During abort, we discard all GUC settings that were applied at nesting
 * levels >= nestLevel.  nestLevel == 1 corresponds to the main transaction.
 */



/*
 * Start up automatic reporting of changes to variables marked GUC_REPORT.
 * This is executed at completion of backend startup.
 */


/*
 * ReportGUCOption: if appropriate, transmit option value to frontend
 */


/*
 * Convert a value from one of the human-friendly units ("kB", "min" etc.)
 * to the given base unit.  'value' and 'unit' are the input value and unit
 * to convert from.  The converted value is stored in *base_value.
 *
 * Returns true on success, false if the input unit is not recognized.
 */


/*
 * Convert a value in some base unit to a human-friendly unit.  The output
 * unit is chosen so that it's the greatest unit that can represent the value
 * without loss.  For example, if the base unit is GUC_UNIT_KB, 1024 is
 * converted to 1 MB, but 1025 is represented as 1025 kB.
 */



/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, optionally followed by
 * a unit name if "flags" indicates a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 *	HINT message, or NULL if no hint provided.
 */




/*
 * Try to parse value as a floating point number in the usual format.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */



/*
 * Lookup the name for an enum option with the selected value.
 * Should only ever be called with known-valid values, so throws
 * an elog(ERROR) if the enum option is not found.
 *
 * The returned string is a pointer to static data and not
 * allocated for modification.
 */



/*
 * Lookup the value for an enum option with the selected name
 * (case-insensitive).
 * If the enum option is found, sets the retval value and returns
 * true. If it's not found, return FALSE and retval is set to 0.
 */



/*
 * Return a list of all available options for an enum, excluding
 * hidden ones, separated by the given separator.
 * If prefix is non-NULL, it is added before the first enum value.
 * If suffix is non-NULL, it is added to the end of the string.
 */


/*
 * Parse and validate a proposed value for the specified configuration
 * parameter.
 *
 * This does built-in checks (such as range limits for an integer parameter)
 * and also calls any check hook the parameter may have.
 *
 * record: GUC variable's info record
 * name: variable name (should match the record of course)
 * value: proposed value, as a string
 * source: identifies source of value (check hooks may need this)
 * elevel: level to log any error reports at
 * newval: on success, converted parameter value is returned here
 * newextra: on success, receives any "extra" data returned by check hook
 *	(caller must initialize *newextra to NULL)
 *
 * Returns true if OK, false if not (or throws error, if elevel >= ERROR)
 */



/*
 * Sets option `name' to given value.
 *
 * The value should be a string, which will be parsed and converted to
 * the appropriate data type.  The context and source parameters indicate
 * in which context this function is being called, so that it can apply the
 * access restrictions properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == PGC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * elevel should normally be passed as zero, allowing this function to make
 * its standard choice of ereport level.  However some callers need to be
 * able to override that choice; they should pass the ereport level to use.
 *
 * Return value:
 *	+1: the value is valid and was successfully applied.
 *	0:	the name or value is invalid (but see below).
 *	-1: the value was not applied because of context, priority, or changeVal.
 *
 * If there is an error (non-existing option, invalid value) then an
 * ereport(ERROR) is thrown *unless* this is called for a source for which
 * we don't want an ERROR (currently, those are defaults, the config file,
 * and per-database or per-user settings, as well as callers who specify
 * a less-than-ERROR elevel).  In those cases we write a suitable error
 * message via ereport() and return 0.
 *
 * See also SetConfigOption for an external interface.
 */
#define newval (newval_union.boolval)
#undef newval
#define newval (newval_union.intval)
#undef newval
#define newval (newval_union.realval)
#undef newval
#define newval (newval_union.stringval)
#undef newval
#define newval (newval_union.enumval)
#undef newval


/*
 * Set the fields for source file and line number the setting came from.
 */


/*
 * Set a config option to the given value.
 *
 * See also set_config_option; this is just the wrapper to be called from
 * outside GUC.  (This function should be used when possible, because its API
 * is more stable than set_config_option's.)
 *
 * Note: there is no support here for setting source file/line, as it
 * is currently not needed.
 */




/*
 * Fetch the current value of the option `name', as a string.
 *
 * If the option doesn't exist, return NULL if missing_ok is true (NOTE that
 * this cannot be distinguished from a string variable with a NULL value!),
 * otherwise throw an ereport and don't return.
 *
 * If restrict_superuser is true, we also enforce that only superusers can
 * see GUC_SUPERUSER_ONLY variables.  This should only be passed as true
 * in user-driven calls.
 *
 * The string is *not* allocated for modification and is really only
 * valid until the next call to configuration related functions.
 */


/*
 * Get the RESET value associated with the given option.
 *
 * Note: this is not re-entrant, due to use of static result buffer;
 * not to mention that a string variable could have its reset_val changed.
 * Beware of assuming the result value is good for very long.
 */



/*
 * flatten_set_variable_args
 *		Given a parsenode List as emitted by the grammar for SET,
 *		convert to the flat string representation used by GUC.
 *
 * We need to be told the name of the variable the args are for, because
 * the flattening rules vary (ugh).
 *
 * The result is NULL if args is NIL (ie, SET ... TO DEFAULT), otherwise
 * a palloc'd string.
 */


/*
 * Write updated configuration parameter values into a temporary file.
 * This function traverses the list of parameters and quotes the string
 * values before writing them.
 */


/*
 * Update the given list of configuration parameters, adding, replacing
 * or deleting the entry for item "name" (delete if "value" == NULL).
 */



/*
 * Execute ALTER SYSTEM statement.
 *
 * Read the old PG_AUTOCONF_FILENAME file, merge in the new variable value,
 * and write out an updated file.  If the command is ALTER SYSTEM RESET ALL,
 * we can skip reading the old file and just write an empty file.
 *
 * An LWLock is used to serialize updates of the configuration file.
 *
 * In case of an error, we leave the original automatic
 * configuration file (PG_AUTOCONF_FILENAME) intact.
 */


/*
 * SET command
 */


/*
 * Get the value to assign for a VariableSetStmt, or NULL if it's RESET.
 * The result is palloc'd.
 *
 * This is exported for use by actions such as ALTER ROLE SET.
 */


/*
 * SetPGVariable - SET command exported as an easily-C-callable function.
 *
 * This provides access to SET TO value, as well as SET TO DEFAULT (expressed
 * by passing args == NIL), but not SET FROM CURRENT functionality.
 */


/*
 * SET command wrapped as a SQL callable function.
 */



/*
 * Common code for DefineCustomXXXVariable subroutines: allocate the
 * new variable's config struct and fill in generic fields.
 */


/*
 * Common code for DefineCustomXXXVariable subroutines: insert the new
 * variable into the GUC variable array, replacing any placeholder.
 */


/*
 * Recursive subroutine for define_custom_variable: reapply non-reset values
 *
 * We recurse so that the values are applied in the same order as originally.
 * At each recursion level, apply the upper-level value (passed in) in the
 * fashion implied by the stack entry.
 */















/*
 * SHOW command
 */





/*
 * SHOW command
 */


/*
 * SHOW ALL command
 */


/*
 * Return GUC variable value by name; optionally return canonical
 * form of name.  Return value is palloc'd.
 */


/*
 * Return GUC variable value by variable number; optionally return canonical
 * form of name.  Return value is palloc'd.
 */


/*
 * Return the total number of GUC variables
 */


/*
 * show_config_by_name - equiv to SHOW X command but implemented as
 * a function.
 */


/*
 * show_all_settings - equiv to SHOW ALL command but implemented as
 * a Table Function.
 */
#define NUM_PG_SETTINGS_ATTS	17



/*
 * show_all_file_settings
 *
 * Returns a table of all parameter settings in all configuration files
 * which includes the config file pathname, the line number, a sequence number
 * indicating the order in which the settings were encountered, the parameter
 * name and value, a bool showing if the value could be applied, and possibly
 * an associated error message.  (For problems such as syntax errors, the
 * parameter name/value might be NULL.)
 *
 * Note: no filtering is done here, instead we depend on the GRANT system
 * to prevent unprivileged users from accessing this function or the view
 * built on top of it.
 */
#define NUM_PG_FILE_SETTINGS_ATTS 7




#ifdef EXEC_BACKEND

/*
 *	These routines dump out all non-default GUC options into a binary
 *	file that is read by all exec'ed backends.  The format is:
 *
 *		variable name, string, null terminated
 *		variable value, string, null terminated
 *		variable sourcefile, string, null terminated (empty if none)
 *		variable sourceline, integer
 *		variable source, integer
 *		variable scontext, integer
 */
static void
write_one_nondefault_variable(FILE *fp, struct config_generic * gconf)
{
	if (gconf->source == PGC_S_DEFAULT)
		return;

	fprintf(fp, "%s", gconf->name);
	fputc(0, fp);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) gconf;

				if (*conf->variable)
					fprintf(fp, "true");
				else
					fprintf(fp, "false");
			}
			break;

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;

				fprintf(fp, "%d", *conf->variable);
			}
			break;

		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) gconf;

				fprintf(fp, "%.17g", *conf->variable);
			}
			break;

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;

				fprintf(fp, "%s", *conf->variable);
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;

				fprintf(fp, "%s",
						config_enum_lookup_by_value(conf, *conf->variable));
			}
			break;
	}

	fputc(0, fp);

	if (gconf->sourcefile)
		fprintf(fp, "%s", gconf->sourcefile);
	fputc(0, fp);

	fwrite(&gconf->sourceline, 1, sizeof(gconf->sourceline), fp);
	fwrite(&gconf->source, 1, sizeof(gconf->source), fp);
	fwrite(&gconf->scontext, 1, sizeof(gconf->scontext), fp);
}

void
write_nondefault_variables(GucContext context)
{
	int			elevel;
	FILE	   *fp;
	int			i;

	Assert(context == PGC_POSTMASTER || context == PGC_SIGHUP);

	elevel = (context == PGC_SIGHUP) ? LOG : ERROR;

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS_NEW, "w");
	if (!fp)
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	for (i = 0; i < num_guc_variables; i++)
	{
		write_one_nondefault_variable(fp, guc_variables[i]);
	}

	if (FreeFile(fp))
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	/*
	 * Put new file in place.  This could delay on Win32, but we don't hold
	 * any exclusive locks.
	 */
	rename(CONFIG_EXEC_PARAMS_NEW, CONFIG_EXEC_PARAMS);
}


/*
 *	Read string, including null byte from file
 *
 *	Return NULL on EOF and nothing read
 */
static char *
read_string_with_null(FILE *fp)
{
	int			i = 0,
				ch,
				maxlen = 256;
	char	   *str = NULL;

	do
	{
		if ((ch = fgetc(fp)) == EOF)
		{
			if (i == 0)
				return NULL;
			else
				elog(FATAL, "invalid format of exec config params file");
		}
		if (i == 0)
			str = guc_malloc(FATAL, maxlen);
		else if (i == maxlen)
			str = guc_realloc(FATAL, str, maxlen *= 2);
		str[i++] = ch;
	} while (ch != 0);

	return str;
}


/*
 *	This routine loads a previous postmaster dump of its non-default
 *	settings.
 */
void
read_nondefault_variables(void)
{
	FILE	   *fp;
	char	   *varname,
			   *varvalue,
			   *varsourcefile;
	int			varsourceline;
	GucSource	varsource;
	GucContext	varscontext;

	/*
	 * Assert that PGC_BACKEND/PGC_SU_BACKEND case in set_config_option() will
	 * do the right thing.
	 */
	Assert(IsInitProcessingMode());

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS, "r");
	if (!fp)
	{
		/* File not found is fine */
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": %m",
							CONFIG_EXEC_PARAMS)));
		return;
	}

	for (;;)
	{
		struct config_generic *record;

		if ((varname = read_string_with_null(fp)) == NULL)
			break;

		if ((record = find_option(varname, true, FATAL)) == NULL)
			elog(FATAL, "failed to locate variable \"%s\" in exec config params file", varname);

		if ((varvalue = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if ((varsourcefile = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsourceline, 1, sizeof(varsourceline), fp) != sizeof(varsourceline))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsource, 1, sizeof(varsource), fp) != sizeof(varsource))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varscontext, 1, sizeof(varscontext), fp) != sizeof(varscontext))
			elog(FATAL, "invalid format of exec config params file");

		(void) set_config_option(varname, varvalue,
								 varscontext, varsource,
								 GUC_ACTION_SET, true, 0, true);
		if (varsourcefile[0])
			set_config_sourcefile(varname, varsourcefile, varsourceline);

		free(varname);
		free(varvalue);
		free(varsourcefile);
	}

	FreeFile(fp);
}
#endif   /* EXEC_BACKEND */

/*
 * can_skip_gucvar:
 * When serializing, determine whether to skip this GUC.  When restoring, the
 * negation of this test determines whether to restore the compiled-in default
 * value before processing serialized values.
 *
 * A PGC_S_DEFAULT setting on the serialize side will typically match new
 * postmaster children, but that can be false when got_SIGHUP == true and the
 * pending configuration change modifies this setting.  Nonetheless, we omit
 * PGC_S_DEFAULT settings from serialization and make up for that by restoring
 * defaults before applying serialized values.
 *
 * PGC_POSTMASTER variables always have the same value in every child of a
 * particular postmaster.  Most PGC_INTERNAL variables are compile-time
 * constants; a few, like server_encoding and lc_ctype, are handled specially
 * outside the serialize/restore procedure.  Therefore, SerializeGUCState()
 * never sends these, and RestoreGUCState() never changes them.
 */


/*
 * estimate_variable_size:
 * Estimate max size for dumping the given GUC variable.
 */


/*
 * EstimateGUCStateSpace:
 * Returns the size needed to store the GUC state for the current process
 */


/*
 * do_serialize:
 * Copies the formatted string into the destination.  Moves ahead the
 * destination pointer, and decrements the maxbytes by that many bytes. If
 * maxbytes is not sufficient to copy the string, error out.
 */


/* Binary copy version of do_serialize() */


/*
 * serialize_variable:
 * Dumps name, value and other information of a GUC variable into destptr.
 */


/*
 * SerializeGUCState:
 * Dumps the complete GUC state onto the memory location at start_address.
 */


/*
 * read_gucstate:
 * Actually it does not read anything, just returns the srcptr. But it does
 * move the srcptr past the terminating zero byte, so that the caller is ready
 * to read the next string.
 */


/* Binary read version of read_gucstate(). Copies into dest */


/*
 * RestoreGUCState:
 * Reads the GUC state at the specified address and updates the GUCs with the
 * values read from the GUC state.
 */


/*
 * A little "long argument" simulation, although not quite GNU
 * compliant. Takes a string of the form "some-option=some value" and
 * returns name = "some_option" and value = "some value" in malloc'ed
 * storage. Note that '-' is converted to '_' in the option name. If
 * there is no '=' in the input string then value will be NULL.
 */



/*
 * Handle options fetched from pg_db_role_setting.setconfig,
 * pg_proc.proconfig, etc.  Caller must specify proper context/source/action.
 *
 * The array parameter must be an array of TEXT (it must not be NULL).
 */



/*
 * Add an entry to an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.
 */



/*
 * Delete an entry from an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.  Also, if the return value
 * is NULL then a null should be stored.
 */



/*
 * Given a GUC array, delete all settings from it that our permission
 * level allows: if superuser, delete them all; if regular user, only
 * those that are PGC_USERSET
 */


/*
 * Validate a proposed option setting for GUCArrayAdd/Delete/Reset.
 *
 * name is the option name.  value is the proposed value for the Add case,
 * or NULL for the Delete/Reset cases.  If skipIfNoPermissions is true, it's
 * not an error to have no permissions to set the option.
 *
 * Returns TRUE if OK, FALSE if skipIfNoPermissions is true and user does not
 * have permission to change this option (all other error cases result in an
 * error being thrown).
 */



/*
 * Called by check_hooks that want to override the normal
 * ERRCODE_INVALID_PARAMETER_VALUE SQLSTATE for check hook failures.
 *
 * Note that GUC_check_errmsg() etc are just macros that result in a direct
 * assignment to the associated variables.  That is ugly, but forced by the
 * limitations of C's macro mechanisms.
 */



/*
 * Convenience functions to manage calling a variable's check_hook.
 * These mostly take care of the protocol for letting check hooks supply
 * portions of the error report on failure.
 */












/*
 * check_hook, assign_hook and show_hook subroutines
 */

#ifdef HAVE_SYSLOG
#endif
#ifdef WIN32
#endif



#ifdef HAVE_SYSLOG
#endif

#ifdef HAVE_SYSLOG
#endif






#ifndef USE_BONJOUR
#endif

#ifndef USE_SSL
#endif











/*
 * pg_timezone_abbrev_initialize --- set default value if not done already
 *
 * This is called after initial loading of postgresql.conf.  If no
 * timezone_abbreviations setting was found therein, select default.
 * If a non-default value is already installed, nothing will happen.
 *
 * This can also be called from ProcessConfigFile to establish the default
 * value after a postgresql.conf entry for it is removed.
 */
























#ifdef USE_PREFETCH
#else
#endif   /* USE_PREFETCH */

#ifdef USE_PREFETCH
#endif   /* USE_PREFETCH */













#include "guc-file.c"
