/* @(#)driver.c	2.1.8.4 */
/* main driver for dss banchmark */

#define DECLARER                   /* EXTERN references get defined here */
#define NO_FUNC  (int (*)()) NULL  /* to clean up tdefs */
#define NO_LFUNC (long (*)()) NULL /* to clean up tdefs */

#include "include/driver.hpp"

#include "include/config.h"

#include <stdlib.h>
#if (defined(_POSIX_) || !defined(WIN32)) /* Change for Windows NT */
#ifndef DOS
#ifdef MAC
#define _POSIX_C_SOURCE 200809L
#endif
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#endif /* WIN32 */
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h> /* */
#include <string.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32) && !defined(_POSIX_))
#include <process.h>
#pragma warning(disable : 4201)
#pragma warning(disable : 4214)
#pragma warning(disable : 4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX

#include "windows.h"

#pragma warning(default : 4201)
#pragma warning(default : 4214)
#endif

#include "include/bcd2.h"
#include "include/dss.h"
#include "include/dsstypes.h"

/*
 * Function prototypes
 */
void usage(void);
int prep_direct(char *);
int close_direct(void);
void kill_load(void);
int pload(int tbl, ssb_appender *appender);
void gen_tbl(int tnum, long start, long count, long upd_num, ssb_appender *appender);
int pr_drange(int tbl, long min, long cnt, long num);
int set_files(int t, int pload);
int partial(int, int, ssb_appender *);

extern int optind, opterr;
extern char *optarg;
long rowcnt = 0, minrow = 0, upd_num = 0;
double flt_scale;
#if (defined(WIN32) && !defined(_POSIX_))
char *spawn_args[25];
#endif

/*
 * general table descriptions. See dss.h for details on structure
 * NOTE: tables with no scaling info are scaled according to
 * another table
 *
 *
 * the following is based on the tdef structure defined in dss.h as:
 * typedef struct
 * {
 * char     *name;            -- name of the table;
 *                               flat file output in <name>.tbl
 * long      base;            -- base scale rowcount of table;
 *                               0 if derived
 * int       (*header) ();    -- function to prep output
 * int       (*loader[2]) (); -- functions to present output
 * long      (*gen_seed) ();  -- functions to seed the RNG
 * int       (*verify) ();    -- function to verfiy the data set without building it
 * int       child;           -- non-zero if there is an associated detail table
 * unsigned long vtotal;      -- "checksum" total
 * }         tdef;
 *
 */

/*
 * flat file print functions; used with -F(lat) option
 */
int pr_cust(customer_t *c, int mode);
int pr_part(part_t *p, int mode);
int pr_supp(supplier_t *s, int mode);
int pr_line(order_t *o, int mode);

/*
 * inline load functions; used with -D(irect) option
 */
int ld_cust(customer_t *c, int mode);
int ld_part(part_t *p, int mode);
int ld_supp(supplier_t *s, int mode);

/*todo: get rid of ld_order*/
int ld_line(order_t *o, int mode);
int ld_order(order_t *o, int mode);

/*
 * seed generation functions; used with '-O s' option
 */
long sd_cust(int child, long skip_count);
long sd_part(int child, long skip_count);
long sd_supp(int child, long skip_count);

long sd_line(int child, long skip_count);
long sd_order(int child, long skip_count);

/*
 * header output functions); used with -h(eader) option
 */
int hd_cust(FILE *f);
int hd_part(FILE *f);
int hd_supp(FILE *f);
int hd_line(FILE *f);

/*
 * data verfication functions; used with -O v option
 */
int vrf_cust(customer_t *c, int mode);
int vrf_part(part_t *p, int mode);
int vrf_supp(supplier_t *s, int mode);
int vrf_line(order_t *o, int mode);
int vrf_order(order_t *o, int mode);
int vrf_date(ssb_date_t, int mode);

typedef int (*func_ptr)();

tdef tdefs[] = {

    {"part.tbl",
     "part table",
     200000,
     hd_part,
     {(func_ptr)pr_part, (func_ptr)ld_part},
     sd_part,
     (func_ptr)vrf_part,
     PSUPP,
     0},
    {0, 0, 0, 0, {0, 0}, 0, 0, 0, 0},
    {"supplier.tbl",
     "suppliers table",
     2000,
     hd_supp,
     {(func_ptr)pr_supp, (func_ptr)ld_supp},
     sd_supp,
     (func_ptr)vrf_supp,
     NONE,
     0},

    {"customer.tbl",
     "customers table",
     30000,
     hd_cust,
     {(func_ptr)pr_cust, (func_ptr)ld_cust},
     sd_cust,
     (func_ptr)vrf_cust,
     NONE,
     0},
    {"date.tbl", "date table", 2556, 0, {(func_ptr)pr_date, (func_ptr)ld_date}, 0, (func_ptr)vrf_date, NONE, 0},
    /*line order is SF*1,500,000, however due to the implementation
      the base here is 150,000 instead if 1500,000*/
    {"lineorder.tbl",
     "lineorder table",
     150000,
     hd_line,
     {(func_ptr)pr_line, (func_ptr)ld_line},
     sd_line,
     (func_ptr)vrf_line,
     NONE,
     0},
    {0, 0, 0, 0, {0, 0}, 0, 0, 0, 0},
    {0, 0, 0, 0, {0, 0}, 0, 0, 0, 0},
    {0, 0, 0, 0, {0, 0}, 0, 0, 0, 0},
    {0, 0, 0, 0, {0, 0}, 0, 0, 0, 0},
};
int *pids;

/*
 * routines to handle the graceful cleanup of multi-process loads
 */

void stop_proc(int signum) {
	exit(0);
}

void kill_load(void) {
	int i;

#if !defined(U2200) && !defined(DOS)
	for (i = 0; i < children; i++)
		if (pids[i])
			KILL(pids[i]);
#endif /* !U2200 && !DOS */
	return;
}

/*
 * re-set default output file names
 */
int set_files(int i, int pload) {
	char line[80], *new_name;

	if (table & (1 << i))
	child_table: {
		if (pload != -1)
			sprintf(line, "%s.%d", tdefs[i].name, pload);
		else
			return (0); // Just use the default placement (i.e. "./<table>.tbl")

		new_name = (char *)malloc(strlen(line) + 1);
		MALLOC_CHECK(new_name);
		strcpy(new_name, line);
		tdefs[i].name = new_name;
		if (tdefs[i].child != NONE) {
			i = tdefs[i].child;
			tdefs[i].child = NONE;
			goto child_table;
		}
	}

		return (0);
}

/*
 * read the distributions needed in the benchamrk
 */
void load_dists(void) {
	read_dist("p_cntr", &p_cntr_set);
	read_dist("colors", &colors);
	read_dist("p_types", &p_types_set);
	read_dist("nations", &nations);
	read_dist("regions", &regions);
	read_dist("o_oprio", &o_priority_set);
	read_dist("instruct", &l_instruct_set);
	read_dist("smode", &l_smode_set);
	read_dist("category", &l_category_set);
	read_dist("rflag", &l_rflag_set);
	read_dist("msegmnt", &c_mseg_set);

	/* load the distributions that contain text generation */
	read_dist("nouns", &nouns);
	read_dist("verbs", &verbs);
	read_dist("adjectives", &adjectives);
	read_dist("adverbs", &adverbs);
	read_dist("auxillaries", &auxillaries);
	read_dist("terminators", &terminators);
	read_dist("articles", &articles);
	read_dist("prepositions", &prepositions);
	read_dist("grammar", &grammar);
	read_dist("np", &np);
	read_dist("vp", &vp);
}

/*
 * generate a particular table
 */
void gen_tbl(int tnum, long start, long count, long upd_num, ssb_appender *appender) {
	static order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	ssb_date_t dt;
	static int completed = 0;
	static int init = 0;
	long i;

	int rows_per_segment = 0;
	int rows_this_segment = -1;
	int residual_rows = 0;

	if (insert_segments) {
		rows_per_segment = count / insert_segments;
		residual_rows = count - (rows_per_segment * insert_segments);
	}

	if (init == 0) {
		INIT_HUGE(o.okey);
		for (i = 0; i < O_LCNT_MAX; i++)
			INIT_HUGE(o.lineorders[i].okey);
		init = 1;
	}

	for (i = start; count; count--, i++) {
		LIFENOISE(1000, i);
		row_start(tnum);

		switch (tnum) {
		case LINE:
			mk_order(i, &o, upd_num % 10000);

			if (insert_segments && (upd_num > 0))
				if ((upd_num / 10000) < residual_rows) {
					if ((++rows_this_segment) > rows_per_segment) {
						rows_this_segment = 0;
						upd_num += 10000;
					}
				} else {
					if ((++rows_this_segment) >= rows_per_segment) {
						rows_this_segment = 0;
						upd_num += 10000;
					}
				}

			if (set_seeds == 0)
				if (validate)
					((int (*)(order_t *, int))tdefs[tnum].verify)(&o, 0);
				else
					appender->pr_line(&o, upd_num);
			break;
		case SUPP:
			mk_supp(i, &supp);
			if (set_seeds == 0)
				if (validate)
					((int (*)(supplier_t *, int))tdefs[tnum].verify)(&supp, 0);
				else
					appender->pr_supp(&supp, upd_num);
			break;
		case CUST:
			mk_cust(i, &cust);
			if (set_seeds == 0)
				if (validate)
					((int (*)(customer_t *, int))tdefs[tnum].verify)(&cust, 0);
				else
					appender->pr_cust(&cust, upd_num);
			break;
		case PART:
			mk_part(i, &part);
			if (set_seeds == 0)
				if (validate)
					((int (*)(part_t *, int))tdefs[tnum].verify)(&part, 0);
				else
					appender->pr_part(&part, upd_num);
			break;
		case DATE:
			mk_date(i, &dt);
			if (set_seeds == 0)
				if (validate)
					((int (*)(ssb_date_t *, int))tdefs[tnum].verify)(&dt, 0);
				else
					appender->pr_date(&dt, 0);
			break;
		}
		row_stop(tnum);
		if (set_seeds && (i % tdefs[tnum].base) < 2) {
			printf("\nSeeds for %s at rowcount %ld\n", tdefs[tnum].comment, i);
			dump_seeds(tnum);
		}
	}
	completed |= 1 << tnum;
}

void usage(void) {
	fprintf(stderr, "%s\n%s\n\t%s\n%s %s\n\n", "USAGE:", "dbgen [-{vfFD}] [-O {fhmsv}][-T {pcsdla}]",
	        "[-s <scale>][-C <procs>][-S <step>]", "dbgen [-v] [-O {dfhmr}] [-s <scale>]",
	        "[-U <updates>] [-r <percent>]");

	fprintf(stderr, "-b <s> -- load distributions for <s>\n");
	fprintf(stderr, "-C <n> -- use <n> processes to generate data\n");
	fprintf(stderr, "          [Under DOS, must be used with -S]\n");
	fprintf(stderr, "-D     -- do database load in line\n");
	fprintf(stderr, "-d <n> -- split deletes between <n> files\n");
	fprintf(stderr, "-f     -- force. Overwrite existing files\n");
	fprintf(stderr, "-F     -- generate flat files output\n");
	fprintf(stderr, "-h     -- display this message\n");
	fprintf(stderr, "-i <n> -- split inserts between <n> files\n");
	fprintf(stderr, "-n <s> -- inline load into database <s>\n");
	fprintf(stderr, "-O d   -- generate SQL syntax for deletes\n");
	fprintf(stderr, "-O f   -- over-ride default output file names\n");
	fprintf(stderr, "-O h   -- output files with headers\n");
	fprintf(stderr, "-O m   -- produce columnar output\n");
	fprintf(stderr, "-O r   -- generate key ranges for deletes.\n");
	fprintf(stderr, "-O v   -- Verify data set without generating it.\n");
	fprintf(stderr, "-q     -- enable QUIET mode\n");
	fprintf(stderr, "-r <n> -- updates refresh (n/100)%% of the\n");
	fprintf(stderr, "          data set\n");
	fprintf(stderr, "-s <n> -- set Scale Factor (SF) to  <n> \n");
	fprintf(stderr, "-S <n> -- build the <n>th step of the data/update set\n");

	fprintf(stderr, "-T c   -- generate cutomers dimension table ONLY\n");
	fprintf(stderr, "-T p   -- generate parts dimension table ONLY\n");
	fprintf(stderr, "-T s   -- generate suppliers dimension table ONLY\n");
	fprintf(stderr, "-T d   -- generate date dimension table ONLY\n");
	fprintf(stderr, "-T l   -- generate lineorder fact table ONLY\n");

	fprintf(stderr, "-U <s> -- generate <s> update sets\n");
	fprintf(stderr, "-v     -- enable VERBOSE mode\n");
	fprintf(stderr, "\nTo generate the SF=1 (1GB), validation database population, use:\n");
	fprintf(stderr, "\tdbgen -vfF -s 1\n");
	fprintf(stderr, "\nTo generate updates for a SF=1 (1GB), use:\n");
	fprintf(stderr, "\tdbgen -v -U 1 -s 1\n");
}

/*
 * pload() -- handle the parallel loading of tables
 */
/*
 * int partial(int tbl, int s) -- generate the s-th part of the named tables data
 */
int partial(int tbl, int s, ssb_appender *appender) {
	long rowcnt;
	long extra;

	if (verbose > 0) {
		fprintf(stderr, "\tStarting to load stage %d of %d for %s...", s, children, tdefs[tbl].comment);
	}

	if (direct == 0)
		set_files(tbl, s);

	rowcnt = set_state(tbl, scale, children, s, &extra);

	if (s == children)
		gen_tbl(tbl, rowcnt * (s - 1) + 1, rowcnt + extra, upd_num, appender);
	else
		gen_tbl(tbl, rowcnt * (s - 1) + 1, rowcnt, upd_num, appender);

	if (verbose > 0)
		fprintf(stderr, "done.\n");

	return (0);
}

#ifndef DOS

int pload(int tbl, ssb_appender *appender) {
	int c = 0, i, status;

	if (verbose > 0) {
		fprintf(stderr, "Starting %d children to load %s", children, tdefs[tbl].comment);
	}
	for (c = 0; c < children; c++) {
		pids[c] = SPAWN();
		if (pids[c] == -1) {
			perror("Child loader not created");
			kill_load();
			exit(-1);
		} else if (pids[c] == 0) /* CHILD */
		{
			SET_HANDLER(stop_proc);
			verbose = 0;
			partial(tbl, c + 1, appender);
			exit(0);
		} else if (verbose > 0) /* PARENT */
			fprintf(stderr, ".");
	}

	if (verbose > 0)
		fprintf(stderr, "waiting...");

	c = children;
	while (c) {
		i = WAIT(&status, pids[c - 1]);
		if (i == -1 && children) {
			if (errno == ECHILD)
				fprintf(stderr, "\nCould not wait on pid %d\n", pids[c - 1]);
			else if (errno == EINTR)
				fprintf(stderr, "\nProcess %d stopped abnormally\n", pids[c - 1]);
			else if (errno == EINVAL)
				fprintf(stderr, "\nProgram bug\n");
		}
		if (!WIFEXITED(status)) {
			(void)fprintf(stderr, "\nProcess %d: ", i);
			if (WIFSIGNALED(status)) {
				(void)fprintf(stderr, "rcvd signal %d\n", WTERMSIG(status));
			} else if (WIFSTOPPED(status)) {
				(void)fprintf(stderr, "stopped, signal %d\n", WSTOPSIG(status));
			}
		}
		c--;
	}

	if (verbose > 0)
		fprintf(stderr, "done\n");
	return (0);
}
#endif

void process_options(double scale_f) {
	int option;

	table = 1 << CUST;
	table |= 1 << PART;
	table |= 1 << SUPP;
	table |= 1 << DATE;
	table |= 1 << LINE;

	flt_scale = scale_f;
	if (flt_scale < MIN_SCALE) {
		int i;

		scale = 1;
		for (i = PART; i < REGION; i++) {
			tdefs[i].base *= flt_scale;
			if (tdefs[i].base < 1)
				tdefs[i].base = 1;
		}
	} else
		scale = (long)flt_scale;
	if (scale > MAX_SCALE) {
		fprintf(stderr, "%s %5.0f %s\n\t%s\n\n", "NOTE: Data generation for scale factors >", MAX_SCALE,
		        "GB is still in development,", "and is not yet supported.\n");
		fprintf(stderr, "Your resulting data set MAY NOT BE COMPLIANT!\n");
	}

	if (children != 1 && step == -1) {
		pids = (int *)malloc(children * sizeof(pid_t));
		MALLOC_CHECK(pids)
	}

	return;
}

/*
 * MAIN
 *
 * assumes the existance of getopt() to clean up the command
 * line handling
 */
int gen_main(double scale_f, ssb_appender *appender) {
	int i;

	table = (1 << CUST) | (1 << SUPP) | (1 << NATION) | (1 << REGION) | (1 << PART_PSUPP) | (1 << ORDER_LINE);
	force = 0;
	insert_segments = 0;
	delete_segments = 0;
	insert_orders_segment = 0;
	insert_lineitem_segment = 0;
	delete_segment = 0;
	verbose = 0;
	columnar = 0;
	set_seeds = 0;
	header = 0;
	direct = 0;
	scale = 1;
	flt_scale = 1.0;
	updates = 0;
	refresh = UPD_PCT;
	step = -1;
	tdefs[LINE].base *= ORDERS_PER_CUST; /* have to do this after init */
	fnames = 0;
	db_name = NULL;
	gen_sql = 0;
	gen_rng = 0;
	children = 1;
	d_path = NULL;

#ifdef NO_SUPPORT
	signal(SIGINT, exit);
#endif /* NO_SUPPORT */
	process_options(scale_f);
#if (defined(WIN32) && !defined(_POSIX_))
	for (i = 0; i < ac; i++) {
		spawn_args[i] = malloc((strlen(av[i]) + 1) * sizeof(char));
		MALLOC_CHECK(spawn_args[i]);
		strcpy(spawn_args[i], av[i]);
	}
	spawn_args[ac] = NULL;
#endif

	if (verbose >= 0) {
		fprintf(stderr, "%s Population Generator (Version %d.%d.%d%s)\n", NAME, VERSION, RELEASE, MODIFICATION, PATCH);
		fprintf(stderr, "Copyright %s %s\n", TPC, C_DATES);
	}

	load_dists();
	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	/**
	** actual data generation section starts here
	**/
	/*
	 * open database connection or set all the file names, as appropriate
	 */
	for (i = PART; i <= REGION; i++) {
		if (table & (1 << i))
			if (set_files(i, -1)) {
				fprintf(stderr, "Load aborted!\n");
				exit(1);
			}
	}

	/*
	 * traverse the tables, invoking the appropriate data generation routine for any to be built
	 */
	for (i = PART; i <= REGION; i++)
		if (table & (1 << i)) {
			if (children > 1 && i < NATION)
				if (step >= 0) {
					if (validate) {
						INTERNAL_ERROR("Cannot validate parallel data generation");
					} else
						partial(i, step, appender);
				} else {
					if (validate) {
						INTERNAL_ERROR("Cannot validate parallel data generation");
					} else
						pload(i, appender);
				}
			else {
				minrow = 1;
				if (i < NATION)
					rowcnt = tdefs[i].base * scale;
				else
					rowcnt = tdefs[i].base;
				if (i == PART) {
					rowcnt = tdefs[i].base * (floor(1 + log((double)(scale)) / (log(2))));
				}
				if (i == DATE) {
					rowcnt = tdefs[i].base;
				}
				if (verbose > 0)
					fprintf(stderr, "%s data for %s [pid: %ld]", (validate) ? "Validating" : "Generating",
					        tdefs[i].comment, DSS_PROC);
				gen_tbl(i, minrow, rowcnt, upd_num, appender);
				if (verbose > 0)
					fprintf(stderr, "done.\n");
			}
			if (validate)
				printf("Validation checksum for %s at %d GB: %0x\n", tdefs[i].name, scale, tdefs[i].vtotal);
		}

	if (direct)
		close_direct();

	return (0);
}
