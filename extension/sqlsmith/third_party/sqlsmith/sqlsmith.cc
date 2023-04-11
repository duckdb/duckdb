#include <chrono>
#include <iostream>

#include <regex>

#include <thread>
#include <typeinfo>

#include <thread>
#include <typeinfo>

#include "grammar.hh"
#include "random.hh"
#include "relmodel.hh"
#include "schema.hh"

#include "dump.hh"
#include "dut.hh"
#include "impedance.hh"
#include "log.hh"

#include "duckdb.hh"
#include "sqlsmith.hh"

#include "duckdb/common/vector.hpp"

using namespace std;

using namespace std::chrono;

extern "C" {
#include <signal.h>
#include <stdlib.h>
#if (!defined(_WIN32) && !defined(WIN32)) || defined(__MINGW32__)
#include <unistd.h>
#define GETPID ::getpid
#else
#include <windows.h>
#define GETPID (int)GetCurrentProcessId
#endif
}

/* make the cerr logger globally accessible so we can emit one last
   report on SIGINT */
cerr_logger *global_cerr_logger;

extern "C" void cerr_log_handler(int) {
	if (global_cerr_logger)
		global_cerr_logger->report();
	exit(1);
}

namespace duckdb_sqlsmith {

int32_t run_sqlsmith(duckdb::DatabaseInstance &database, SQLSmithOptions opt) {
	//
	//	if (options.count("help")) {
	//		cerr << "    --duckdb=URI         SQLite database to send queries to" << endl
	//		     << "    --seed=int           seed RNG with specified int instead "
	//		        "of PID"
	//		     << endl
	//		     << "    --dump-all-queries   print queries as they are generated" << endl
	//		     << "    --dump-all-graphs    dump generated ASTs" << endl
	//		     << "    --dry-run            print queries instead of executing "
	//		        "them"
	//		     << endl
	//		     << "    --exclude-catalog    don't generate queries using catalog "
	//		        "relations"
	//		     << endl
	//		     << "    --max-queries=long   terminate after generating this many "
	//		        "queries"
	//		     << endl
	//		     << "    --rng-state=string    deserialize dumped rng state" << endl
	//		     << "    --verbose            emit progress output" << endl
	//		     << "    --version            print version information and exit" << endl
	//		     << "    --help               print available command line options "
	//		        "and exit"
	//		     << endl;
	//		return 0;
	//	} else if (options.count("version")) {
	//		return 0;
	//	}

	try {
		shared_ptr<schema> schema;
		schema = make_shared<schema_duckdb>(database, opt.exclude_catalog, opt.verbose_output);

		scope scope;
		long queries_generated = 0;
		schema->fill_scope(scope);

		//		if (options.count("rng-state")) {
		//			istringstream(options["rng-state"]) >> smith::rng;
		//		} else {
		smith::rng.seed(opt.seed >= 0 ? opt.seed : GETPID());
		//		}

		duckdb::vector<shared_ptr<logger>> loggers;

		loggers.push_back(make_shared<impedance_feedback>());

		if (opt.verbose_output) {
			auto l = make_shared<cerr_logger>();
			global_cerr_logger = &*l;
			loggers.push_back(l);
			signal(SIGINT, cerr_log_handler);
		}

		if (opt.dump_all_graphs)
			loggers.push_back(make_shared<ast_logger>());

		if (opt.dump_all_queries)
			loggers.push_back(make_shared<query_dumper>());

		//		if (options.count("dry-run")) {
		//			while (1) {
		//				shared_ptr<prod> gen = statement_factory(&scope);
		//				gen->out(cout);
		//				for (auto l : loggers)
		//					l->generated(*gen);
		//				cout << ";" << endl;
		//				queries_generated++;
		//
		//				if (opt.max_queries >= 0 && (queries_generated >= opt.max_queries))
		//					return 0;
		//			}
		//		}

		shared_ptr<dut_base> dut;

		dut = make_shared<dut_duckdb>(database);

		if (opt.verbose_output)
			cerr << "Running queries..." << endl;

		bool has_complete_log = !opt.complete_log.empty();
		bool has_log = !opt.log.empty();
		ofstream complete_log;
		if (has_complete_log) {
			complete_log.open(opt.complete_log);
		}
		while (1) /* Loop to recover connection loss */
		{
			while (1) { /* Main loop */

				if (opt.max_queries >= 0 && (++queries_generated > opt.max_queries)) {
					if (global_cerr_logger)
						global_cerr_logger->report();
					return 0;
				}

				/* Invoke top-level production to generate AST */
				shared_ptr<prod> gen = statement_factory(&scope);

				for (auto l : loggers)
					l->generated(*gen);

				/* Generate SQL from AST */
				ostringstream s;
				gen->out(s);

				// write the query to the complete log that has all the
				// queries
				if (has_complete_log) {
					complete_log << s.str() << ";" << endl;
					complete_log.flush();
				}

				// write the last-executed query to a separate log file
				if (has_log) {
					ofstream out_file;
					out_file.open(opt.log);
					out_file << s.str() << ";" << endl;
					out_file.close();
				}

				/* Try to execute it */
				try {
					dut->test(s.str());
					for (auto l : loggers)
						l->executed(*gen);
				} catch (const dut::failure &e) {
					for (auto l : loggers)
						try {
							l->error(*gen, e);
						} catch (runtime_error &e) {
							cerr << endl << "log failed: " << typeid(*l).name() << ": " << e.what() << endl;
						}
					if ((dynamic_cast<const dut::broken *>(&e))) {
						/* re-throw to outer loop */
						throw;
					}
				}
			}
		}
	} catch (const exception &e) {
		cerr << e.what() << endl;
		exit(1);
	}
}

} // namespace duckdb_sqlsmith
