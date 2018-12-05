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

using namespace std;

using namespace std::chrono;

extern "C" {
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
}

/* make the cerr logger globally accessible so we can emit one last
   report on SIGINT */
cerr_logger *global_cerr_logger;

extern "C" void cerr_log_handler(int) {
	if (global_cerr_logger)
		global_cerr_logger->report();
	exit(1);
}

int main(int argc, char *argv[]) {
	map<string, string> options;
	regex optregex("--(help|verbose|target|duckdb|monetdb|version|dump-"
	               "all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-"
	               "state|exclude-catalog)(?:=((?:.|\n)*))?");

	for (char **opt = argv + 1; opt < argv + argc; opt++) {
		smatch match;
		string s(*opt);
		if (regex_match(s, match, optregex)) {
			options[string(match[1])] = match[2];
		} else {
			cerr << "Cannot parse option: " << *opt << endl;
			options["help"] = "";
		}
	}

	if (options.count("help")) {
		cerr << "    --duckdb=URI         SQLite database to send queries to" << endl
		     << "    --seed=int           seed RNG with specified int instead "
		        "of PID"
		     << endl
		     << "    --dump-all-queries   print queries as they are generated" << endl
		     << "    --dump-all-graphs    dump generated ASTs" << endl
		     << "    --dry-run            print queries instead of executing "
		        "them"
		     << endl
		     << "    --exclude-catalog    don't generate queries using catalog "
		        "relations"
		     << endl
		     << "    --max-queries=long   terminate after generating this many "
		        "queries"
		     << endl
		     << "    --rng-state=string    deserialize dumped rng state" << endl
		     << "    --verbose            emit progress output" << endl
		     << "    --version            print version information and exit" << endl
		     << "    --help               print available command line options "
		        "and exit"
		     << endl;
		return 0;
	} else if (options.count("version")) {
		return 0;
	}

	try {
		shared_ptr<schema> schema;
		if (options.count("duckdb")) {
			schema = make_shared<schema_duckdb>(options["duckdb"], options.count("exclude-catalog"));
		} else {
			cerr << "No DuckDB database specified!" << endl;
			return 1;
		}

		scope scope;
		long queries_generated = 0;
		schema->fill_scope(scope);

		if (options.count("rng-state")) {
			istringstream(options["rng-state"]) >> smith::rng;
		} else {
			smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());
		}

		vector<shared_ptr<logger>> loggers;

		loggers.push_back(make_shared<impedance_feedback>());

		if (options.count("verbose")) {
			auto l = make_shared<cerr_logger>();
			global_cerr_logger = &*l;
			loggers.push_back(l);
			signal(SIGINT, cerr_log_handler);
		}

		if (options.count("dump-all-graphs"))
			loggers.push_back(make_shared<ast_logger>());

		if (options.count("dump-all-queries"))
			loggers.push_back(make_shared<query_dumper>());

		if (options.count("dry-run")) {
			while (1) {
				shared_ptr<prod> gen = statement_factory(&scope);
				gen->out(cout);
				for (auto l : loggers)
					l->generated(*gen);
				cout << ";" << endl;
				queries_generated++;

				if (options.count("max-queries") && (queries_generated >= stol(options["max-queries"])))
					return 0;
			}
		}

		shared_ptr<dut_base> dut;

		if (options.count("duckdb")) {
			dut = make_shared<dut_duckdb>(options["duckdb"]);
		} else {
			cerr << "No DuckDB database specified!" << endl;
			return 1;
		}

		cerr << "Running queries..." << endl;

		ofstream complete_log;
		complete_log.open("sqlsmith.complete.log");
		while (1) /* Loop to recover connection loss */
		{
			try {
				while (1) { /* Main loop */

					if (options.count("max-queries") && (++queries_generated > stol(options["max-queries"]))) {
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
					complete_log << s.str() << endl;
					complete_log.flush();

					// write the last-executed query to a separate log file
					ofstream out_file;
					out_file.open("sqlsmith.log");
					out_file << s.str() << endl;
					out_file.close();

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
							/* re-throw to outer loop to recover session. */
							throw;
						}
					}
				}
			} catch (const dut::broken &e) {
				/* Give server some time to recover. */
				this_thread::sleep_for(milliseconds(1000));
			}
		}
	} catch (const exception &e) {
		cerr << e.what() << endl;
		return 1;
	}
}
