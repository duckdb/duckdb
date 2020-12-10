#include "fts_indexing.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

#define SQL(...) #__VA_ARGS__

static string fts_schema_name(string schema, string table) {
	return "fts_" + schema + "_" + table;
}

string drop_fts_index_query(ClientContext &context, FunctionParameters parameters) {
    auto qname = QualifiedName::Parse(parameters.values[0].str_value);
	qname.schema = qname.schema == INVALID_SCHEMA ? DEFAULT_SCHEMA : qname.schema;
	string fts_schema = fts_schema_name(qname.schema, qname.name);

	if (!context.catalog.schemas->GetEntry(context, fts_schema)) {
		throw CatalogException("a FTS index does not exist on table '%s.%s'. Create one with 'PRAGMA create_fts_index()'.",
		                       qname.schema, qname.name);
	}

	return "DROP SCHEMA " + fts_schema + " CASCADE;";
}

static string indexing_script(string input_schema, string input_table, string input_id, vector<string> input_values,
                              string stemmer) {
	string fts_schema = fts_schema_name(input_schema, input_table);
	// weird way to have decently readable SQL code in here
    string result = SQL(
        DROP SCHEMA IF EXISTS %fts_schema% CASCADE;
        CREATE SCHEMA %fts_schema%;
	    CREATE TABLE %fts_schema%.stopwords (sw VARCHAR);
	    INSERT INTO %fts_schema%.stopwords VALUES ('a'), ('a''s'), ('able'), ('about'), ('above'), ('according'), ('accordingly'), ('across'), ('actually'), ('after'), ('afterwards'), ('again'), ('against'), ('ain''t'), ('all'), ('allow'), ('allows'), ('almost'), ('alone'), ('along'), ('already'), ('also'), ('although'), ('always'), ('am'), ('among'), ('amongst'), ('an'), ('and'), ('another'), ('any'), ('anybody'), ('anyhow'), ('anyone'), ('anything'), ('anyway'), ('anyways'), ('anywhere'), ('apart'), ('appear'), ('appreciate'), ('appropriate'), ('are'), ('aren''t'), ('around'), ('as'), ('aside'), ('ask'), ('asking'), ('associated'), ('at'), ('available'), ('away'), ('awfully'), ('b'), ('be'), ('became'), ('because'), ('become'), ('becomes'), ('becoming'), ('been'), ('before'), ('beforehand'), ('behind'), ('being'), ('believe'), ('below'), ('beside'), ('besides'), ('best'), ('better'), ('between'), ('beyond'), ('both'), ('brief'), ('but'), ('by'), ('c'), ('c''mon'), ('c''s'), ('came'), ('can'), ('can''t'), ('cannot'), ('cant'), ('cause'), ('causes'), ('certain'), ('certainly'), ('changes'), ('clearly'), ('co'), ('com'), ('come'), ('comes'), ('concerning'), ('consequently'), ('consider'), ('considering'), ('contain'), ('containing'), ('contains'), ('corresponding'), ('could'), ('couldn''t'), ('course'), ('currently'), ('d'), ('definitely'), ('described'), ('despite'), ('did'), ('didn''t'), ('different'), ('do'), ('does'), ('doesn''t'), ('doing'), ('don''t'), ('done'), ('down'), ('downwards'), ('during'), ('e'), ('each'), ('edu'), ('eg'), ('eight'), ('either'), ('else'), ('elsewhere'), ('enough'), ('entirely'), ('especially'), ('et'), ('etc'), ('even'), ('ever'), ('every'), ('everybody'), ('everyone'), ('everything'), ('everywhere'), ('ex'), ('exactly'), ('example'), ('except'), ('f'), ('far'), ('few'), ('fifth'), ('first'), ('five'), ('followed'), ('following'), ('follows'), ('for'), ('former'), ('formerly'), ('forth'), ('four'), ('from'), ('further'), ('furthermore'), ('g'), ('get'), ('gets'), ('getting'), ('given'), ('gives'), ('go'), ('goes'), ('going'), ('gone'), ('got'), ('gotten'), ('greetings'), ('h'), ('had'), ('hadn''t'), ('happens'), ('hardly'), ('has'), ('hasn''t'), ('have'), ('haven''t'), ('having'), ('he'), ('he''s'), ('hello'), ('help'), ('hence'), ('her'), ('here'), ('here''s'), ('hereafter'), ('hereby'), ('herein'), ('hereupon'), ('hers'), ('herself'), ('hi'), ('him'), ('himself'), ('his'), ('hither'), ('hopefully'), ('how'), ('howbeit'), ('however'), ('i'), ('i''d'), ('i''ll'), ('i''m'), ('i''ve'), ('ie'), ('if'), ('ignored'), ('immediate'), ('in'), ('inasmuch'), ('inc'), ('indeed'), ('indicate'), ('indicated'), ('indicates'), ('inner'), ('insofar'), ('instead'), ('into'), ('inward'), ('is'), ('isn''t'), ('it'), ('it''d'), ('it''ll'), ('it''s'), ('its'), ('itself'), ('j'), ('just'), ('k'), ('keep'), ('keeps'), ('kept'), ('know'), ('knows'), ('known'), ('l'), ('last'), ('lately'), ('later'), ('latter'), ('latterly'), ('least'), ('less'), ('lest'), ('let'), ('let''s'), ('like'), ('liked'), ('likely'), ('little'), ('look'), ('looking'), ('looks'), ('ltd'), ('m'), ('mainly'), ('many'), ('may'), ('maybe'), ('me'), ('mean'), ('meanwhile'), ('merely'), ('might'), ('more'), ('moreover'), ('most'), ('mostly'), ('much'), ('must'), ('my'), ('myself'), ('n'), ('name'), ('namely'), ('nd'), ('near'), ('nearly'), ('necessary'), ('need'), ('needs'), ('neither'), ('never'), ('nevertheless'), ('new'), ('next'), ('nine'), ('no'), ('nobody'), ('non'), ('none'), ('noone'), ('nor'), ('normally'), ('not'), ('nothing'), ('novel'), ('now'), ('nowhere'), ('o'), ('obviously'), ('of'), ('off'), ('often'), ('oh'), ('ok'), ('okay'), ('old'), ('on'), ('once'), ('one'), ('ones'), ('only'), ('onto'), ('or'), ('other'), ('others'), ('otherwise'), ('ought'), ('our'), ('ours'), ('ourselves'), ('out'), ('outside'), ('over'), ('overall'), ('own');
	    INSERT INTO %fts_schema%.stopwords VALUES ('p'), ('particular'), ('particularly'), ('per'), ('perhaps'), ('placed'), ('please'), ('plus'), ('possible'), ('presumably'), ('probably'), ('provides'), ('q'), ('que'), ('quite'), ('qv'), ('r'), ('rather'), ('rd'), ('re'), ('really'), ('reasonably'), ('regarding'), ('regardless'), ('regards'), ('relatively'), ('respectively'), ('right'), ('s'), ('said'), ('same'), ('saw'), ('say'), ('saying'), ('says'), ('second'), ('secondly'), ('see'), ('seeing'), ('seem'), ('seemed'), ('seeming'), ('seems'), ('seen'), ('self'), ('selves'), ('sensible'), ('sent'), ('serious'), ('seriously'), ('seven'), ('several'), ('shall'), ('she'), ('should'), ('shouldn''t'), ('since'), ('six'), ('so'), ('some'), ('somebody'), ('somehow'), ('someone'), ('something'), ('sometime'), ('sometimes'), ('somewhat'), ('somewhere'), ('soon'), ('sorry'), ('specified'), ('specify'), ('specifying'), ('still'), ('sub'), ('such'), ('sup'), ('sure'), ('t'), ('t''s'), ('take'), ('taken'), ('tell'), ('tends'), ('th'), ('than'), ('thank'), ('thanks'), ('thanx'), ('that'), ('that''s'), ('thats'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('thence'), ('there'), ('there''s'), ('thereafter'), ('thereby'), ('therefore'), ('therein'), ('theres'), ('thereupon'), ('these'), ('they'), ('they''d'), ('they''ll'), ('they''re'), ('they''ve'), ('think'), ('third'), ('this'), ('thorough'), ('thoroughly'), ('those'), ('though'), ('three'), ('through'), ('throughout'), ('thru'), ('thus'), ('to'), ('together'), ('too'), ('took'), ('toward'), ('towards'), ('tried'), ('tries'), ('truly'), ('try'), ('trying'), ('twice'), ('two'), ('u'), ('un'), ('under'), ('unfortunately'), ('unless'), ('unlikely'), ('until'), ('unto'), ('up'), ('upon'), ('us'), ('use'), ('used'), ('useful'), ('uses'), ('using'), ('usually'), ('uucp'), ('v'), ('value'), ('various'), ('very'), ('via'), ('viz'), ('vs'), ('w'), ('want'), ('wants'), ('was'), ('wasn''t'), ('way'), ('we'), ('we''d'), ('we''ll'), ('we''re'), ('we''ve'), ('welcome'), ('well'), ('went'), ('were'), ('weren''t'), ('what'), ('what''s'), ('whatever'), ('when'), ('whence'), ('whenever'), ('where'), ('where''s'), ('whereafter'), ('whereas'), ('whereby'), ('wherein'), ('whereupon'), ('wherever'), ('whether'), ('which'), ('while'), ('whither'), ('who'), ('who''s'), ('whoever'), ('whole'), ('whom'), ('whose'), ('why'), ('will'), ('willing'), ('wish'), ('with'), ('within'), ('without'), ('won''t'), ('wonder'), ('would'), ('would'), ('wouldn''t'), ('x'), ('y'), ('yes'), ('yet'), ('you'), ('you''d'), ('you''ll'), ('you''re'), ('you''ve'), ('your'), ('yours'), ('yourself'), ('yourselves'), ('z'), ('zero');
        CREATE MACRO %fts_schema%.tokenize(s) AS string_split_regex(regexp_replace(lower(strip_accents(s)), '(\\\\.|[^a-z])', ' ', 'g'), '\s+');

        CREATE TABLE %fts_schema%.docs AS (
            SELECT
                rowid + 1 AS docid,
                ii.%input_id% AS name
            FROM
                %input_schema%.%input_table% AS ii
        );

        CREATE TABLE %fts_schema%.terms AS (
            SELECT
                term,
                docid,
                row_number() OVER (PARTITION BY docid) AS pos
            FROM (
                WITH unstopped_tokens AS (
                    SELECT unnest(%fts_schema%.tokenize(%input_values%)) AS w, rank() OVER (ORDER BY ii.%input_id%) AS docid
                    FROM %input_schema%.%input_table% AS ii
                )
                SELECT
                    stem(ut.w, '%stemmer%') AS term,
                    ut.docid AS docid
                FROM unstopped_tokens AS ut
                WHERE w NOT IN (SELECT sw FROM %fts_schema%.stopwords)
            ) AS sq
            WHERE
                len(term) != 0 AND term NOT NULL
        );

        ALTER TABLE %fts_schema%.docs ADD len INT;
        UPDATE %fts_schema%.docs d
        SET len = (
            SELECT count(term)
            FROM %fts_schema%.terms t
            WHERE t.docid = d.docid
        );

        CREATE TABLE %fts_schema%.dict AS
        WITH distinct_terms AS (
            SELECT DISTINCT term
            FROM %fts_schema%.terms
        )
        SELECT
            row_number() OVER (PARTITION BY (SELECT NULL)) AS termid,
            dt.term
        FROM
            distinct_terms AS dt;

        ALTER TABLE %fts_schema%.terms ADD termid INT;
        UPDATE %fts_schema%.terms t
        SET termid = (
            SELECT termid
            FROM %fts_schema%.dict d
            WHERE t.term = d.term
        );
        ALTER TABLE %fts_schema%.terms DROP term;

        ALTER TABLE %fts_schema%.dict ADD df INT;
        UPDATE %fts_schema%.dict d
        SET df = (
            SELECT count(distinct docid)
            FROM %fts_schema%.terms t
            WHERE d.termid = t.termid
            GROUP BY termid
        );

        CREATE TABLE %fts_schema%.stats AS (
            SELECT COUNT(docs.docid) AS num_docs, SUM(docs.len) / COUNT(docs.len) AS avgdl
            FROM %fts_schema%.docs AS docs
        );

        CREATE MACRO %fts_schema%.match_bm25(docname, query_string, k=1.2, b=0.75, conjunctive=0) AS docname IN (
            WITH tokens AS
                (SELECT stem(unnest(%fts_schema%.tokenize(query_string)), '%stemmer%') AS t),
            qtermids AS
                (SELECT termid FROM %fts_schema%.dict AS dict, tokens WHERE dict.term = tokens.t),
            qterms AS
                (SELECT termid, docid FROM %fts_schema%.terms AS terms WHERE termid IN (SELECT qtermids.termid FROM qtermids)),
            subscores AS (
                SELECT
                    docs.docid, len, term_tf.termid, tf, df,
                    (log(((SELECT num_docs FROM %fts_schema%.stats) - df + 0.5) / (df + 0.5))* ((tf * (k + 1)/(tf + k * (1 - b + b * (len / (SELECT avgdl FROM %fts_schema%.stats))))))) AS subscore
                FROM
                    (SELECT termid, docid, COUNT(*) AS tf FROM qterms GROUP BY docid, termid) AS term_tf
                JOIN
                    (SELECT docid FROM qterms GROUP BY docid HAVING CASE WHEN conjunctive THEN COUNT(DISTINCT termid) = (SELECT COUNT(*) FROM tokens) ELSE 1 END) AS cdocs
                ON
                    term_tf.docid = cdocs.docid
                JOIN
                    %fts_schema%.docs AS docs
                ON
                    term_tf.docid = docs.docid
                JOIN
                    %fts_schema%.dict AS dict
                ON
                    term_tf.termid = dict.termid
            )
            SELECT name
            FROM (SELECT docid, sum(subscore) AS score FROM subscores GROUP BY docid) AS scores
            JOIN %fts_schema%.docs AS docs
            ON scores.docid = docs.docid ORDER BY score DESC LIMIT 1000
        );
    );

    // fill in variables (inefficiently, but keeps SQL script readable)
	result = StringUtil::Replace(result, "%fts_schema%", fts_schema);
	result = StringUtil::Replace(result, "%input_schema%", input_schema);
	result = StringUtil::Replace(result, "%input_table%", input_table);
	result = StringUtil::Replace(result, "%input_id%", input_id);
    result = StringUtil::Replace(result, "%stemmer%", stemmer);

	// input value columns are a bit different because they need to be concatenated before indexing
	// and the table alias must be given (in case a column is called 'table', etc.)
	for (idx_t i = 0; i < input_values.size(); i++) {
		input_values[i] = "ii." + input_values[i];
	}
	result = StringUtil::Replace(result, "%input_values%", StringUtil::Format("concat(%s)", StringUtil::Join(input_values, ", ' ', ")));


	return result;
}

string create_fts_index_query(ClientContext &context, FunctionParameters parameters) {
    auto qname = QualifiedName::Parse(parameters.values[0].str_value);
    qname.schema = qname.schema == INVALID_SCHEMA ? DEFAULT_SCHEMA : qname.schema;
    string fts_schema = fts_schema_name(qname.schema, qname.name);

    if (!context.catalog.schemas->GetEntry(context, qname.schema)) {
        throw CatalogException("No such schema: '%s'", qname.schema);
    }
    auto schema = (SchemaCatalogEntry *)context.catalog.schemas->GetEntry(context, qname.schema);
    if (!schema->tables.GetEntry(context, qname.name)) {
        throw CatalogException("No such table: '%s.%s'", qname.schema, qname.name);
    }

	// get named parameters
	string stemmer = "porter";
	if (parameters.named_parameters.find("stemmer") != parameters.named_parameters.end()) {
		stemmer = parameters.named_parameters["stemmer"].str_value;
	}
	bool overwrite = false;
	if (parameters.named_parameters.find("overwrite") != parameters.named_parameters.end()) {
		overwrite = parameters.named_parameters["overwrite"].value_.boolean;
	}

	// throw error if an index already exists on this table
	if (context.catalog.schemas->GetEntry(context, fts_schema) && !overwrite) {
		throw CatalogException("a FTS index already exists on table '%s.%s'. Supply 'overwite=1' to overwrite, or "
		                       "drop the existing index with 'PRAGMA drop_fts_index()' before creating a new one.",
		                       qname.schema, qname.name);
	}

	// positional parameters (vararg document text fields to be indexed)
	auto doc_id = parameters.values[1].str_value;
	vector<string> doc_values;
	for (idx_t i = 2; i < parameters.values.size(); i++) {
		doc_values.push_back(parameters.values[i].str_value);
	}
	if (doc_values.empty()) {
		throw Exception("at least one column must be supplied for indexing!");
	}

	return indexing_script(qname.schema, qname.name, doc_id, doc_values, stemmer);
}

} // namespace duckdb
