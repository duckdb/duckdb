#include "catch.hpp"
#include "duckdb/common/fstream_util.hpp"
#include "duckdb/common/gzip_stream.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <sstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Run Sakila test queries", "[sakila][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	string token;

	// read schema
	fstream schema_file;
	FstreamUtil::OpenFile("test/sakila/duckdb-sakila-schema.sql", schema_file);

	// woo hacks, use getline to separate queries
	while (getline(schema_file, token, ';')) {
		REQUIRE_NO_FAIL(con.Query(token));
	}

	// read data
	GzipStream data_file("test/sakila/duckdb-sakila-insert-data.sql.gz");
	while (getline(data_file, token, ';')) {
		REQUIRE_NO_FAIL(con.Query(token));
	}
	// only enable verification here because of 40k or so inserts above
	con.EnableQueryVerification();

	result = con.Query("SELECT first_name,last_name FROM actor ORDER BY actor_id LIMIT 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(
	    result, 0, {"PENELOPE", "NICK", "ED", "JENNIFER", "JOHNNY", "BETTE", "GRACE", "MATTHEW", "JOE", "CHRISTIAN"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"GUINESS", "WAHLBERG", "CHASE", "DAVIS", "LOLLOBRIGIDA", "NICHOLSON", "MOSTEL", "JOHANSSON",
	                      "SWANK", "GABLE"}));

	result = con.Query("SELECT actor_id, first_name, last_name FROM actor WHERE first_name='JOE' ORDER BY actor_id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {9}));
	REQUIRE(CHECK_COLUMN(result, 1, {"JOE"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"SWANK"}));

	result =
	    con.Query("SELECT actor_id, first_name, last_name FROM actor WHERE last_name LIKE '%GEN%' ORDER BY actor_id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {14, 41, 107, 166}));
	REQUIRE(CHECK_COLUMN(result, 1, {"VIVIEN", "JODIE", "GINA", "NICK"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"BERGEN", "DEGENERES", "DEGENERES", "DEGENERES"}));

	result = con.Query("SELECT country_id, country FROM country WHERE country IN('Afghanistan', 'Bangladesh', 'China') "
	                   "ORDER BY country_id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 12, 23}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Afghanistan", "Bangladesh", "China"}));

	result = con.Query("SELECT last_name, COUNT(*) as lnc FROM actor GROUP BY last_name HAVING COUNT(*) >=4 ORDER BY "
	                   "lnc desc, last_name");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"KILMER", "NOLTE", "TEMPLE"}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 4, 4}));

	result = con.Query("SELECT first_name, last_name, address FROM staff INNER JOIN address ON staff.address_id = "
	                   "address.address_id order by first_name, last_name");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"Jon", "Mike"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Stephens", "Hillyer"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"1411 Lillydale Drive", "23 Workhaven Lane"}));

	result =
	    con.Query("SELECT first_name, last_name, SUM(amount) as total FROM staff INNER JOIN payment ON staff.staff_id "
	              "= payment.staff_id AND payment_date LIKE '2005-08%' GROUP BY first_name, last_name ORDER BY total");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"Mike", "Jon"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Hillyer", "Stephens"}));
	REQUIRE(CHECK_COLUMN(result, 2, {11853.65, 12218.48}));

	result = con.Query("SELECT title, COUNT(actor_id) as actor_count FROM film_actor INNER JOIN film ON "
	                   "film_actor.film_id = film.film_id GROUP BY title order by actor_count desc, title limit 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"LAMBS CINCINATTI", "BOONDOCK BALLROOM", "CHITTY LOCK", "CRAZY HOME", "DRACULA CRYSTAL",
	                      "MUMMY CREATURES", "RANDOM GO", "ARABIA DOGMA", "HELLFIGHTERS SIERRA", "LESSON CLEOPATRA"}));
	REQUIRE(CHECK_COLUMN(result, 1, {15, 13, 13, 13, 13, 13, 13, 12, 12, 12}));

	result = con.Query("SELECT title, COUNT(title) as copies_available FROM film INNER JOIN inventory ON film.film_id "
	                   "= inventory.film_id WHERE title = 'HUNCHBACK IMPOSSIBLE' GROUP BY title");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"HUNCHBACK IMPOSSIBLE"}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	result = con.Query("SELECT first_name, last_name, SUM(amount) as total_paid FROM payment INNER JOIN customer ON "
	                   "payment.customer_id = customer.customer_id GROUP BY first_name, last_name ORDER BY total_paid "
	                   "desc, first_name limit 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"KARL", "ELEANOR", "CLARA", "MARION", "RHONDA", "TOMMY", "WESLEY", "TIM", "MARCIA", "ANA"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"SEAL", "HUNT", "SHAW", "SNYDER", "KENNEDY", "COLLAZO", "BULL", "CARY", "DEAN", "BRADLEY"}));
	REQUIRE(CHECK_COLUMN(result, 2, {221.55, 216.54, 195.58, 194.61, 194.61, 186.62, 177.6, 175.61, 175.58, 174.66}));

	result = con.Query(
	    "SELECT title FROM film WHERE title LIKE 'K%' OR title LIKE 'Q%' AND title IN ( SELECT title FROM film WHERE "
	    "language_id IN ( SELECT language_id FROM language WHERE name ='English' ) ) ORDER BY title");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"KANE EXORCIST", "KARATE MOON", "KENTUCKIAN GIANT", "KICK SAVANNAH", "KILL BROTHERHOOD",
	                      "KILLER INNOCENT", "KING EVOLUTION", "KISS GLORY", "KISSING DOLLS", "KNOCK WARLOCK",
	                      "KRAMER CHOCOLATE", "KWAI HOMEWARD", "QUEEN LUKE", "QUEST MUSSOLINI", "QUILLS BULL"}));

	result = con.Query("SELECT first_name, last_name FROM actor WHERE actor_id IN ( SELECT actor_id FROM film_actor "
	                   "WHERE film_id IN ( SELECT film_id FROM film WHERE title = 'ALONE TRIP' ) ) ORDER BY actor_id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"ED", "KARL", "UMA", "WOODY", "SPENCER", "CHRIS", "LAURENCE", "RENEE"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"CHASE", "BERRY", "WOOD", "JOLIE", "DEPP", "DEPP", "BULLOCK", "BALL"}));

	result = con.Query(
	    "SELECT first_name, last_name, email FROM customer JOIN address ON (customer.address_id = address.address_id) "
	    "JOIN city ON (city.city_id = address.city_id) JOIN country ON (country.country_id = city.country_id) WHERE "
	    "country.country= 'Canada' ORDER BY first_name, last_name");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"CURTIS", "DARRELL", "DERRICK", "LORETTA", "TROY"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"IRBY", "POWER", "BOURQUE", "CARPENTER", "QUIGLEY"}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {"CURTIS.IRBY@sakilacustomer.org", "DARRELL.POWER@sakilacustomer.org",
	                      "DERRICK.BOURQUE@sakilacustomer.org", "LORETTA.CARPENTER@sakilacustomer.org",
	                      "TROY.QUIGLEY@sakilacustomer.org"}));

	result = con.Query("SELECT title FROM film WHERE film_id IN ( SELECT film_id FROM film_category WHERE category_id "
	                   "IN ( SELECT category_id FROM category WHERE name='Family' ) ) ORDER BY film_id LIMIT 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(
	    CHECK_COLUMN(result, 0,
	                 {"AFRICAN EGG", "APACHE DIVINE", "ATLANTIS CAUSE", "BAKED CLEOPATRA", "BANG KWAI",
	                  "BEDAZZLED MARRIED", "BILKO ANONYMOUS", "BLANKET BEVERLY", "BLOOD ARGONAUTS", "BLUES INSTINCT"}));

	result = con.Query("SELECT title, COUNT(rental_id) as rental_count FROM rental JOIN inventory ON "
	                   "(rental.inventory_id = inventory.inventory_id) JOIN film ON (inventory.film_id = film.film_id) "
	                   "GROUP BY film.title ORDER BY COUNT(rental_id) DESC, title limit 10");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(
	    CHECK_COLUMN(result, 0,
	                 {"BUCKET BROTHERHOOD", "ROCKETEER MOTHER", "FORWARD TEMPLE", "GRIT CLOCKWORK", "JUGGLER HARDLY",
	                  "RIDGEMONT SUBMARINE", "SCALAWAG DUCK", "APACHE DIVINE", "GOODFELLAS SALUTE", "HOBBIT ALIEN"}));
	REQUIRE(CHECK_COLUMN(result, 1, {34, 33, 32, 32, 32, 32, 32, 31, 31, 31}));

	result = con.Query("SELECT store.store_id, SUM(amount) as sum_amount FROM store INNER JOIN staff ON store.store_id "
	                   "= staff.store_id INNER JOIN payment ON payment.staff_id = staff.staff_id GROUP BY "
	                   "store.store_id order by sum_amount");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {33489.47, 33927.04}));

	result = con.Query("SELECT store_id, city, country FROM store INNER JOIN address ON store.address_id = "
	                   "address.address_id INNER JOIN city ON city.city_id = address.city_id INNER JOIN country ON "
	                   "country.country_id = city.country_id order by store_id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Lethbridge", "Woodridge"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Canada", "Australia"}));

	result = con.Query("SELECT name, SUM(amount) FROM category INNER JOIN film_category ON category.category_id = "
	                   "film_category.category_id INNER JOIN inventory ON film_category.film_id = inventory.film_id "
	                   "INNER JOIN rental ON inventory.inventory_id = rental.inventory_id INNER JOIN payment ON "
	                   "rental.rental_id = payment.rental_id GROUP BY name ORDER BY SUM(amount) DESC LIMIT 5");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"Sports", "Sci-Fi", "Animation", "Drama", "Comedy"}));
	REQUIRE(CHECK_COLUMN(result, 1, {5314.21, 4756.98, 4656.3, 4587.39, 4383.58}));

	// these four queries are equivalent, see https://www.jooq.org/benchmark

	result = con.Query("SELECT first_name, last_name, count(*) FROM actor a JOIN film_actor fa USING (actor_id) WHERE "
	                   "last_name LIKE 'A%' GROUP BY fa.actor_id, a.first_name, a.last_name ORDER BY count(*) DESC");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"KIRSTEN", "CHRISTIAN", "ANGELINA", "KIM", "CUBA", "DEBBIE", "MERYL"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AKROYD", "AKROYD", "ASTAIRE", "ALLEN", "ALLEN", "AKROYD", "ALLEN"}));
	REQUIRE(CHECK_COLUMN(result, 2, {34, 32, 31, 28, 25, 24, 22}));

	result = con.Query(
	    "SELECT first_name, last_name, count(*) FROM ( SELECT * FROM actor WHERE last_name LIKE 'A%' ) a JOIN "
	    "film_actor USING (actor_id) GROUP BY a.actor_id, a.first_name, a.last_name ORDER BY count(*) DESC");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"KIRSTEN", "CHRISTIAN", "ANGELINA", "KIM", "CUBA", "DEBBIE", "MERYL"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AKROYD", "AKROYD", "ASTAIRE", "ALLEN", "ALLEN", "AKROYD", "ALLEN"}));
	REQUIRE(CHECK_COLUMN(result, 2, {34, 32, 31, 28, 25, 24, 22}));

	result =
	    con.Query("SELECT * FROM ( SELECT first_name, last_name, ( SELECT count(*) FROM film_actor fa WHERE a.actor_id "
	              "= fa.actor_id ) AS c FROM actor a WHERE last_name LIKE 'A%' ) a WHERE c > 0 ORDER BY c DESC");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"KIRSTEN", "CHRISTIAN", "ANGELINA", "KIM", "CUBA", "DEBBIE", "MERYL"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AKROYD", "AKROYD", "ASTAIRE", "ALLEN", "ALLEN", "AKROYD", "ALLEN"}));
	REQUIRE(CHECK_COLUMN(result, 2, {34, 32, 31, 28, 25, 24, 22}));

	result = con.Query("SELECT first_name, last_name, c FROM actor JOIN ( SELECT actor_id, count(*) c FROM film_actor "
	                   "GROUP BY actor_id ) fa USING (actor_id) WHERE last_name LIKE 'A%' ORDER BY c DESC");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"KIRSTEN", "CHRISTIAN", "ANGELINA", "KIM", "CUBA", "DEBBIE", "MERYL"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AKROYD", "AKROYD", "ASTAIRE", "ALLEN", "ALLEN", "AKROYD", "ALLEN"}));
	REQUIRE(CHECK_COLUMN(result, 2, {34, 32, 31, 28, 25, 24, 22}));
}
