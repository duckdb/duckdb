CREATE TABLE actor (
    actor_id numeric NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    last_update TIMESTAMP NOT NULL --,
    --PRIMARY KEY (actor_id)

);

CREATE TABLE country (
    country_id SMALLINT NOT NULL,
    country VARCHAR(50) NOT NULL,
    last_update TIMESTAMP,
    PRIMARY KEY (country_id)
);

CREATE TABLE city (
    city_id int NOT NULL,
    city VARCHAR(50) NOT NULL,
    country_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (city_id) --,
    --CONSTRAINT fk_city_country FOREIGN KEY (country_id) REFERENCES country (country_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE address (
    address_id int NOT NULL,
    address VARCHAR(50) NOT NULL,
    address2 VARCHAR(50) DEFAULT NULL,
    district VARCHAR(20) NOT NULL,
    city_id INT NOT NULL,
    postal_code VARCHAR(10) DEFAULT NULL,
    phone VARCHAR(20) NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (address_id) --,
    --  CONSTRAINT fk_address_city FOREIGN KEY (city_id) REFERENCES city (city_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE
LANGUAGE (
    language_id SMALLINT NOT NULL,
    name CHAR(20) NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (language_id)
);

CREATE TABLE category (
    category_id SMALLINT NOT NULL,
    name VARCHAR(25) NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (category_id)
);

CREATE TABLE customer (
    customer_id INT NOT NULL,
    store_id INT NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    email VARCHAR(50) DEFAULT NULL,
    address_id INT NOT NULL,
    active CHAR(1) DEFAULT 'Y' NOT NULL,
    create_date TIMESTAMP NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (customer_id) --,
    -- CONSTRAINT fk_customer_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE NO ACTION ON UPDATE CASCADE,
    --CONSTRAINT fk_customer_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE film (
    film_id int NOT NULL,
    title VARCHAR(255) NOT NULL,
    description string DEFAULT NULL,
    release_year VARCHAR(4) DEFAULT NULL,
    language_id SMALLINT NOT NULL,
    original_language_id SMALLINT DEFAULT NULL,
    rental_duration SMALLINT DEFAULT 3 NOT NULL,
    rental_rate DECIMAL(4, 2) DEFAULT 4.99 NOT NULL,
    length SMALLINT DEFAULT NULL,
    replacement_cost DECIMAL(5, 2) DEFAULT 19.99 NOT NULL,
    rating VARCHAR(10) DEFAULT 'G',
    special_features VARCHAR(100) DEFAULT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (film_id) --,
    -- CONSTRAINT CHECK_special_features CHECK(special_features is null or
    --                                                          special_features like '%Trailers%' or
    --                                                          special_features like '%Commentaries%' or
    --                                                          special_features like '%Deleted Scenes%' or
    --                                                          special_features like '%Behind the Scenes%'),
    -- CONSTRAINT CHECK_special_rating CHECK(rating in ('G','PG','PG-13','R','NC-17')),
    -- CONSTRAINT fk_film_language FOREIGN KEY (language_id) REFERENCES language (language_id) ,
    -- CONSTRAINT fk_film_language_original FOREIGN KEY (original_language_id) REFERENCES language (language_id)

);

CREATE TABLE film_actor (
    actor_id INT NOT NULL,
    film_id INT NOT NULL,
    last_update TIMESTAMP NOT NULL --,
    -- PRIMARY KEY (actor_id, film_id) --,
    -- CONSTRAINT fk_film_actor_actor FOREIGN KEY (actor_id) REFERENCES actor (actor_id) ON DELETE NO ACTION ON UPDATE CASCADE,
    -- CONSTRAINT fk_film_actor_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE film_category (
    film_id INT NOT NULL,
    category_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL --,
    --PRIMARY KEY (film_id, category_id) --,
    -- CONSTRAINT fk_film_category_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE NO ACTION ON UPDATE CASCADE,
    -- CONSTRAINT fk_film_category_category FOREIGN KEY (category_id) REFERENCES category (category_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE film_text (
    film_id SMALLINT NOT NULL,
    title VARCHAR(255) NOT NULL,
    description string,
    PRIMARY KEY (film_id)
);

CREATE TABLE inventory (
    inventory_id INT NOT NULL,
    film_id INT NOT NULL,
    store_id INT NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (inventory_id) --,
    -- CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE NO ACTION ON UPDATE CASCADE,
    -- CONSTRAINT fk_inventory_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE staff (
    staff_id SMALLINT NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    address_id INT NOT NULL,
    picture string DEFAULT NULL,
    email VARCHAR(50) DEFAULT NULL,
    store_id INT NOT NULL,
    active SMALLINT DEFAULT 1 NOT NULL,
    username VARCHAR(16) NOT NULL,
    PASSWORD VARCHAR(40) DEFAULT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (staff_id) --,
    -- CONSTRAINT fk_staff_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE NO ACTION ON UPDATE CASCADE,
    -- CONSTRAINT fk_staff_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE NO ACTION ON UPDATE CASCADE

);

CREATE TABLE store (
    store_id INT NOT NULL,
    manager_staff_id SMALLINT NOT NULL,
    address_id INT NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (store_id) --,
    -- CONSTRAINT fk_store_staff FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) ,
    -- CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES address (address_id)

);

CREATE TABLE payment (
    payment_id int NOT NULL,
    customer_id INT NOT NULL,
    staff_id SMALLINT NOT NULL,
    rental_id INT DEFAULT NULL,
    amount DECIMAL(5, 2) NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (payment_id) --,
    -- CONSTRAINT fk_payment_rental FOREIGN KEY (rental_id) REFERENCES rental (rental_id) ON DELETE SET NULL ON UPDATE CASCADE,
    -- CONSTRAINT fk_payment_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ,
    -- CONSTRAINT fk_payment_staff FOREIGN KEY (staff_id) REFERENCES staff (staff_id)

);

CREATE TABLE rental (
    rental_id INT NOT NULL,
    rental_date TIMESTAMP NOT NULL,
    inventory_id INT NOT NULL,
    customer_id INT NOT NULL,
    return_date TIMESTAMP DEFAULT NULL,
    staff_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL,
    PRIMARY KEY (rental_id) --,
    -- CONSTRAINT fk_rental_staff FOREIGN KEY (staff_id) REFERENCES staff (staff_id) ,
    -- CONSTRAINT fk_rental_inventory FOREIGN KEY (inventory_id) REFERENCES inventory (inventory_id) ,
    -- CONSTRAINT fk_rental_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)

);

CREATE VIEW customer_list AS
SELECT
    cu.customer_id AS ID,
    cu.first_name || ' ' || cu.last_name AS name,
    a.address AS address,
    a.postal_code AS zip_code,
    a.phone AS phone,
    city.city AS city,
    country.country AS country,
    CASE WHEN cu.active = 1 THEN
        'active'
    ELSE
        ''
    END AS notes,
    cu.store_id AS SID
FROM
    customer AS cu
    JOIN address AS a ON cu.address_id = a.address_id
    JOIN city ON a.city_id = city.city_id
    JOIN country ON city.country_id = country.country_id;

CREATE VIEW film_list AS
SELECT
    film.film_id AS FID,
    film.title AS title,
    film.description AS description,
    category.name AS category,
    film.rental_rate AS price,
    film.length AS length,
    film.rating AS rating,
    actor.first_name || ' ' || actor.last_name AS actors
FROM
    category
    LEFT JOIN film_category ON category.category_id = film_category.category_id
    LEFT JOIN film ON film_category.film_id = film.film_id
    JOIN film_actor ON film.film_id = film_actor.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id;

CREATE VIEW staff_list AS
SELECT
    s.staff_id AS ID,
    s.first_name || ' ' || s.last_name AS name,
    a.address AS address,
    a.postal_code AS zip_code,
    a.phone AS phone,
    city.city AS city,
    country.country AS country,
    s.store_id AS SID
FROM
    staff AS s
    JOIN address AS a ON s.address_id = a.address_id
    JOIN city ON a.city_id = city.city_id
    JOIN country ON city.country_id = country.country_id;

CREATE VIEW sales_by_store AS
SELECT
    s.store_id,
    c.city || ',' || cy.country AS store,
    m.first_name || ' ' || m.last_name AS manager,
    SUM(p.amount) AS total_sales
FROM
    payment AS p
    INNER JOIN rental AS r ON p.rental_id = r.rental_id
    INNER JOIN inventory AS i ON r.inventory_id = i.inventory_id
    INNER JOIN store AS s ON i.store_id = s.store_id
    INNER JOIN address AS a ON s.address_id = a.address_id
    INNER JOIN city AS c ON a.city_id = c.city_id
    INNER JOIN country AS cy ON c.country_id = cy.country_id
    INNER JOIN staff AS m ON s.manager_staff_id = m.staff_id
GROUP BY
    s.store_id,
    c.city || ',' || cy.country,
    m.first_name || ' ' || m.last_name;

CREATE VIEW sales_by_film_category AS
SELECT
    c.name AS category,
    SUM(p.amount) AS total_sales
FROM
    payment AS p
    INNER JOIN rental AS r ON p.rental_id = r.rental_id
    INNER JOIN inventory AS i ON r.inventory_id = i.inventory_id
    INNER JOIN film AS f ON i.film_id = f.film_id
    INNER JOIN film_category AS fc ON f.film_id = fc.film_id
    INNER JOIN category AS c ON fc.category_id = c.category_id
GROUP BY
    c.name;
