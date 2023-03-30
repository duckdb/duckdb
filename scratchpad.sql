SELECT grade from Students, (SELECT * from B.courseID = Students.courseID) WHERE Students.name = 'ABC';

SELECT *
  FROM customer
 WHERE (SELECT SUM(order_amount)
          FROM orders
         WHERE orders.customer_id = customer.customer_id
           ()) > 1000;


SELECT * 
    FROM (SELECT 42) t(i), 
         (SELECT * 
            FROM (SELECT 142 k) t3(k), 
                 (SELECT k) t4(l)) t2(j);

Bind SELECT_STMT;
Bind FROM_CLAUSE;
Bind JoinRef
Bind Left Side of JoinRef
Bind TableRef

LateralBinder :: ExpressionBinder

Bind Right Side of JoinRef


SELECT * FROM (SELECT 42) t(i)
WHERE i IN (SELECT * FROM (SELECT 42 k) t3(k)
            WHERE k IN (SELECT * FROM (SELECT 42 l) t4(l) WHERE i-k = 0));

SELECT *
    FROM (SELECT 42) t(i),           
         (SELECT *              
            FROM (SELECT 142 k) t3(k),           
                 (SELECT 1 WHERE i+k=0) t4(l));