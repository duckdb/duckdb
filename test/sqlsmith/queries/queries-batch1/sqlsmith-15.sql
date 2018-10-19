UPDATE
    main.customer
SET
    c_phone = CAST(NULL AS VARCHAR)
RETURNING
    main.customer.c_address AS c0,
    main.customer.c_address AS c1,
    main.customer.c_comment AS c2
