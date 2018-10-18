update main.customer set c_phone = cast(null as VARCHAR) returning main.customer.c_address as c0, main.customer.c_address as c1, main.customer.c_comment as c2 
