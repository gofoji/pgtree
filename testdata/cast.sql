CREATE CAST (bigint AS int4)
    WITH FUNCTION int4(bigint) AS ASSIGNMENT;
CREATE CAST (bigint AS int4)
    WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (bigint AS int4)
    WITH INOUT;
drop CAST (bigint AS int4);
