CREATE OPERATOR CLASS gist__int_ops DEFAULT FOR TYPE _int4 USING gist AS
    OPERATOR 3 &&,
    OPERATOR 6 =(anyarray, anyarray),
    OPERATOR 7 @>,
    OPERATOR 8 <@,
    OPERATOR 20 @@(_int4, query_int),
    FUNCTION 1 g_int_consistent(internal, _int4, int, oid, internal),
    FUNCTION 2 g_int_union(internal, internal),
    FUNCTION 3 g_int_compress(internal),
    FUNCTION 4 g_int_decompress(internal),
    FUNCTION 5 g_int_penalty(internal, internal, internal),
    FUNCTION 6 g_int_picksplit(internal, internal),
    FUNCTION 7 g_int_same(_int4, _int4, internal);
DROP OPERATOR CLASS widget_ops USING btree;
