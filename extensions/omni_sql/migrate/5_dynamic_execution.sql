create function execq(text, integer) returns int8
as
'MODULE_PATHNAME', 'execq' language c strict;
