CREATE OR REPLACE FUNCTION hypocost_substitute_index(
	search TEXT,
	replace_index REGCLASS
) RETURNS bool
LANGUAGE C STRICT
AS '$libdir/hypocost', 'hypocost_substitute_index';

CREATE OR REPLACE FUNCTION hypocost_substitute_reset() RETURNS bool
LANGUAGE C STRICT
AS '$libdir/hypocost', 'hypocost_substitute_reset';
