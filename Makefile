EXTENSION = hypocost
MODULE_big = hypocost
DATA = hypocost--0.0.1.sql
OBJS = hypocost.o hypocost_explain.o hypocost_plan.o hypocost_func.o
# If PG_CONFIG is not set, try the default build folder.
PG_CONFIG ?= ../../build/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
