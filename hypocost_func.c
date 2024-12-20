#include "postgres.h"
#include "fmgr.h"
#include "catalog/namespace.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "optimizer/plancat.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/cost.h"
#include "nodes/bitmapset.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"

#include "hypocost.h"
#include "nodes/print.h"


/* Define a struct for the entries */
typedef struct SubEntry
{
    char *search;
    Oid index_oid;
} SubEntry;

/* Global list to store entries */
static List *sublist = NIL;

PG_FUNCTION_INFO_V1(hypocost_substitute_index);
PG_FUNCTION_INFO_V1(hypocost_substitute_reset);

Datum hypocost_substitute_index(PG_FUNCTION_ARGS)
{
    MemoryContext oldcontext;
    char* search = NULL;
    Oid index_oid;
    SubEntry *new_entry;

    /* Check and retrieve input arguments */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errmsg("Arguments cannot be NULL")));
    /* Switch to TopMemoryContext first */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    search = TextDatumGetCString(PG_GETARG_DATUM(0));
    index_oid = PG_GETARG_OID(1);

    new_entry = (SubEntry *) palloc(sizeof(SubEntry));
    new_entry->search = search;
    new_entry->index_oid = index_oid;

    /* Add the new entry to the global list */
    sublist = lappend(sublist, new_entry);
    MemoryContextSwitchTo(oldcontext);
    PG_RETURN_BOOL(true);
}

Datum hypocost_substitute_reset(PG_FUNCTION_ARGS)
{
    ListCell *cell;
    MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    foreach(cell, sublist)
    {
        SubEntry *entry = (SubEntry *) lfirst(cell);
        pfree(entry->search);
        pfree(entry);
    }
    list_free(sublist);
    sublist = NIL;
    MemoryContextSwitchTo(oldcontext);
    PG_RETURN_BOOL(true);
}


static RelOptInfo*
hypocost_fake_opt(PlannerInfo* root, Path *path, Oid filter_oid, List* filter_oids, bool build_indexes, bool allowbitmap)
{
	RelOptInfo* rel = NULL;
	bool old_enable_bitmapscan = enable_bitmapscan;
	bool old_enable_indexscan = enable_indexscan;
	bool old_enable_seqscan = enable_seqscan;
	RelOptInfo* roi = root->simple_rel_array[path->parent->relid];
	get_relation_info_hook_type prev_get_relation_info_hook = NULL;

	PG_TRY();
	{
		int i = -1;
		prev_get_relation_info_hook = get_relation_info_hook;
		get_relation_info_hook = NULL;
		root->simple_rel_array[path->parent->relid] = NULL;

		// Imitate and fake generate a relation to get the index list.
		// Somehow, if we use the current [roi], it gets borked!
		rel = build_simple_rel(root, path->parent->relid, NULL);
		rel->baserestrictinfo = roi->baserestrictinfo;
		rel->has_eclass_joins = roi->has_eclass_joins;
		rel->eclass_indexes = roi->eclass_indexes;
		rel->relids = roi->relids;

		// Add ...
		// Think this happens when a Parallel Append plan is involved...unfortunate...
		while ((i = bms_next_member(rel->eclass_indexes, i)) >= 0)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) list_nth(root->eq_classes, i);
			int j = -1;
			while ((j = bms_next_member(rel->relids, j)) >= 0)
			{
				cur_ec->ec_relids = bms_add_member(cur_ec->ec_relids, j);
			}
		}

		rel->joininfo = roi->joininfo;
		rel->reltarget = roi->reltarget;
		rel->top_parent_relids = roi->top_parent_relids;

		{
			ListCell* l;
			foreach(l, rel->indexlist)
			{
				IndexOptInfo* iinfo = (IndexOptInfo*)lfirst(l);

				if (filter_oid != 0)
				{
					if (iinfo->indexoid != filter_oid)
					{
						rel->indexlist = foreach_delete_current(rel->indexlist, l);
					}
				}

				if (filter_oids != NIL)
				{
					ListCell* foid;
					bool match = false;
					foreach (foid, filter_oids)
					{
						match = match || (iinfo->indexoid == lfirst_oid(foid));
					}

					if (!match)
					{
						rel->indexlist = foreach_delete_current(rel->indexlist, l);
					}
				}
			}
		}

		if (build_indexes)
		{
			check_index_predicates(root, rel);
			// Assume we have some [sane] cost...
			enable_bitmapscan = allowbitmap;
			enable_indexscan = !allowbitmap;
			enable_seqscan = false;
			create_index_paths(root, rel);
			enable_bitmapscan = old_enable_bitmapscan;
			enable_indexscan = old_enable_indexscan;
			enable_seqscan = old_enable_seqscan;
		}
		root->simple_rel_array[path->parent->relid] = roi;
	}
	PG_FINALLY();
	{
		get_relation_info_hook = prev_get_relation_info_hook;
		root->simple_rel_array[path->parent->relid] = roi;
		enable_bitmapscan = old_enable_bitmapscan;
	}
	PG_END_TRY();

	return rel;
}

List*
hypocost_check_replace(PlannerInfo* root, Path* path, bool inc_pk)
{
	if (IsA(path, BitmapOrPath))
	{
		List* rets = NIL;
		ListCell   *lc;
		BitmapOrPath* bpath = (BitmapOrPath*)path;
		foreach(lc, bpath->bitmapquals)
		{
			Path *bitmapqual = (Path *) lfirst(lc);
			List* oids = hypocost_check_replace(root, bitmapqual, inc_pk);
			if (oids != NIL)
				rets = list_concat(rets, oids);
		}
		return rets;
	}
	else if (IsA(path, BitmapAndPath))
	{
		List* rets = NIL;
		ListCell   *lc;
		BitmapAndPath* bpath = (BitmapAndPath*)path;
		foreach(lc, bpath->bitmapquals)
		{
			Path *bitmapqual = (Path *) lfirst(lc);
			List* oids = hypocost_check_replace(root, bitmapqual, inc_pk);
			if (oids != NIL)
				rets = list_concat(rets, oids);
		}
		return rets;
	}
	else if (IsA(path, BitmapHeapPath))
	{
		return hypocost_check_replace(root, ((BitmapHeapPath*)path)->bitmapqual, inc_pk);
	}
	else if (IsA(path, IndexPath))
	{
		ListCell *cell;
		IndexPath* ipath = (IndexPath*)path;
		const char *indexname = explain_get_index_name_hook(ipath->indexinfo->indexoid);
		if (indexname == NULL) {
			indexname = get_rel_name(ipath->indexinfo->indexoid);
		} else {
			indexname = pstrdup(indexname);
		}

		foreach(cell, sublist)
		{
			SubEntry *entry = (SubEntry *) lfirst(cell);
			if (strstr(indexname, entry->search) != NULL)
			{
				RelOptInfo* rel = hypocost_fake_opt(root, path, entry->index_oid, NIL, false, true);
				if (rel && list_length(rel->indexlist) > 0)
				{
					return list_make1_oid(entry->index_oid);
				}
			}
		}

		if (inc_pk)
		{
			return list_make1_oid(ipath->indexinfo->indexoid);
		}
	}

	return NIL;
}

void hypocost_substitute_bpath(PlannerInfo* root, Path* path, List* oids)
{
	BitmapHeapPath* bpath;
	ParamPathInfo* pinfo = path->param_info;
	Assert(IsA(path, BitmapHeapPath));
	bpath = (BitmapHeapPath*)path;

	if (IsA(bpath->bitmapqual, BitmapOrPath) || IsA(bpath->bitmapqual, BitmapAndPath) || IsA(bpath->bitmapqual, IndexPath))
	{
		ListCell* l;
		RelOptInfo* rel = hypocost_fake_opt(root, (Path*)bpath, 0, oids, true, true);
		foreach(l, rel->pathlist)
		{
			struct Path* nipath = lfirst(l);
			ParamPathInfo* nppath = nipath->param_info;
			if (nipath != NULL && IsA(nipath, BitmapHeapPath) &&
			    (nppath && pinfo && bms_compare(nppath->ppi_req_outer, pinfo->ppi_req_outer) == 0))
			{
				// First, prioritize compare equal.
				BitmapHeapPath* bnipath = (BitmapHeapPath*)nipath;
				bpath->bitmapqual = bnipath->bitmapqual;
				return;
			}
		}

		foreach(l, rel->pathlist)
		{
			struct Path* nipath = lfirst(l);
			ParamPathInfo* nppath = nipath->param_info;
			if (nipath != NULL && IsA(nipath, BitmapHeapPath) && nppath && pinfo && bms_is_subset(nppath->ppi_req_outer, pinfo->ppi_req_outer))
			{
				// Then, prioritize subset.
				BitmapHeapPath* bnipath = (BitmapHeapPath*)nipath;
				bpath->bitmapqual = bnipath->bitmapqual;
				return;
			}
		}

		foreach(l, rel->pathlist)
		{
			struct Path* nipath = lfirst(l);
			ParamPathInfo* nppath = nipath->param_info;
			if (nipath != NULL && IsA(nipath, BitmapHeapPath) && (nppath == NULL))
			{
				BitmapHeapPath* bnipath = (BitmapHeapPath*)nipath;
				bpath->bitmapqual = bnipath->bitmapqual;
				return;
			}
		}
	}
}

void hypocost_check_substitute(PlannerInfo* root, IndexPath* ipath, Path* outer)
{
	if (list_length(sublist) == 0)
		return;

	if (outer != NULL)
	{
		if (IsA(outer, BitmapOrPath) || IsA(outer, BitmapAndPath) || IsA(outer, BitmapHeapPath))
		{
			return;
		}
	}

	if (ipath->path.parent)
	{
		ListCell *cell;
		bool overwritten = false;
		RelOptInfo* ipparent = ipath->path.parent;
		ParamPathInfo* pinfo = ipath->path.param_info;
		const char *indexname = explain_get_index_name_hook(ipath->indexinfo->indexoid);
		if (indexname == NULL) {
			indexname = get_rel_name(ipath->indexinfo->indexoid);
		} else {
			indexname = pstrdup(indexname);
		}

		foreach(cell, sublist)
		{
			SubEntry *entry = (SubEntry *) lfirst(cell);
			if (strstr(indexname, entry->search) != NULL)
			{
				RelOptInfo* rel = hypocost_fake_opt(root, (Path*)ipath, entry->index_oid, NIL, true, false);
				if (rel)
				{
					ListCell* l;
					IndexPath* tpath = NULL;
					foreach(l, rel->pathlist)
					{
						struct Path* nipath = lfirst(l);
						if (nipath != NULL && IsA(nipath, IndexPath))
						{
							IndexPath* inipath = (IndexPath*)nipath;
							ParamPathInfo* nppath = inipath->path.param_info;
							IndexOptInfo* iinfo = inipath->indexinfo;

							// Preserve the parameterization if possible...
							if (iinfo->indexoid == entry->index_oid && ((bool)pinfo) == ((bool)nppath))
							{
								if ((pinfo == NULL && nppath == NULL) ||
								    (pinfo && nppath && bms_compare(pinfo->ppi_req_outer, nppath->ppi_req_outer) == 0))
								{
									if (tpath == NULL)
									{
										// Find an exact param match first.
										tpath = inipath;
									}
									else if (((bool)ipath->path.pathkeys) == ((bool)inipath->path.pathkeys))
									{
										// Override if the sort pathkeys matters...
										tpath = inipath;
									}
								}
							}
						}
					}

					if (!tpath)
					{
						foreach(l, rel->pathlist)
						{
							struct Path* nipath = lfirst(l);
							if (nipath != NULL && IsA(nipath, IndexPath))
							{
								IndexPath* inipath = (IndexPath*)nipath;
								ParamPathInfo* nppath = inipath->path.param_info;
								IndexOptInfo* iinfo = inipath->indexinfo;
								// Preserve the parameterization if possible...
								if (iinfo->indexoid == entry->index_oid && ((bool)pinfo) == ((bool)nppath))
								{
									// Allow for new path to be param-subset of the old one (i.e., relaxation).
									if (pinfo && nppath && bms_is_subset(nppath->ppi_req_outer, pinfo->ppi_req_outer))
									{
										tpath = inipath;
										break;
									}
								}
							}
						}
					}

					if (!tpath)
					{
						foreach(l, rel->pathlist)
						{
							struct Path* nipath = lfirst(l);
							if (nipath != NULL && IsA(nipath, IndexPath))
							{
								IndexPath* inipath = (IndexPath*)nipath;
								ParamPathInfo* nppath = inipath->path.param_info;
								IndexOptInfo* iinfo = inipath->indexinfo;
								// Of last resort, just try the index itself without outer rel
								if (iinfo->indexoid == entry->index_oid && !nppath)
								{
									tpath = inipath;
									break;
								}
							}
						}
					}

					if (tpath != NULL)
					{
						// Hackery to preserve parallel + costs
						tpath->path.parallel_aware = ipath->path.parallel_aware;
						tpath->path.parallel_safe = ipath->path.parallel_safe;
						tpath->path.parallel_workers = ipath->path.parallel_workers;
						tpath->loop_count = ipath->loop_count;
						tpath->partial_path = ipath->partial_path;
						tpath->indexselectivity = ipath->indexselectivity;
						tpath->indextotalcost = ipath->indextotalcost;
						tpath->path.rows = ipath->path.rows;
						tpath->path.startup_cost = ipath->path.startup_cost;
						tpath->path.total_cost = ipath->path.total_cost;
						tpath->path.param_info = ipath->path.param_info;
						// This seems even darker....
						tpath->path.pathkeys = ipath->path.pathkeys;
						tpath->path.pathtarget = ipath->path.pathtarget;
						// Attempt an override.. Just do a memcpy()  and try to fix up the RelOptInfo...
						memcpy(ipath, tpath, sizeof(IndexPath));
						ipath->path.parent = ipparent;
						overwritten = true;
						break;
					}

					if (overwritten)
					{
						break;
					}

					if (!overwritten && ipath->path.pathtype == T_IndexOnlyScan && list_length(rel->indexlist) > 0)
					{
						// Case where we would no longer generate an IndexOnlyScan.....
						// Just try to degrade it to an IndexScan...
						ipath->indexinfo = (IndexOptInfo*)list_nth(rel->indexlist, 0);
						ipath->path.pathtype = T_IndexScan;
						break;
					}
				}
			}
		}
	}
}
