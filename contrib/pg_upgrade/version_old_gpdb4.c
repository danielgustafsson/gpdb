/*
 *	version_old_gpdb4.c
 *
 *	GPDB-version-specific routines
 */
#include "pg_upgrade.h"

#include "access/transam.h"

/*
 * old_GPDB4_dump_array_types
 *
 *	While being in this file, and being specific for upgrading from GPDB4, it
 *	needs to run in the new cluster. Since array types for base relation types
 *	came in PostgreSQL 8.3 and CREATE TABLE automatically creates them, we
 *	need to synchronize the OIDs for these so perform a mini dump after the
 *	restore into the new cluster for use when upgrading the segments.
 */
void
old_GPDB4_dump_array_types(migratorContext *ctx, Cluster whichCluster)
{
	ClusterInfo    *new_cluster = &ctx->new;
	DbInfo		   *active_db;
	PGconn		   *conn;
	PGresult	   *res;
	int				dbnum;
	int				i;
	char		   *typname;
	char		   *typoid;
	char		   *typnamespace;
	FILE		   *script = NULL;
	char			output_path[MAXPGPATH];

	prep_status(ctx, "Dumping array types");

	snprintf(output_path, sizeof(output_path), "%s/%s", ctx->cwd, ARRAY_DUMP_FILE);

	for (dbnum = 0; dbnum < new_cluster->dbarr.ndbs; dbnum++)
	{
		active_db = &new_cluster->dbarr.dbs[dbnum];
		conn = connectToServer(ctx, active_db->db_name, whichCluster);

		res = executeQueryOrDie(ctx, conn,
								"SELECT ta.oid, ta.typname, ta.typnamespace "
								"FROM   pg_type t "
								"       JOIN pg_class ON t.typname = relname "
								"       JOIN pg_type ta ON ta.oid = t.typarray "
								"GROUP BY 1, 2, 3");

		if (PQntuples(res) > 0)
		{
			if (!script && (script = fopen(output_path, "w")) == NULL)
				pg_log(ctx, PG_FATAL, "Could not create necessary file:  %s\n",
					   output_path);
		}

		for (i = 0; i < PQntuples(res); i++)
		{
			typoid = pg_strdup(ctx, PQgetvalue(res, i, PQfnumber(res, "oid")));
			typname = pg_strdup(ctx, PQgetvalue(res, i, PQfnumber(res, "typname")));
			typnamespace = pg_strdup(ctx, PQgetvalue(res, i, PQfnumber(res, "typnamespace")));

			fprintf(script,
					"SELECT binary_upgrade.preassign_arraytype_oid('%s'::pg_catalog.oid, "
																  "'%s'::text, "
																  "'%s'::pg_catalog.oid);\n",
					 typoid, typname, typnamespace);

			pg_free(typoid);
			pg_free(typname);
			pg_free(typnamespace);
		}

		PQclear(res);
		PQfinish(conn);
	}
	check_ok(ctx);

	if (script)
	{
		fclose(script);

		pg_log(ctx, PG_REPORT, "\n\n"
			   "| In upgrading from GPDB4, new array types\n"
			   "| are created for base rowtypes. In order to\n"
			   "| dispatch these OIDs to the QE segments,\n"
			   "| pg_upgrade has left a dump file which need\n"
			   "| to be placed in the working directory of\n"
			   "| pg_upgrade on the QE before upgrading:\n"
			   "| %s\n\n", ARRAY_DUMP_FILE);
	}
}

/*
 * old_8_3_check_for_money_data_type_usage()
 *	8.2 -> 8.3
 *	Money data type was widened from 32 to 64 bits. It's incompatible, and we have no
 *  support for converting it.
 */
void
old_GPDB4_check_for_money_data_type_usage(migratorContext *ctx, Cluster whichCluster)
{
	ClusterInfo *active_cluster = (whichCluster == CLUSTER_OLD) ?
	&ctx->old : &ctx->new;
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status(ctx, "Checking for invalid 'name' user columns");

	snprintf(output_path, sizeof(output_path), "%s/tables_using_money.txt",
			 ctx->cwd);

	for (dbnum = 0; dbnum < active_cluster->dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname,
					i_attname;
		DbInfo	   *active_db = &active_cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(ctx, active_db->db_name, whichCluster);

		/*
		 * 
		 */
		res = executeQueryOrDie(ctx, conn,
								"SELECT n.nspname, c.relname, a.attname "
								"FROM	pg_catalog.pg_class c, "
								"		pg_catalog.pg_namespace n, "
								"		pg_catalog.pg_attribute a "
								"WHERE	c.oid = a.attrelid AND "
								"		NOT a.attisdropped AND "
								"		a.atttypid = 'pg_catalog.money'::pg_catalog.regtype AND "
								"		c.relnamespace = n.oid AND "
								/* exclude possibly orphaned temp tables */
							 	"		n.nspname != 'pg_catalog' AND "
								"		n.nspname !~ '^pg_temp_' AND "
								"		n.nspname !~ '^pg_toast_temp_' AND "
								"		n.nspname != 'information_schema' ");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		i_attname = PQfnumber(res, "attname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen(output_path, "w")) == NULL)
				pg_log(ctx, PG_FATAL, "Could not create necessary file:  %s\n", output_path);
			if (!db_used)
			{
				fprintf(script, "Database:  %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname),
					PQgetvalue(res, rowno, i_attname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (found)
	{
		fclose(script);
		pg_log(ctx, PG_REPORT, "fatal\n");
		pg_log(ctx, PG_FATAL,
			   "| Your installation contains the \"money\" data type in\n"
			   "| user tables.  This data type changed its internal\n"
			   "| format between your old and new clusters so this\n"
			   "| cluster cannot currently be upgraded.  You can\n"
			   "| remove the problem tables and restart the migration.\n"
			   "| A list of the problem columns is in the file:\n"
			   "| \t%s\n\n", output_path);
	}
	else
		check_ok(ctx);
}
