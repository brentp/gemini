#!/usr/bin/env python
import os
import shutil
import sys
import uuid

import sqlalchemy
import database as gemini_db
import gemini_load_chunk


def _run(session, chunk_db, *cmds):
    e = session.bind
    connection = e.raw_connection()

    cursor = connection.cursor()

    cmd = "attach ? as toMerge"
    cursor.execute(cmd, (chunk_db,))
    session.begin(subtransactions=True)

    for cmd in cmds:
        cursor.execute(cmd)

    cmd = "detach toMerge"
    cursor.execute(cmd)

    session.commit()


def append_variant_info(session, chunk_db):
    """
    Append the variant and variant_info data from a chunk_db
    to the main database.
    """
    _run(session, chunk_db, "INSERT INTO variants SELECT * FROM toMerge.variants",
            "INSERT INTO variant_impacts SELECT * FROM toMerge.variant_impacts")

def append_sample_genotype_counts(session, chunk_db):
    """
    Append the sample_genotype_counts from a chunk_db
    to the main database.
    """
    _run(session, chunk_db, "INSERT INTO sample_genotype_counts \
           SELECT * FROM toMerge.sample_genotype_counts")


def append_sample_info(session, chunk_db):
    """
    Append the sample info from a chunk_db
    to the main database.
    """
    _run(session, chunk_db, "create table samples as select * from toMerge.samples where 1=0",
                            "INSERT INTO samples SELECT * FROM toMerge.samples")


def append_resource_info(session, chunk_db):
    """
    Append the resource info from a chunk_db
    to the main database.
    """
    cmd = "INSERT INTO resources SELECT * FROM toMerge.resources"
    _run(session, chunk_db, cmd)


def append_version_info(session, chunk_db):
    """
    Append the version info from a chunk_db
    to the main database.
    """
    cmd = "INSERT INTO version SELECT * FROM toMerge.version"
    _run(session, chunk_db, cmd)

def append_vcf_header(session, chunk_db):
    """
    Append the vcf_header from a chunk_db
    to the main database.
    """
    cmd = "INSERT INTO vcf_header SELECT * FROM toMerge.vcf_header"
    _run(session, chunk_db, cmd)

def append_gene_summary(session, chunk_db):
    """
    Append the gene_summary from a chunk_db
    to the main database.
    """
    cmd = "INSERT INTO gene_summary SELECT * FROM toMerge.gene_summary"
    _run(session, chunk_db, cmd)

def append_gene_detailed(session, chunk_db):
    """
    Append the gene_detailed from a chunk_db
    to the main database.
    """
    cmd = "INSERT INTO gene_detailed SELECT * FROM toMerge.gene_detailed"
    _run(session, chunk_db, cmd)


def update_sample_genotype_counts(main_curr, chunk_db):
    """
    Update the main sample_genotype_counts table with the
    counts observed in one of the chunked databases (chunk_db)
    """
    from . import database

    session, metadata = database.get_session_metadata(chunk_db)

    if session.bind.name == "sqlite":
        session.execute('PRAGMA synchronous=OFF')
        session.execute('PRAGMA journal_mode=MEMORY')

    cmd = "SELECT sample_id, num_hom_ref, \
                  num_het, num_hom_alt, \
                  num_unknown FROM sample_genotype_counts"

    conn = session.connection()
    main_conn = main_curr.connection()

    for row in conn.execute(cmd):
        main_conn.execute("""UPDATE sample_genotype_counts
                          SET num_hom_ref = num_hom_ref + ?,
                              num_het = num_het + ?,
                              num_hom_alt = num_hom_alt + ?,
                              num_unknown = num_unknown + ?
                          WHERE sample_id= ? """,
                          (row['num_hom_ref'],
                           row['num_het'],
                           row['num_hom_alt'],
                           row['num_unknown'],
                           row['sample_id']))
    session.close()


def merge_db_chunks(args):

    # open up a new database
    if os.path.exists(args.db):
        os.remove(args.db)
    from . import database

    gemini_db.create_tables(args.db, gemini_load_chunk.get_extra_effects_fields(args) if args.vcf else [])

    session, metadata = database.get_session_metadata(args.db)

    if session.bind.name == "sqlite":
        session.execute('PRAGMA synchronous=OFF')
        session.execute('PRAGMA journal_mode=MEMORY')

    # create the gemini database tables for the new DB
    databases = []
    for db in args.chunkdbs:
        databases.append(db)

    for idx, dba in enumerate(databases):

        db = dba[0]

        append_variant_info(session, db)

        # we only need to add these tables from one of the chunks.
        if idx == 0:
            append_sample_genotype_counts(session, db)
            append_sample_info(session, db)
            append_resource_info(session, db)
            append_version_info(session, db)
            append_vcf_header(session, db)
            append_gene_summary(session, db)
            append_gene_detailed(session, db)
        else:
            update_sample_genotype_counts(session, db)

    if args.index:
        gemini_db.create_indices(session)

    session.close()


def merge_chunks(parser, args):
    errors = []
    for try_count in range(2):
        try:
            if try_count > 0:
                tmp_dbs = [os.path.join(args.tempdir, "%s.db" % uuid.uuid4())
                           for _ in args.chunkdbs]
                for chunk_db, tmp_db in zip(args.chunkdbs, tmp_dbs):
                    shutil.copyfile(chunk_db[0], tmp_db)
                    chunk_db[0] = tmp_db

                output_db = args.db
                args.db = os.path.join(args.tempdir, "%s.db" % uuid.uuid4())

            merge_db_chunks(args)

            if try_count > 0:
                shutil.move(args.db, output_db)
                for tmp_db in tmp_dbs:
                    os.remove(tmp_db)
            break
        except sqlalchemy.exc.OperationalError, e:
            errors.append(str(e))
            sys.stderr.write("OperationalError: %s\n" % e)
    else:
        raise Exception("Attempted workaround for SQLite locking issue on NFS "
                        "drives has failed. One possible reason is that the temp directory "
                        "%s is also on an NFS drive. Error messages from SQLite: %s"
                        % (args.tempdir, " ".join(errors)))
