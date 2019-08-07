import pandas as pd
import psycopg2, psycopg2.extras
from pathlib import Path

def percentage(a, b):
    return '%.1f%%'%(a/b*100) if b>0 else '0%'

# def add_xval(cur, num_folds, partition_name):
#     cur.execute("""UPDATE frame SET partitions = partitions || hstore('%s', chr(floor(random() * %d + 65)::int));""" % (partition_name, num_folds))
#     cur.execute("""INSERT INTO partition_ns (name, labels, created_on, summary, impl_version) values ('%s', array%s, now(), true, 2);""" %(partition_name, list(map(chr, range(65, 65+num_folds)))))

def summarize_v2(cur, partition_name, label):
    cur.execute("SELECT count(*) FROM frame where partitions -> '{}' = '{}' and partitions ? 'active';".format(partition_name, label))
    (active, ) = cur.fetchone()
    # cur.execute("SELECT count(*) FROM frame where partitions -> '{}' = '{}';".format(partition_name, label))
    # (partition_total, ) = cur.fetchone()
    cur.execute("SELECT count(*) FROM frame where partitions ?& array['{}', 'active'];".format(partition_name))
    (namespace_total, ) = cur.fetchone()
    print('  ', label, '=', active, '/', namespace_total, '=', percentage(active, namespace_total))

def get_summary(cur):
    cur.execute("SELECT name, labels, impl_version FROM partition_ns where summary=true;")
    summary = cur.fetchall()
    for partition_name, labels, impl_version in summary:
        print('Summary on', partition_name, '(namespaced)' if impl_version and impl_version>=2 else '')
        for label in labels:
            if impl_version == 2:
                summarize_v2(cur, partition_name, label)
            else:
                cur.execute("SELECT count(*) FROM frame where partitions ?& array['{}', 'active'];".format(label))
                (active, ) = cur.fetchone()
                if partition_name != 'active':
                    cur.execute("SELECT count(*) FROM frame where partitions ?| array{} and partitions ? 'active';".format(labels))
                    (namespace_total, ) = cur.fetchone()
                    print('  ', label, '=', active, '/', namespace_total, '=', percentage(active, namespace_total))
                else:
                    cur.execute("SELECT count(*) FROM frame;")
                    (total, ) = cur.fetchone()
                    print('  ', label, '=', active, '/', total, '=', percentage(active, total))

def metadata(DATABASE, init):
    conn = psycopg2.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM frame;")
    (r, ) = cur.fetchone()
    if r == 0:
        init(conn)
    get_summary(cur)
    conn.close()
