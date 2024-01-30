from datetime import datetime, timedelta

from arrow import arrow
from psycopg import ClientCursor, connect

with connect("postgresql://postgres:postgres@127.0.0.1/prac", cursor_factory=ClientCursor) as conn:
    conn.execute('DROP TABLE IF EXISTS company')

    sql = """CREATE TABLE IF NOT EXISTS company
    ( 
        ID1  text NOT NULL,
        T1   timestamptz,
        T2   int4,
        IND  text,
        PRO  int4,
        DJJG float8,
        XZQH float8,
        F11  float8,
        F12  float8
    ) PARTITION BY RANGE (T1);"""
    conn.execute(sql)
    conn.execute('CREATE INDEX ON company(T1);')

    begin_date = "2022-01-01T00:00:00+0800"
    begin_obj = datetime.fromisoformat(begin_date)
    end_obj = begin_obj + timedelta(hours=10000)
    for x in arrow.Arrow.range("month", begin_obj, end_obj):
        tablename = f"company_{x.year}_{x.month}"
        sql = f"CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF company FOR VALUES FROM (%s) TO (%s);"
        conn.execute(sql, [x.isoformat(), x.shift(months=1).isoformat()])
        sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {tablename}_id1_idx ON {tablename}(id1)"
        conn.execute(sql)

    sql = """
    INSERT INTO company
    SELECT MD5(id::text),
           %s::timestamptz + INTERVAL '1 hours'*id,
           (RANDOM() * 12)::int,
           gen_hanzi(4),
           (RANDOM() * 33)::int, -- 0 to 33
           RANDOM() * 10000000,
           RANDOM() * 10000000,
           RANDOM() * 1000,
           RANDOM() * 1000
    FROM GENERATE_SERIES(1, 10000) AS t(id);
    """
    conn.execute(sql, [begin_date])
