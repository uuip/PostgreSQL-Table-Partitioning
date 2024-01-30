import psycopg
from psycopg import ClientCursor

with psycopg.connect("postgresql://postgres:postgres@127.0.0.1/prac", cursor_factory=ClientCursor) as conn:
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
    ) PARTITION BY LIST (PRO);"""
    conn.execute(sql)
    conn.execute('CREATE INDEX ON company(PRO);')

    for x in range(34):
        tablename = f"company_{x}"
        sql = f"CREATE TABLE IF NOT EXISTS {tablename} PARTITION OF company FOR VALUES in ({x});"
        conn.execute(sql)
        sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {tablename}_id1_idx ON {tablename}(ID1)"
        conn.execute(sql)

    begin_date = "2022-01-01 00:00:00 Asia/Shanghai"
    sql = """
    INSERT INTO company
    SELECT MD5(id::text),
           %s::timestamptz + INTERVAL '1 minutes'*id,
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
