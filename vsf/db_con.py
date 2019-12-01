import sqlite3
import sys
import pandas as pd

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS cells4 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mdn INTEGER NOT NULL,
    app INTEGER NOT NULL,
    amplitude_id INTEGER NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100),
    event_time DATETIME,
    client_event_time DATETIME,
    client_upload_time DATETIME,
    server_upload_time DATETIME,
    event_id INTEGER NOT NULL,
    session_id INTEGER,
    event_type VARCHAR(100),
    amplitude_event_type VARCHAR(20),
    version_name VARCHAR(20),
    os_name VARCHAR(20),
    os_version VARCHAR(16),
    device_brand VARCHAR(20),
    device_manufacturer VARCHAR(20),
    device_model VARCHAR(20),
    device_carrier VARCHAR(20),
    country VARCHAR(20),
    language VARCHAR(20),
    location_lat REAL,
    location_lng REAL,
    ip_address VARCHAR(50),
    event_properties VARCHAR,
    user_properties VARCHAR,
    region VARCHAR(30),
    city VARCHAR(30),
    dma VARCHAR(100),
    device_family VARCHAR(20),
    device_type VARCHAR(20),
    platform VARCHAR(16),
    uuid VARCHAR,
    paying VARCHAR(30),
    start_version VARCHAR(50),
    user_creation_time REAL,
    library VARCHAR(100),
    idfa INTEGER,
    adid INTEGER,
    Event VARCHAR,
    Prop VARCHAR
       
);'''

CREATE_INDEX_SQLS = (
    'CREATE INDEX IF NOT EXISTS idk_mdn ON vsf_events (mdn);',
)


INSERT_ENTRY_SQL = '''
INSERT INTO vsf_events (
   mdn, app, amplitude_id, device_id, user_id, event_time,
   client_event_time, client_upload_time, server_upload_time,
   event_id, session_id, event_type, amplitude_event_type,
   version_name, os_name, os_version, device_brand,
   device_manufacturer, device_model, device_carrier, country,
   language, location_lat, location_lng, ip_address,
   event_properties, user_properties, region, city, dma,
   device_family, device_type, platform, uuid, paying,
   start_version, user_creation_time, library, idfa, adid, Event, Prop
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

class VSFDatabase(object):
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.counter = 0

    def close(self):
        self.conn.commit()
        self.conn.close()

    def create_tables(self):
        self.conn.execute(CREATE_TABLE_SQL)
        for sql in CREATE_INDEX_SQLS:
            self.conn.execute(sql)
        self.conn.commit()

    def insert(self, values):
        self.counter += 1
        sys.stderr.write("insert #{}\n".format(self.counter))
        cur = self.conn.cursor()
        cur.executemany(INSERT_ENTRY_SQL, values)
        self.conn.commit()
        sys.stderr.write("wrote {} rows\n".format(cur.rowcount))

    def insert_from_df(self, df, is_table_create):
        if_exists= 'append'
        if is_table_create == True:
            if_exists = 'replace'
        self.counter += 1
        sys.stderr.write("inserting dataframe #{}\n".format(self.counter))
        cur = self.conn.cursor()
        df.to_sql('vsf_events', self.conn, if_exists= if_exists, index=True)
        self.conn.commit()
        sys.stderr.write("wrote {} rows\n".format(cur.rowcount))
        

    def read_into_df(self):
        return pd.read_sql('select * from vsf_events', self.conn)
        
