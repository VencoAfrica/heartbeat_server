import sqlite3

class MySQLITE:
    """
    A simple wrapper around the sqlite3
    database to provide a simple interface
    for inserting and querying data.
    """

    @property
    def db(self):
        return self._db

    def __init__(self, db_name: str):
        self._db = sqlite3.connect(db_name)
        self._db.row_factory = sqlite3.Row
        self._cursor = self._db.cursor()

    def execute(self, sql: str, params: tuple = None):
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        self._db.commit()

    def db_add(self, table: str, data: dict, ccu_id=None):
        if table == 'ccu':
            self._db.execute('CREATE TABLE IF NOT EXISTS ccu (id INTEGER PRIMARY KEY INCERMENT NOT NULL, ccu VARCHAR, timestamp INTEGER DEFAULT CURRENT_TIMESTAMP)')
            self._db.execute('INSERT INTO ccu (ccu) VALUES (?)', (data['ccu'],))
            return self._db.execute('SELECT id FROM ccu WHERE ccu = ?', (data['ccu'],)).fetchone()[0]
        if table == 'meters':
            self._db.execute('CREATE TABLE IF NOT EXISTS meters (id INTEGER PRIMARY KEY, meter TEXT, ccu_id INTEGER REFERENCES ccus(id), timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            for meter in data['meters']:
                self._db.execute('INSERT INTO meters (meter, ccu_id) VALUES (?, ?)', (meter, ccu_id))
        else:
            raise Exception(f'Unknown table {table}')
        

    def add(self, ccu: str, meter: list, callback_url: str) -> bool:
        """
        Add a new ccu or meter list to the database.
        """
        # return ccu id from db after adding
        index = self.db_add('ccu', ccu)
        self.db_add('meters', meter, index)

    def get_ccu_and_meters(self, ccu: str) -> dict:
        """
        Get a ccu and its meters from the database.
        """
        ccu = self._db.execute('SELECT * FROM ccu WHERE ccu = ?', (ccu,)).fetchone()
        meters = self._db.execute('SELECT * FROM meters WHERE ccu_id = ?', (ccu['id'],)).fetchall()
        return {
            'ccu': ccu,
            'meters': meters
        }

    def close(self):
        self._db.close()