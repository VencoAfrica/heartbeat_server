import sqlite3

from requests import delete

class Db:
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
        
    def add_ccu(self, ccu: str):
        self._db.execute('CREATE TABLE IF NOT EXISTS ccu (id INTEGER PRIMARY KEY, ccu VARCHAR, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
        self._db.execute('INSERT OR IGNORE INTO ccu (ccu) VALUES (?)', (ccu,))
        return self._db.execute('SELECT id FROM ccu WHERE ccu = ?', (ccu,)).fetchone()[0]
    
    def add_meters(self, ccu_id: int, meters: list):
        self._db.execute('CREATE TABLE IF NOT EXISTS meters (id INTEGER PRIMARY KEY, meter VARCHAR, ccu_id INTEGER REFERENCES ccus(id), timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
        for meter in meters:
            self._db.execute('INSERT OR IGNORE INTO meters (meter, ccu_id) VALUES (?, ?)', (meter, ccu_id))

    def add(self, ccu: str, meter: list, callback_url: str) -> bool:
        """
        Add a new ccu or meter list to the database.
        """
        # return ccu id from db after adding
        index = self.add_ccu(ccu)
        self.add_meters(index, meter)

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

    def update_ccu(self, old_ccu: str, new_ccu: str):
        """
        Update a ccu value in the database.
        """
        ccu = self.get_ccu_and_meters(old_ccu)
        if ccu:
            self._db.execute('UPDATE ccu SET ccu = ? WHERE id = ?', (new_ccu, ccu['id']))
            return ccu['id']


    def close(self):
        self._db.close()