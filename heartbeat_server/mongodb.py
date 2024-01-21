from pymongo import MongoClient

CONNECTION_STRING = "mongodb://localhost:27017/"

client = MongoClient(CONNECTION_STRING)
db = client['cars-database']

class MongoDb:
    """
    A simple wrapper around the mongoDB
    database to provide a simple interface
    for inserting and querying data.
    """

    @property
    def db(self):
        return self._db

    def __init__(self, db_name: str):
        self._db = client[db_name]

    # def execute(self, sql: str, params: tuple = None):
    #     if params:
    #         self._cursor.execute(sql, params)
    #     else:
    #         self._cursor.execute(sql)
    #     self._db.commit()

    def add_meter_reading(self, reading={}):
        collection = self.db["meter_reading"]
        collection.insert_one(reading)

    def add_meter_readings(self, readings={}):
        collection = self.db["meter_reading"]
        collection.insert_many(readings)

    def delete_meter_reading(self, filters={}):
        collection = self.db["meter_reading"]
        collection.delete_one(filters)

    def delete_meter_readings(self, filters={}):
        collection = self.db["meter_reading"]
        collection.delete_many(filters)
        
    # def add_ccu(self, ccu: str):
    #     if not re.match(r'^MTRK[0-9]{12}$', ccu):
    #         raise Exception(f'Invalid ccu: {ccu}')
    #     self._db.execute('CREATE TABLE IF NOT EXISTS ccu (id INTEGER PRIMARY KEY, ccu VARCHAR UNIQUE, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
    #     try:
    #         self._db.execute('INSERT INTO ccu (ccu) VALUES (?)', (ccu,))
    #         ccu_id = self._db.execute('SELECT id FROM ccu WHERE ccu = ?', (ccu,)).fetchone()[0]
    #     except sqlite3.IntegrityError:
    #         ccu_id = self._db.execute('SELECT id FROM ccu WHERE ccu = ?', (ccu,)).fetchone()[0]
    #     return ccu_id
    
    # def add_meters(self, ccu_id: int, meters: list):
    #     self._db.execute('CREATE TABLE IF NOT EXISTS meters (id INTEGER PRIMARY KEY, meter VARCHAR UNIQUE, ccu_id INTEGER REFERENCES ccus(id), timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
    #     for meter in meters:
    #         if not re.match(r'^[A-Za-z]*[0-9]{12}$', meter):
    #             raise Exception(f'Invalid meter: {meter}')
    #         self._db.execute('INSERT OR IGNORE INTO meters (meter, ccu_id) VALUES (?, ?)', (meter, ccu_id))

    # def add(self, ccu: str, meter: list) -> bool:
    #     """
    #     Add a new ccu or meter list to the database.
    #     """
    #     # return ccu id from db after adding
    #     index = self.add_ccu(ccu)
    #     self.add_meters(index, meter)

    # def get_meters(self, ccu_no: str):
    #     """
    #     Get a ccu and its meters from the database.
    #     """
    #     ccu = self._db.execute('SELECT * FROM ccu WHERE ccu = ?', (ccu_no,)).fetchone()
    #     if not ccu:
    #         raise Exception(f'No ccu found: {ccu_no}')
    #     meters = [meter['meter'] for meter in self._db.execute('SELECT * FROM meters WHERE ccu_id = ?', (ccu['id'],)).fetchall()]
    #     return meters


    # def update_ccu(self, old_ccu: str, new_ccu: str):
    #     """
    #     Update a ccu value in the database.
    #     """
    #     ccu = self.get_ccu_and_meters(old_ccu)
    #     if ccu:
    #         self._db.execute('UPDATE ccu SET ccu = ? WHERE id = ?', (new_ccu, ccu['id']))
    #         return ccu['id']


    def close(self):
        self._db.close()