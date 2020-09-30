# imports
import json
import sqlite3

from mixpanel_api import Mixpanel
import os.path

# Mixpanel Credentials
MIXPANEL_CONFIG = {
    'token': '822e86280408209502ba396ccce7fedb',
    'secret': '27aa5485bf91c8d3f8feaae0ad15e9e7',
    'out': 'data/out/events.json',
    'format': 'json',
    'params': {
        'from_date': '2020-01-01',
        'to_date': '2020-09-30'
    }
}

class TaskTrial:
    '''
    The TaskTrial instance just serves as an organized
    interface of the script.
    '''
    conn = None  # sqlite3 connection
    data = None  # contents of csv
    m = None     # Mixpanel instance

    def __init__(self):
        print('Context: __init__(self)')

        # Instantiate Mixpanel instance
        print('\tInstantiating Mixpanel instance...')
        self.m = Mixpanel(api_secret=MIXPANEL_CONFIG['secret'], token=MIXPANEL_CONFIG['token'])
        print('\tInstantiation successful.')

        print('\tDoes events.csv file exist?', end=' ')
        if not os.path.isfile(MIXPANEL_CONFIG['out']):
            print('Y')
            print('\tFile events.csv does not exist... Retrieving events from Mixpanel.')
            self.export_events(MIXPANEL_CONFIG)
            print('\tEvents successfully exported. Loading file...')
        else:
            print('N')
            print('\tFile events.csv already exists. Loading file.')

        self.data = self.load_csv()

        print('\tEstablishing connection.')
        self.conn = self.create_connection('lxf_trial_task.db')

        self.create_events_table(self.conn, self.data['rows'])
        self.insert_data(self.conn, self.data['cols'])

        print('Thank you for running me.')

    def export_events(self, cfg):
        '''
        simplifies use of Mixpanel.export_events
        and allows flexible use later on.
        :param cfg: dict: Mixpanel configuration
        :return: None
        '''
        print('Context: export_events(self, cfg)')
        print('\tExporting events...')
        self.m.export_events(
            output_file=cfg['out'],
            format=cfg['format'],
            params=cfg['params'],
        )
        print('\tEvents imported successfully.')

    def load_json(self):
        '''
        Loads events from Mixpanel as JSONL and converted to a python dict list.
        :return: {}: field names (rows) and the events in json.
        '''
        print('Context: load_json(self)')
        print('\tImporting JSON file')

        # rows are the field anmes, cols are the event data themselves in dict list
        data = {'rows': [], 'cols': []}
        with open(MIXPANEL_CONFIG['out'], 'r') as f:
            result = [json.loads(jline) for jline in f.read().splitlines()][0]
            rows = ['event']
            cols = []

            for dictionary in result:
                col = {'event': dictionary['event']}
                for key, value in dictionary['properties'].items():
                    nkey = key.replace('$', '').replace(' ', '_').lower()
                    if nkey not in rows:
                        rows.append(nkey)

                    # for non-string values, possibility of
                    # side-effect from client library
                    col[nkey] = str(value)
                cols.append(col)

            print('\tData stored successfully.')

            data['rows'] = rows
            data['cols'] = cols

        return data

    def create_connection(self, db_file):
        '''
        if database file exists, establishes connection of SQLite3 database
        else create SQLite3 database and establish connection.
        :param db_file: str: location of the sqlite3 database file
        :return: SQLite3 Connection object
        '''
        print('Context: create_connection(self, db_file)')

        conn = None

        try:
            conn = sqlite3.connect(database=db_file)
            print('\tDatabase connection successfully established.')
            return conn
        except sqlite3.Error as e:
            print('\t', e)

        return conn

    def create_events_table(self, conn, fields):
        '''
        creates the events table with event fields (name and properties)
        :param conn: SQLite3 Connection object
        :param fields: event fields (name and properties)
        :return: None
        '''
        print('Context: create_events_table(self, conn, fields)')

        print('\tPreparing create events table command.')
        cmd = '''

        CREATE TABLE IF NOT EXISTS events (
            id integer PRIMARY KEY AUTOINCREMENT,
        '''

        for i, v in enumerate(fields):
            cmd += '\t' + v + ' text'
            if i < len(fields) - 1:
                cmd += ',\n'

        cmd += '\n);'

        try:
            c = conn.cursor()
            c.execute(cmd)
        except sqlite3.Error as e:
            print(e)

        print('\tTable events successfully created.')

    def insert_data(self, conn, data):
        '''
        inserts event data to database
        :param conn: SQLite3 Connection Object
        :param data: lst: event data
        :return: None
        '''
        print('Context: insert_data(self)')

        # cache to prevent re-writing to db
        utils = {
            'i': [1]
        }

        # i is for keeping track how many records have been
        # written to the database
        for i, col in enumerate(data):
            placeholders = '?,' * len(col)
            placeholders = placeholders[:-1]
            values = []

            print(f'\t{i} of {len(data)}')

            sql = f'''INSERT INTO events ({', '.join(list(col.keys()))}) VALUES ({placeholders})'''

            cur = conn.cursor()
            cur.execute(sql, list(col.values()))
            conn.commit()

        return utils


TaskTrial()
