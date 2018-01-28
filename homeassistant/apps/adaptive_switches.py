import appdaemon.appapi as appapi
from datetime import timedelta
import sqlite3

db_file = 'adaptive_switch.sqlite'


class AdaptiveSwitches(appapi.AppDaemon):

    def initialize(self):
        min_duration = self.args.get('ignore_duration', 15)          
        entity = self.args.get('entity_id', 'switch')
        with sqlite3.connect(db_file) as db:
            db.execute('CREATE TABLE IF NOT EXISTS state_history (time DATETIME, entity TEXT, attr TEXT, state TEXT )')
            db.execute('CREATE TABLE IF NOT EXISTS algo_result (entity TEXT, attr TEXT, algo TEXT, result TEXT)')
            db.commit()
        on_listener = self.listen_state(self.start_timer, entity=entity, new='on', old='off', duration=min_duration)
        off_listener = self.listen_state(self.stop_timer, entity=entity, new='off', old='on', duration=min_duration)


    def start_timer(self, entity, attr, old, new, **kwargs):
        with sqlite3.connect(db_file) as db:
            db.execute('INSERT INTO state_history ({}, {}, {}, {})'.format(str(self.datetime()), entity, attr, new))
            result = db.execute('SELECT FROM algo_result result where entity = "{}"'.format(entity))
            self.run_in(lambda entity, **_: self.set_state(entity, value='off'), resut.fetchone()['result'])


    def stop_timer(self, entity, attr, old, new, **kwargs):
        with sqlite3.connect(db_file) as db:
            db.execute('INSERT INTO state_history ({}, {}, {}, {})'.format(str(self.datetime()), entity, attr, new))
            db.execute('INSERT INTO algo_result ({}, {}, {}, {})'.format(entity, attr, 'avg', average_duration(entity)))

    def average_duration(self, entity):
        with sqlite3.connect(db_file) as db:
            result = db.execute("SELECT * from state_history where entity = '{}'".format(entity))
            count = len(result)/2
            dur_sum = timedelta()
            for row in result:
                if row['state'] == 'on':
                    dur_start = row['time']
                if row['state'] == 'off':
                    dur_end = row['time']
                    dur_sum += dur_end - dur_start
        return dur_sum/count
 
