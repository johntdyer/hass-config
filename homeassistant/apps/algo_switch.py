import appdaemon.appapi as appapi
import sqlite3
from datetime import datetime, timedelta

#
# https://github.com/aneisch/home-assistant-config/blob/master/extras/appdaemon/apps/algo_timer.py
# Automatically turn off entity after averaged amount of time
#


def convert_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp)


class SmartTimer(appapi.AppDaemon):

    db = '/srv/hass/algo_switch.sqlite'

    def initialize(self):
        self.log("Begining SmartTimer setup")
        db = sqlite3.connect(self.db)
        db.execute(
            "CREATE TABLE IF NOT EXISTS `intervals` ( `start` FLOAT NOT NULL UNIQUE, `end` FLOAT UNIQUE, `split` INTEGER DEFAULT 0 CHECK(split in (0,1)), PRIMARY KEY(`start`,`end`) )")
        db.execute(
            "CREATE TABLE IF NOT EXISTS `averages` ( `intvl_start` FLOAT NOT NULL UNIQUE, `intvl_sum` FLOAT NOT NULL, `intvl_count` INTEGER NOT NULL, `intvl_avg` FLOAT NOT NULL )")
        self.log("Database setup complete!")
        self.listen_state(
            self.begin_interval, self.args['entity_id'], new=self.args.get('begin', 'on'))
        self.listen_state(
            self.end_interval, self.args['entity_id'], new=self.args.get('end', 'off'))
        intvl_cursor = db.execute("SELECT intvl_start FROM averages")
        if self.args.get('preload', True) and len(intvl_cursor.fetchall()) == 0:
            self.preload()
        self.log("Completed SmartTimer setup")
        db.commit()
        db.close()
        # Path to our DB as defined in apps.yaml
        #db_location = self.args["db_location"]

        # Set up callback and listen for state change of desired switch
        # self.listen_state(self.calculate_times,
        #                  self.args["entity_id"], db_location=db_location)
        # self.log("Initializing.. DB located at: {}".format(
        #    db_location), level='WARN')

    def schedule_off(self):
        db = sqlite3.connect(self.db)
        cur = db.cursor()
        cur.execute(
            "SELECT intvl_avg from averages where intvl_start = (select max(intvl_start) from averages)")
        last_avg = cur.fetchone()
        if last_avg:
            last_avg = last_avg[0]
        else:
            last_avg = self.args.get('default_interval', 12)
        self.log("Scheduling {} to turn {} in {} seconds".format(
            self.args['entity_id'], 'off', last_avg))
        self.run_in(self.average_exceeded, last_avg)
        db.commit()
        db.close()

    def begin_interval(self, entity, attribute, old, new, kwargs):
        timestamp = datetime.utcnow().timestamp()
        if new == self.args.get('begin', 'on'):
            self.log("{} changed to state {} at {}".format(
                entity, new, timestamp))
            self.schedule_off()
            db = sqlite3.connect(self.db)
            cur = db.cursor()
            cur.execute(
                "SELECT `start`, `end`, `split` from intervals where start = (select max(start) from intervals)")
            prev_intvl = cur.fetchone()
            if prev_intvl and prev_intvl[1]:
                out_duration = timestamp - prev_intvl[1]
                if out_duration < self.args.get('min_out', 15):
                    cur.execute(
                        "UPDATE intervals SET split = 1 where start = ?", (prev_intvl[0],))
                    cur.execute(
                        "DELETE from averages where intvl_start = ?", (prev_intvl[0],))
                    db.commit()
                    return
            cur.execute(
                "INSERT INTO intervals (`start`) values (?)", (timestamp,))
            db.commit()
            db.close()

    def end_interval(self, entity, attribute, old, new, kwargs):
        self.log("{} changed to state {}".format(entity, new))
        timestamp = datetime.utcnow().timestamp()
        if new == self.args.get('end', 'off'):
            db = sqlite3.connect(self.db)
            cursor = db.execute(
                "SELECT * from intervals where start = (select max(start) from intervals)")
            results = cursor.fetchall()
            # if len(results) == 1:
            self.log("Latest interval: {}".format(results[0]))
            latest_start = results[0][0]
            cursor.execute("UPDATE intervals SET end = ? WHERE start = ?",
                           (timestamp, latest_start))
            db.commit()
            cursor.execute("SELECT * from intervals")
            rows = cursor.fetchall()
            intvl_sum = 0
            for intvl in rows:
                begin = intvl[0]
                end = intvl[1]
                diff = end - begin
                intvl_sum += diff
            avg = intvl_sum / len(rows)
            self.log("Latest average: {}".format(avg))
            cursor.execute("INSERT INTO averages VALUES (?,?,?,?)",
                           (latest_start, intvl_sum, len(rows), avg))
            db.commit()
            db.close()

    # Calculate the average amount of time a switch is on

    def preload(self):
        # Calculate past lengths of time
        self.log("Preloading intervals from {}".format(self.args["hass_db"]))
        conn = sqlite3.connect(
            "file:" + self.args["hass_db"] + "?mode=ro", uri=True)
        c = conn.cursor()
        sql = "select {fields} from {table} where {conditions} order by date(last_changed) DESC {limit}"
        select = "last_changed"
        table = 'states'
        where = "entity_id = '{}' and state = '{}'".format(
            self.args['entity_id'], self.args.get('begin', 'on'))
        limit = ""
        if self.args.get('max_records', None):
            limit = "limit {}".format(int(self.args['max_records']))
        c.execute(sql.format(fields=select, table=table,
                             conditions=where, limit=limit))
        results = c.fetchall()

        times = []

        for row in results:
            try:
                prev_on = on
            except NameError:
                prev_on = False
            on = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')
            where = "state = '{}' and last_changed > '{}'".format(
                self.args.get('end ', 'off'), row[0])
            if prev_on:
                where += " and last_changed < '{}'".format(
                    prev_on.strftime('%Y-%m-%d %H:%M:%S.%f'))
            sql_full = sql.format(fields=select, table=table,
                                  conditions=where, limit='')
            self.log("Querying db: {}".format(sql_full), level='DEBUG')
            c.execute(sql_full)
            results = c.fetchall()
            # pick oldest record returned
            try:
                off = datetime.strptime(results[-1][0], '%Y-%m-%d %H:%M:%S.%f')
                self.log("Interval begin: {}".format(on), level="DEBUG")
                self.log("Interval end: {}".format(off), level="DEBUG")
            except IndexError:
                self.log("Could not find valid end time!")
                continue
            try:
                diff = timedelta.total_seconds(off - on)
                self.log("Duration length: {}".format(diff))
                if diff < 0:
                    self.log("PRELOAD: interval is negative")
                    del off
                    del on
                elif diff < self.args.get('min_duration', 0):
                    self.log(
                        "PRELOAD: Off state less than acceptable duration")
                    on = prev_on
                elif diff > self.args.get('max_interval', 3600):
                    self.log("PRELOAD: Interval larger than allowed ({}), dropping".format(
                        self.args.get('max_interval', 3600)))
                else:
                    times.append(diff)
                    self.log(
                        "PRELOAD: Found duration of {} seconds".format(diff))
            except NameError:
                pass

                # Calculate average time spent "on"
        average = sum(times, 0) / len(times)
        # Get total seconds from datetime hours, minutes, seconds
        #average_seconds = int(timedelta.total_seconds(average))
        self.log("Preload complete, average: {}".format(average))
        conn.close()
        # Schedule our turn_off action X seconds in the future, the average time entity has spent is "on"
        # self.log("Scheduling turn_off of %s in %s seconds." %
        #         (self.args["entity_id"], str(average_seconds)))
        #self.run_in(self.average_exceeded, average_seconds)

    def average_exceeded(self, kwargs):
        self.log("Average time exceeded, turning off %s" %
                 self.args["entity_id"])
        # Turn off specified entity
        self.turn_off(self.args["entity_id"])
