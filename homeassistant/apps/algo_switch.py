import appdaemon.appapi as appapi
import sqlite3
from datetime import datetime, timedelta

#
# https://github.com/aneisch/home-assistant-config/blob/master/extras/appdaemon/apps/algo_timer.py
# Automatically turn off entity after averaged amount of time
#


class SmartTimer(appapi.AppDaemon):
    def initialize(self):

        # Path to our DB as defined in apps.yaml
        db_location = self.args["db_location"]

        # Set up callback and listen for state change of desired switch
        self.listen_state(self.calculate_times,
                          self.args["entity_id"], db_location=db_location)
        self.log("Initializing.. DB located at: {}".format(db_location), level='WARN')

    # Calculate the average amount of time a switch is on
    def calculate_times(self, entity, attribute, old, new, kwargs):

        # Only execute if turned on
        if new == "on":

            # Calculate past lengths of time
            conn = sqlite3.connect(
                "file:" + kwargs["db_location"] + "?mode=ro", uri=True)
            c = conn.cursor()
            sql = "select {fields} from {table} where {conditions} order by date(last_changed) DESC {limit}"
            select = "state,last_changed"
            table = 'states'
            where = "entity_id = '{}'".format(self.args['entity_id'])
            limit = ""
            if self.args.get('max_records', None):
                limit = "limit {}".format(int(self.args['max_records']) * 2)
            c.execute(sql.format(fields=select, table=table,
                                 conditions=where, limit=limit))
            results = c.fetchall()
            conn.close()
            self.log("Entity {} changed.. reprocessing times".format(entity))

            records = 0
            times = []

            if self.args["debug"]:
                self.log(results)

            # Iterate through entity changed times and create a list of times spent "on"
            for result in results:
                if result[0] == "on":
                    try:
                        on
                    except NameError:
                        pass
                    else:
                        prev_on = on
                    on = strptime(result[1], '%Y-%m-%d %H:%M:%S.%f')
                    try:
                        off
                    except NameError:
                        pass
                    else:
                        if (on - off) < timedelta(seconds=self.args.get('min_duration')):
                            self.log("Off state less than acceptable duration")
                            on = prev_on
                else:
                    off = strptime(result[1], '%Y-%m-%d %H:%M:%S.%f')
                    # Takes "off" timestamp and subtracts "on" timestamp to find timedelta
                    try:
                        length = off - on
                    except NameError:
                        # If we have an 'off' with no 'on' we should...
                        continue
                    if self.args["debug"]:
                        self.log("Found length: " + str(length))
                    times.append(length)
                    records += 1

            # Calculate average time spent "on"
            average = sum(times, timedelta(0)) / len(times)
            # Get total seconds from datetime hours, minutes, seconds
            average_seconds = int(timedelta.total_seconds(average))

            if self.args["debug"]:
                self.log(
                    "Calculation complete, determined average on time of " + str(average))

            # Schedule our turn_off action X seconds in the future, the average time entity has spent is "on"
            self.log("Scheduling turn_off of %s in %s seconds." %
                     (self.args["entity_id"], str(average_seconds)))
            self.run_in(self.average_exceeded, average_seconds)

    def average_exceeded(self, kwargs):
        self.log("Average time exceeded, turning off %s" %
                 self.args["entity_id"])
        # Turn off specified entity
        self.turn_off(self.args["entity_id"])
