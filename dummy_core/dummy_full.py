#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import random
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
import re
from integration_core import Integration
import datetime
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML
from dummy_core._version import __desc__

# Your Specific integration imports go here, make sure they are in requirements!
import requests
import jupyter_integrations_utility as jiu
#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Dummy(Integration):
    # Static Variables
    # The name of the integration
    name_str = "dummy"
    instances = {}


    # These are te variables that can be set using environmental variables
    # Note, the actual variable will have the name_str and _ prepended
    # Value in custom_evars: conn_default
    # Actual integration variable: dummy_conn_default
    # How to set in Environment: JUPYTER_DUMMY_CONN_DEFAULT="someconn"

    custom_evars = ["conn_default", "default_pass"]


    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    # These don't have to be the same as the Environment variables, but if the are not listed here, a user cannot set them with set
    # Note the actual variable will have the name_str and _ prepended
    # Value in custom_allowed_set_opts: conn_default
    # Actual integration variable: dummy_conn_default

    custom_allowed_set_opts = ["conn_default", "default_pass", "rand_query_min_time", "rand_query_max_time", "rand_query_min_results", "rand_query_max_results", "rand_query_min_columns", "rand_query_max_columns"]


    # These are the actual variables defined
    # Note the actual variable will have the name_str and _ prepended
    # Value in custom_allowed_set_opts: conn_default
    # Actual integration variable: dummy_conn_default
    myopts = {}
    myopts["conn_default"] = ["prod", "Default instance to connect with"]
    myopts["default_pass"] = ["password", "Default password to use to show successful login"]
    myopts["rand_query_min_time"] = [2, "Number of seconds that a random query will take at a minimum"]
    myopts["rand_query_max_time"] = [30, "Number of seconds that a radom query will take at a minimum"]
    myopts["rand_query_min_results"] = [10, "Minimum Number of BS results"]
    myopts["rand_query_max_results"] = [500, "Maximum Number of BS results"]
    myopts["rand_query_min_columns"] = [2, "Minimum Number of BS Columns in results"]
    myopts["rand_query_max_columns"] = [20, "Maximum Number of BS Columns in results"]






    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Dummy, self).__init__(shell, debug=debug)
        self.debug = debug
        self.custom_evars = [f"{self.name_str}_{x}" for x in self.custom_evars]
        self.custom_allowed_set_opts = [f"{self.name_str}_{x}" for x in self.custom_allowed_set_opts]

        #Add local variables to opts dict
        for k in self.myopts.keys():
            newk = f"{self.name_str}_{k}"
            self.opts[newk] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()

    def customAuth(self, instance):
        # result codes:
        #  0   Success result is -1 (Error)
        # -1   Unknown Error (Default
        # -2   Error in connect (Probably password issues?
        # -3   Instance not Found

        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            print("Instance %s not found in instances - Connection Failed" % instance)
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst['session'] = None
            mypass = ""
            if inst['enc_pass'] is not None:
                mypass = self.ret_dec_pass(inst['enc_pass'])
                inst['connect_pass'] = ""
            if mypass == self.opts["dummy_default_pass"][0]:
                inst['session'] = requests.Session()
                result = 0
            else:
                print(f"Unable to connect to Dummy Instance {instance} because you didn't use the dummy password of {self.opts['dummy_default_pass'][0]}")
                result = -2

        return result


    def validateQuery(self, query, instance):
        bRun = True
        bReRun = False

        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Example Validation

        # Warn only - Don't change bRun
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter

        # Warn and do not allow submission
        # There is no way for a user to submit this query
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun

    def parseQuery(self, query):
        # qtime - how much time to take
        # qresults - Number of results
        # qerror - fake an error (pass error=True to make this work)

        out_query = {
                       "qtime": None,
                       "qresults": None,
                       "qcolumns": None,
                       "qerror": False
                    }


        for l in query.split("\n"):
            if l.find("=") >= 0:
                this_split = l.split("=")
                q_k = this_split[0].strip()
                q_v = this_split[1].strip()
                if q_k in out_query:
                    if q_k == "qerror":
                        if q_v.lower() == "true":
                            out_query[q_k] = True
                    else:
                        if self.debug:
                            print(f"Got {q_k} for {q_v}")
                        try:
                            out_query[q_k] = int(q_v)
                        except:
                            print("Query Time and Rows values must be an integer.")
                            print("Example: qtime=50")
                else:
                    if self.debug:
                        print(f"Provided (and ignored) Line: {l}")
        if out_query["qtime"] is None:
            qtime = random.randint(self.opts["dummy_rand_query_min_time"][0], self.opts["dummy_rand_query_max_time"][0])
            out_query["qtime"] = qtime

        if out_query['qresults'] is None:
            qresults = random.randint(self.opts["dummy_rand_query_min_results"][0], self.opts["dummy_rand_query_max_results"][0])
            out_query['qresults'] = qresults

        if out_query['qcolumns'] is None:
            qcolumns = random.randint(self.opts["dummy_rand_query_min_columns"][0], self.opts["dummy_rand_query_max_columns"][0])
            out_query['qcolumns'] = qcolumns

        if self.debug:
            print("Query Def:")
            print(f"{out_query}")
        return out_query

    def customQuery(self, query, instance, reconnect=True):
        mydf = None
        status = ""

        q_dict = self.parseQuery(query)

        print(f"Faking running a query for {q_dict['qtime']} seconds")
        time.sleep(q_dict['qtime'])

        if q_dict['qerror'] == True:
           status = "Failure - Some Dummy Error"
        else:
            if q_dict['qresults'] <= 0:
                status = "Success - No Results"
            else:

                out_dict = {}
                col_types = ['strings', 'dates', 'ints', 'floats']
                weird_words = ['happy', 'bad', 'fun', 'crap', 'Going somewhere?', 'this is weird', "Can I type more"]
                for i in range(q_dict['qcolumns']):

                    col_name = f"COLUMN_{i}"
                    col_type = random.choice(col_types)
                    if col_type == "dates":
                        out_dict[col_name] = [random.choice(pd.date_range(datetime.datetime(2000, 1, 1), datetime.datetime(2023, 1, 1))) for i in range(q_dict['qresults'])]
                    elif col_type == 'ints':
                        out_dict[col_name] = [random.randint(0, 10000) for i in range(q_dict['qresults'])]
                    elif col_type == 'floats':
                        out_dict[col_name] = [random.random() for i in range(q_dict['qresults'])]
                    elif col_type == 'strings':
                         out_dict[col_name] = [random.choice(weird_words) for i in range(q_dict['qresults'])]

                mydf = pd.DataFrame(out_dict)
                status = "Success"


        return mydf, status


# Display Help can be customized
    def customOldHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp('qtime=400')

    def retCustomDesc(self):
        return __desc__


    def customHelp(self, curout):
        n = self.name_str
        mn = self.magic_name
        m = "%" + mn
        mq = "%" + m
        table_header = "| Magic | Description |\n"
        table_header += "| -------- | ----- |\n"
        out = curout
        qexamples = []
        qexamples.append(["myinstance", "qtime=400'", "Run a Default Query and take 400 seconds"])
        out += self.retQueryHelp(qexamples)

        return out




    # This is the magic name.
    @line_cell_magic
    def dummy(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)

