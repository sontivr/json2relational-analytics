import sys, os, subprocess
import csv
import json
from datetime import datetime as dt
import re
import argparse
import time


class ProcessJsonFiles(object):
    """
    Class to convert json data files into csv to load them into relational schema
    """

    def __init__(self):
        """
        Initialization and arguments parsing
        """

        ''' handle commandline args '''
        parser = argparse.ArgumentParser()
        parser.add_argument("input", help="json file or directory with json files")
        parser.add_argument("--host", default="localhost", help="hostname")
        parser.add_argument("-p", "--port", action="count", default=5432, help="port number")
        parser.add_argument("-d", "--dbname", default="testdb", help="database name")
        parser.add_argument("-u", "--user", default="RamanaSonti", help="user name to use to connect to the database")
        args = parser.parse_args()

        input = args.input
        self.host = args.host
        self.port = args.port
        self.dbname = args.dbname
        self.user = args.user

        ''' database connection string '''
        self.db_conn_str = 'psql -h {} -p {} -U {} {} -c '.format(self.host, self.port, self.user, self.dbname)

        self.load_ts = dt.now()
        self.processing_ts = dt.strftime(self.load_ts, '%Y%m%d%H%M%S')

        if os.path.isfile(input):
            self.input_type = 'file'
            self.inputfile = input
            self.output_dir = '{}_processed_{}'.format(os.path.dirname(input), self.processing_ts)

        if os.path.isdir(input):
            self.input_type = 'dir'
            self.input_dir = input
            self.output_dir = '{}_processed_{}'.format(input, self.processing_ts)

        os.mkdir(self.output_dir)
        print('output_dir={}'.format(self.output_dir))

        ''' counters files processed/loaded '''
        self.files_processed = 0
        self.files_loaded = 0

    def process_file(self, inputfile):
        """

        :param inputfile: json file to be processed
        :return:
        """
        with open(inputfile) as f:
            data = json.load(f)

        fname = os.path.splitext(os.path.basename(inputfile))[0]
        orders_outfile = '{}/{}.orders.csv'.format(self.output_dir, fname)
        lineitem_outfile = '{}/{}.line_items.csv'.format(self.output_dir, fname)

        lineitem_fp = open(lineitem_outfile, "w")
        orders_fp = open(orders_outfile, "w")

        lineitem_out = csv.writer(lineitem_fp)
        orders_out = csv.writer(orders_fp)

        for k, orders in data.items():
            assert k == 'orders'
            for order in orders:
                ''' line_items should go into its own csv file '''
                if order['line_items'] is not None:
                    for lineitem in order['line_items']:
                        lineitem_plus = {**lineitem, **{'order_id': order['id'], 'order_number': order['order_number'],
                                                        'user_id': order['user_id'], 'email': order['email'],
                                                        'phone': order['phone'], 'load_ts': self.load_ts}}
                        lineitem_out.writerow(lineitem_plus.values())
                    del (order['line_items'])

                order_plus = {**order, **{'load_ts': self.load_ts}}
                orders_out.writerow(order_plus.values())

        lineitem_fp.close()
        orders_fp.close()

        self.files_processed += 1

    def process_dir(self):
        """
        Iterate over the directory of json files
        :return:
        """
        for f in os.listdir(self.input_dir):
            inputfile = '{}/{}'.format(self.input_dir, f)
            self.process_file(inputfile)

    def convert_json2csv(self):
        """
        Convert json to csv format
        :return:
        """

        if self.input_type == 'file':
            self.process_file(self.inputfile)

        if self.input_type == 'dir':
            self.process_dir()
            os.rename(self.input_dir, '{}_{}'.format(self.input_dir, self.processing_ts))

    def load(self):
        """
        Load csv files into myapp.orders and myapp.line_items
        :return:
        """
        start = time.time()
        self.convert_json2csv()
        end = time.time()
        print('Converted {} file(s) into csv in {} sec'.format(self.files_processed, round(end - start, 3)))

        start = time.time()
        for f in os.listdir(self.output_dir):
            print('{}: Loading {}'.format(dt.now(), f))
            if re.match(".*orders.csv$", f):
                orders_csv = '{}/{}'.format(self.output_dir, f)
                cmd = 'cat {} | {} "COPY myapp.orders from STDIN DELIMITER \',\' NULL \'\' ;\"'.format(orders_csv,
                                                                                                       self.db_conn_str)

            if re.match(".*line_items.csv$", f):
                line_items_csv = '{}/{}'.format(self.output_dir, f)
                cmd = 'cat {} | {} \"COPY myapp.line_items from STDIN DELIMITER \',\' NULL\'\' ;\"'.format(
                    line_items_csv, self.db_conn_str)

            try:
                retcode = subprocess.call(cmd, shell=True)
                if retcode != 0:
                    print("Child returned", retcode, file=sys.stderr)
                    break
            except OSError as e:
                print("Execution failed:", e, file=sys.stderr)
                break

            self.files_loaded += 1

        cmd = '{} \"analyze myapp.orders; analyze myapp.line_items;\"'.format(self.db_conn_str)

        try:
            retcode = subprocess.call(cmd, shell=True)
            if retcode != 0:
                print("Child returned", retcode, file=sys.stderr)
        except OSError as e:
            print("Execution failed:", e, file=sys.stderr)

        end = time.time()
        print('Loaded {} files(s) into database in {} sec'.format(self.files_loaded, round(end - start, 3)))


if __name__ == '__main__':
    a = ProcessJsonFiles()
    a.load()
