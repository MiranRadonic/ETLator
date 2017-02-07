from logging import Logger
import sys
import csv
from tempfile import NamedTemporaryFile
import shutil
from datetime import datetime


class Table(object):
    """ Table in a database. Abstract class. """

    def __init__(self, name, column_names, pkey=None, show_progress=False,
                 replace_None_on_insert=None,
                 local_log_path=None, master_logger=None,
                 counters=None, src_table=None, default_args=None):
        self.name = name
        self.column_names = column_names
        self.operation_counter = 0
        self.insert_counter = 0
        self.pkey = pkey
        self.src_table = src_table
        self.default_args = default_args
        self.local_log_path = None
        self.master_logger = None
        self.counters = None
        self.paths = None
        self.show_progress = show_progress
        self.replace_None_on_insert=replace_None_on_insert
        tables = None
        if default_args is not None:
            (self.local_log_path, self.master_logger, self.counters, self.paths, tables) = default_args
        if counters is not None:
            self.counters = counters
        if master_logger is not None:
            self.master_logger = master_logger
        if local_log_path is not None:
            self.local_log_path = local_log_path
        if self.local_log_path is not None:
            self.logger = Logger(self.name, self.local_log_path, 'DEBUG', self.master_logger, self.paths)
        else:
            self.logger = None

        if tables is not None:
            tables.append(self)
        if self.local_log_path is not None:
            splitpath = self.local_log_path.split('/')[:-1]
            splitpath.append('DOC_' + self.name + '.txt')
            self.doc_path = '/'.join(splitpath)
        self.finalized = False


    def copy(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        row = self.get_next_row()
        if row is None:
            raise StopIteration
        else:
            return row

    def next(self):
        return(self.__next__())

    def get_next_row(self):
        pass

    def insert(self, row):
        pass

    def lookup(self, row, columns=None):
        pass

    def auto_transfer(self):
        if self.src_table is None:
            self.log('ERROR', 'Auto transfer failed! Source table not defined.')
            if self.counters is not None:
                self.counters['errors'] += 1
        try:
            for row in self.src_table:
                self.insert(row)
        except:
            self.log('ERROR', 'Auto transfer failed!')
            if self.counters is not None:
                self.counters['errors'] += 1

    def replace_None(self,row):
        if self.replace_None_on_insert is None:
            return row
        for k in row.keys():
            if row[k] is None:
                if k in self.replace_None_on_insert.keys():
                    row[k] = self.replace_None_on_insert[k]
        return row

    def log(self, level, message):
        if self.logger is not None:
            self.logger.log(level, message)

    def finalize(self):
        if self.finalized == True:
            return
        self.logger.finalize()
        if self.doc_path is not None:
            txt = 'Table name: ' + self.name + '\n'
            txt += 'Columns: ' + ', '.join(self.column_names) + '\n'
            if self.pkey is not None:
                txt += 'Primary key: ' + self.pkey + '\n'
            if self.src_table is not None:
                txt += 'Source table: ' + self.src_table.name + '\n'
            if self.counters is not None:
                txt += 'Nr of inserts: ' + str(self.insert_counter) + '\n'
                print(self.name + ' done - ' + str(self.insert_counter) + ' inserts')
            if self.doc_path.endswith('.txt'):
                try:
                    with open(self.doc_path, 'w+') as doc_file:
                        doc_file.write(txt)
                except:
                    pass
        self.finalized = True


class SQLTable(Table):

    """ Table in SQL compatible database """
    """ WARNING! Do not use as source and destination at the same time! """

    def __init__(self, connector,
                 name, column_names,
                 pkey=None, commit_count=0, select_query=None, cache_size=1000,
                 batch_size=None, show_progress=False,
                 replace_None_on_insert=None,
                 id_insert=False, local_log_path=None, master_logger=None,
                 counters=None, src_table=None, default_args=None):

        super(SQLTable, self).__init__(name=name, column_names=column_names,
                                       pkey=pkey, show_progress=False,
                                       replace_None_on_insert=None,
                                       local_log_path=local_log_path,
                                       master_logger=master_logger,
                                       counters=counters, src_table=None,
                                       default_args=default_args)
        self.connector = connector
        self.cursor = connector.cursor()
        self.commit_count = commit_count
        self.id_insert = id_insert
        if self.id_insert == False:
            self.insert_columns = [c for c in column_names if c != pkey]
        else:
            self.insert_columns = column_names

        self.select_query = select_query
        if self.select_query is None:
            self.select_query = "SELECT " + ', '.join(column_names) + " FROM " + self.name

        self.srcconnector = self.connector.copy()
        self.srccursor = self.srcconnector.cursor()
        self.srccursor.execute(self.select_query)
        self.counter = 0
        self.cache_size = cache_size
        self.src_cache= []
        self.lkp_cache = []
        self.batch_size = batch_size
        self.batch_rows = []


    def copy(self):
        return SQLTable(connector=self.connector,
                             name=self.name, column_names=self.column_names,
                             pkey=self.pkey, commit_count=self.commit_count,
                             select_query=self.select_query,
                             show_progress=self.show_progress,
                             replace_None_on_insert=self.replace_None_on_insert,
                             id_insert=self.id_insert,
                             local_log_path=self.local_log_path,
                             master_logger=self.master_logger,
                             counters=self.counters,
                             src_table=self.src_table,
                             default_args=self.default_args)

    def get_next_row(self):
        if self.show_progress:
            count = self.get_count()
            perc = (self.counter * 100) // count
            sys.stdout.write('\r')
            sys.stdout.write("%s [%-20s] %d%% - %d of %d" % (self.name, '='*(perc//5), perc, self.counter, count))
            if self.counter == count:
                sys.stdout.write('\n')
            sys.stdout.flush()
        self.counter += 1

        try:
            if len(self.src_cache) == 0:
                self.src_cache =self.srccursor.fetchmany(self.cache_size)
            row = self.src_cache[0]
            self.src_cache = self.src_cache[1:]
            return dict(zip(self.column_names, row))
            #return dict(zip(self.column_names, self.srccursor.fetchone()))
        except Exception as e:
            # print e.message
            # if self.show_progress:
            #     count = self.get_count()
            #     perc = (count * 100) // count
            #     sys.stdout.write('\r')
            #     sys.stdout.write("%s [%-20s] %d%% - %d of %d" % (self.name, '='*(perc//5), perc, count, count))
            #     sys.stdout.write('\n')
            #     sys.stdout.flush()
            return None

    def get_count(self):
        return self.srccursor.rowcount

    def autocommit(self):
        if self.commit_count is not None:
            if self.operation_counter >= self.commit_count:
                self.operation_counter = 0
                try:
                    self.connector.commit()
                except:
                    self.log('ERROR', 'Autocommit failed!')
                    if self.counters is not None:
                        self.counters['errors'] += 1

    def insert(self, row):
        row = self.replace_None(row)
        self.operation_counter += 1
        query = "INSERT INTO " + self.name + "("
        firstcol = True
        for column in self.insert_columns:
            if firstcol == True:
                firstcol = False
            else:
                query += ", "
            query += column
        query += ") VALUES ("
        firstcol = True
        vals = []
        for column in self.insert_columns:
            vals.append(row[column])
            if firstcol == True:
                firstcol = False
            else:
                query += ", "
            query += "%s"
        query += ")"
        if self.batch_size is None:
            try:
                self.cursor.execute(query, tuple(vals))
                self.log('INFO', 'Insert Successful, row = ' + str(row))
                if self.counters is not None:
                    self.counters['inserts'] += 1
                self.insert_counter += 1
            except Exception as e:
                self.log('ERROR', 'Insert failed! Query: "' + query + '", Row: "' + str(row) + '"''", DBerror: "' + e.message +'"')
                if self.counters is not None:
                    self.counters['errors'] += 1
            self.autocommit()
        else:
            self.batch_rows.append(tuple(vals))
            if self.counters is not None:
                self.counters['inserts'] += 1
            self.insert_counter += 1
            if len(self.batch_rows) >= self.batch_size:
                try:
                    self.cursor.executemany(query, self.batch_rows)
                    self.cursor.commit()
                    self.log('INFO', 'Batch Insert Successful')
                except Exception as e:
                    self.log('ERROR', 'Batch Insert failed! Query: "' + query + '"''", DBerror: "' + e.message +'"')
                    if self.counters is not None:
                        self.counters['errors'] += 1



    def update(self, row, columns=None):
        self.operation_counter += 1
        query = "UPDATE " + self.name + " SET "
        vals = []
        if columns is None:
            firstcol = True
            for column in self.insert_columns:
                if firstcol == True:
                    firstcol = False
                else:
                    query += ", "
                query += column + " = %s"
                vals.append(row[column])
        else:
            firstcol = True
            for column in columns:
                if firstcol == True:
                    firstcol = False
                else:
                    query += ", "
                query += column + " = %s"
                vals.append(row[column])
        query += " WHERE " + self.pkey + " = %s"
        vals.append(row[self.pkey])
        try:
            self.cursor.execute(query, tuple(vals))
        except Exception as e:
            self.log('ERROR', 'Update failed! Query: "' + query + '", DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
        self.log('INFO', 'Update Successful, row = ' + str(row))
        if self.counters is not None:
            self.counters['updates'] += 1
        self.autocommit()

    def lookup(self, row, columns=None):
        result = {}
        try:
            for r in self.lkp_cache:
                same = True
                if columns is not None:
                    for c in columns:
                        if r[c] != row[c]:
                            same = False
                else:
                    for c in self.column_names:
                        if r[c] != row[c]:
                            same = False
                if same == True:
                    return r

        except:
            pass
        result = self.lookup_raw(row, columns)
        if result is not None:
            if len(self.lkp_cache) > self.cache_size:
                self.lkp_cache.pop()
            self.lkp_cache.append(result)
        return result

    def lookup_raw(self, row, columns=None):
        query = "SELECT " + ', '.join(self.column_names) + " FROM " + self.name + " WHERE "
        vals = []
        if columns is None:
            if self.pkey is not None:
                query += self.pkey + " = %s"
                vals.append(row[self.pkey])
            else:
                firstcol = True
                for column in self.column_names:
                    if firstcol == True:
                        firstcol = False
                    else:
                        query += " AND "
                    query += column + " = %s"
                    vals.append(row[column])
        else:
            firstcol = True
            for column in columns:
                if firstcol == True:
                    firstcol = False
                else:
                    query += " AND "
                query += column + " = %s"
                vals.append(row[column])
        try:
            self.cursor.execute(query, tuple(vals))
            try:
                return dict(zip(self.column_names, self.cursor.fetchone()))
            except:
                return None
        except Exception as e:
            self.log('ERROR', 'Lookup failed! Query: "' + query + '", DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
            #raise

    def finalize(self):
        super(SQLTable, self).finalize()
        self.connector.commit()
        self.connector.close()


class SCDTable(SQLTable):

    """ Slowly changing dimension type 2 in SQL compatible databse """
    """ WARNING! Do not use as source and destination at the same time! """

    def __init__(self, connector,
                 name, column_names, scd_lookup_columns,
                 date_from_cloumn, date_to_column,
                 date_from_value=datetime.now(), date_to_value=datetime.now(),
                 predecessor_column=None, active_flag_column=None,
                 active_flag_active_value='T', active_flag_inactive_value='F',
                 pkey=None, commit_count=0, select_query=None,
                 show_progress=False,
                 id_insert=False, local_log_path=None, master_logger=None,
                 counters=None, src_table=None, default_args=None):

        super(SCDTable, self).__init__(connector=connector,
                     name=name, column_names=column_names,
                     pkey=pkey, commit_count=commit_count,
                     select_query=select_query,
                     show_progress=show_progress,
                     id_insert=id_insert,
                     local_log_path=local_log_path, master_logger=master_logger,
                     counters=counters, src_table=None, default_args=default_args)

        self.scd_lookup_columns = scd_lookup_columns
        self.date_from_cloumn = date_from_cloumn
        self.date_to_column = date_to_column
        self.date_to_value=date_to_value
        self.date_from_value=date_from_value
        self.active_flag_column=active_flag_column
        self.active_flag_active_value=active_flag_active_value
        self.active_flag_inactive_value=active_flag_inactive_value
        self.predecessor_column = predecessor_column

    def copy(self):
        return SCDTable(connector=self.connector,
                             name=self.name, column_names=self.column_names,
                             scd_lookup_columns=self.scd_lookup_columns,
                             date_from_cloumn=self.date_from_cloumn,
                             date_to_column=self.date_to_column,
                             date_from_value=self.date_from_value,
                             date_to_value=self.date_to_value,
                             active_flag_column=self.active_flag_column,
                             active_flag_active_value=self.active_flag_active_value,
                             active_flag_inactive_value=self.active_flag_inactive_value,
                             pkey=self.pkey, commit_count=self.commit_count,
                             select_query=self.select_query,
                             show_progress=self.show_progress,
                             id_insert=self.id_insert,
                             local_log_path=self.local_log_path,
                             master_logger=self.master_logger,
                             counters=self.counters,
                             src_table=None,
                             default_args=self.default_args)

    def scd_update(self, row):
        oldrow = self.lookup(row, self.scd_lookup_columns)
        if oldrow is not None:
            try:
                oldrow[self.active_flag_column] = self.active_flag_inactive_value
                oldrow[self.date_to_column] = self.date_from_value
            except:
                pass
            try:
                self.update(oldrow, [self.active_flag_column, self.date_to_column])
            except:
                self.update(oldrow, [self.date_to_column])

            if self.predecessor_column is not None:
                if self.pkey is not None:
                    row[self.predecessor_column] = oldrow[self.pkey]
                else:
                    self.log('WARNING', """Predecessor column set,
                                        but primary key is not in table: """
                                        + self.name)
                    if self.counters is not None:
                        self.counters['warnings'] += 1

        row[self.date_from_cloumn] = self.date_from_value
        row[self.date_to_column] = self.date_to_value
        row[self.active_flag_column] = self.active_flag_active_value

        self.insert(row)

    def lookup(self, row, columns=None):
        query = "SELECT " + ', '.join(self.column_names) + " FROM " + self.name + " WHERE "
        vals = []
        if columns is None:
            if self.pkey is not None:
                query += self.pkey + " = %s AND " + self.active_flag_column + " = %s"
                vals.append(row[self.pkey])
                vals.append(active_flag_active_value)
            else:
                firstcol = True
                for column in self.column_names:
                    if firstcol == True:
                        firstcol = False
                    else:
                        query += " AND "
                    query += column + " = %s"
                    vals.append(row[column])
        else:
            firstcol = True
            for column in columns:
                if firstcol == True:
                    firstcol = False
                else:
                    query += " AND "
                query += column + " = %s"
                vals.append(row[column])
            try:
                query += " AND " + self.active_flag_column + " = %s"
                vals.append(active_flag_active_value)
            except:
                pass

        try:
            self.cursor.execute(query, tuple(vals))
            try:
                d = dict(zip(self.column_names, self.cursor.fetchone()))
                return d
            except:
                return None
        except Exception as e:
            self.log('ERROR', 'Lookup failed! Query: "' + query + '", DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
            #raise

    def lookup_all(self, row, columns=None):
        query = "SELECT " + ', '.join(self.column_names) + " FROM " + self.name + " WHERE "
        vals = []
        if columns is None:
            if self.pkey is not None:
                query += self.pkey + " = %s"
                vals.append(row[self.pkey])
            else:
                firstcol = True
                for column in self.column_names:
                    if firstcol == True:
                        firstcol = False
                    else:
                        query += " AND "
                    query += column + " = %s"
                    vals.append(row[column])
        else:
            firstcol = True
            for column in columns:
                if firstcol == True:
                    firstcol = False
                else:
                    query += " AND "
                query += column + " = %s"
                vals.append(row[column])
        try:
            self.cursor.execute(query, tuple(vals))
            try:
                rows = self.cursor.fetchall()
                return [dict(zip(self.column_names, row)) for row in rows]
            except:
                return None
        except Exception as e:
            self.log('ERROR', 'Lookup failed! Query: "' + query + '", DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
            #raise


class CSVTable(Table):

    """ Table in CSV file """

    def __init__(self, name, column_names, filename,
                 pkey=None, show_progress=False,
                 local_log_path = None, master_logger = None,
                 counters = None, src_table=None, default_args=None,
                 delimiter=',', quotechar='"', escapechar=None,
                 skipinitialspace=False, lineterminator='\n',
                 quoting=csv.QUOTE_MINIMAL, doublequote=False,
                 skipheader=False, dialect=None):

        super(CSVTable, self).__init__(name=name, column_names=column_names,
                                       pkey=pkey,
                                       local_log_path=local_log_path,
                                       master_logger=master_logger,
                                       counters=counters,
                                       src_table=None,
                                       default_args=default_args)
        self.filename = filename
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.lineterminator = lineterminator
        self.escapechar = escapechar
        self.skipinitialspace = skipinitialspace
        self.quoting = quoting
        self.doublequote = doublequote
        self.dialect = dialect
        self.skipheader = skipheader

        self.tempfile = NamedTemporaryFile(delete=False)

        #with open(filename, 'rb') as self.csvfile:#, self.tempfile:
        try:
            self.csvfile = open(filename, 'rb+')
        except:
            self.csvfile = open(filename, 'wb+')

        if self.dialect is not None:
            self.reader = csv.DictReader(self.csvfile,
                                    fieldnames=self.column_names,
                                    dialect=self.dialect)
            self.writer = csv.DictWriter(self.tempfile,
                                    fieldnames=self.column_names,
                                    dialect=self.dialect)
        else:
            self.reader = csv.DictReader(self.csvfile,
                                     fieldnames=self.column_names,
                                     delimiter=self.delimiter,
                                     quotechar=self.quotechar,
                                     escapechar=self.escapechar,
                                     skipinitialspace=self.skipinitialspace,
                                     lineterminator=self.lineterminator,
                                     quoting=self.quoting,
                                     doublequote=self.doublequote)
            self.writer = csv.DictWriter(self.tempfile,
                                     delimiter=self.delimiter,
                                     fieldnames=self.column_names,
                                     quotechar=self.quotechar,
                                     escapechar=self.escapechar,
                                     skipinitialspace=self.skipinitialspace,
                                     lineterminator=self.lineterminator,
                                     quoting=self.quoting,
                                     doublequote=self.doublequote)

        if self.skipheader:
            self.reader.next()


        self.show_progress = show_progress
        self.counter = 0
        if self.show_progress:
            self.count = self.get_count()

    def copy(self):
        return CSVTable(name=self.name, column_names=self.column_names,
                     filename=self.filename,
                     pkey=self.pkey,
                     local_log_path=self.local_log_path,
                     master_logger=self.master_logger,
                     counters=self.counters, src_table=None, default_args=self.default_args,
                     delimiter=self.delimiter, quotechar=self.quotechar,
                     escapechar=self.escapechar,
                     skipinitialspace=self.skipinitialspace,
                     lineterminator=self.lineterminator,
                     quoting=self.quoting, doublequote=self.doublequote,
                     skipheader=self.skipheader, dialect=self.dialect)

    def get_next_row(self):
        if self.show_progress:
            count = self.count
            perc = (self.counter * 100) // count
            sys.stdout.write('\r')
            sys.stdout.write("%s [%-20s] %d%% - %d of %d" % (self.name, '='*(perc//5), perc, self.counter, count))
            if self.counter == count:
                sys.stdout.write('\n')
            sys.stdout.flush()
        self.counter += 1

        row = self.reader.next()
        if row is StopIteration:
            return None
        return row

    def get_count(self):
        counter = 0
        for i in self.copy():
            counter += 1
        self.count = counter
        return counter

    def insert(self, row):
        try:
            self.writer.writerow(row)
        except Exception as e:
            print(e.message)
            self.log('ERROR', 'Insert failed! Error: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1

        self.log('INFO', 'Insert Successful, row = ' + str(row))
        if self.counters is not None:
            self.counters['inserts'] += 1
        self.insert_counter += 1

    def lookup(self, row, columns=None):
        try:
            if self.dialect is not None:
                lkpreader = csv.DictReader(self.csvfile,
                                        fieldnames=self.column_names,
                                        dialect=self.dialect)
            else:
                lkpreader = csv.DictReader(self.csvfile,
                                         fieldnames=self.column_names,
                                         delimiter=self.delimiter,
                                         quotechar=self.quotechar,
                                         escapechar=self.escapechar,
                                         skipinitialspace=self.skipinitialspace,
                                         lineterminator=self.lineterminator,
                                         quoting=self.quoting,
                                         doublequote=self.doublequote)

            if self.skipheader:
                lkpreader.next()

            for srcrow in lkpreader:
                if columns is None:
                    if self.pkey is not None:
                        if srcrow[self.pkey] == row[self.pkey]:
                            return srcrow
                    else:
                        if srcrow == row:
                            return srcrow
                else:
                    same = True
                    for column in columns:
                        if srcrow[column] != row[column]:
                            same = False
                    if same:
                        return srcrow
            return None
        except Exception as e:
            print(e.message)
            self.log('ERROR', 'Lookup failed! Error: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1

    def finalize(self):
        self.csvfile.close()
        shutil.move(self.tempfile.name, self.filename)


class MongoDBTable(Table):
    """ Flat JSON object in MongoDB database """
    """ WARNING! Do not use as source and destination at the same time! """

    def __init__(self, name, column_names, dbName, collection, uri=None,
                 pkey=None, show_progress=False, replace_None_on_insert=None,
                 local_log_path=None, master_logger=None,
                 counters=None, src_table=None, default_args=None):
        super(MongoDBTable, self).__init__(name=name, column_names=column_names,
                                           pkey=pkey, show_progress=False,
                                           replace_None_on_insert=None,
                                           local_log_path=local_log_path,
                                           master_logger=master_logger,
                                           counters=counters, src_table=None,
                                           default_args=default_args)
        self.uri = uri
        self.dbName = dbName
        self.collName = collection
        self.json = __import__('json')
        self.pymongo = __import__('pymongo')

        self.client = None
        if self.uri is None:
            self.client = self.pymongo.MongoClient()
        else:
            self.client = self.pymongo.MongoClient(self.uri)

        self.db = self.client[self.dbName]
        self.collection = self.db[self.collName]

        self.cursor = self.collection.find()

    def copy(self):
        return MongoDBTable(name=self.name, column_names=self.column_names,
                             dbName=self.dbName, collection=self.collName,
                             uri=self.uri,
                             pkey=self.pkey, show_progress=self.show_progress,
                             replace_None_on_insert=self.replace_None_on_insert,
                             local_log_path=self.local_log_path,
                             master_logger=self.master_logger,
                             counters=self.counters,
                             src_table=self.src_table, default_args=self.default_args)

    def get_next_row(self):
        if self.show_progress:
            count = self.count
            perc = (self.counter * 100) // count
            sys.stdout.write('\r')
            sys.stdout.write("%s [%-20s] %d%% - %d of %d" % (self.name, '='*(perc//5), perc, self.counter, count))
            if self.counter == count:
                sys.stdout.write('\n')
            sys.stdout.flush()
        self.counter += 1

        row = self.cursor.next()
        if row is StopIteration:
            return None
        return row

    def get_count(self):
        return self.cursor.count

    def insert(self, row):
        try:
            if replace_None_on_insert is not None:
                row = self.replace_None(row)
            self.collection.insert_one(self.json.dumps(row))
            self.log('INFO', 'Insert Successful, row = ' + str(row))
            if self.counters is not None:
                self.counters['inserts'] += 1
            self.insert_counter += 1
        except Exception as e:
            self.log('ERROR', 'Insert failed! Row: "' + str(row) + '"''", DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1

    def lookup(self, row, columns=None):
        row2 = dict()
        if columns is not None:
            for col in columns:
                row2[col] = row[col]
        else:
            if self.pkey is not None:
                row2[self.pkey] = row[self.pkey]
            else:
                for col in self.column_names:
                    row2[col] = row[col]
        cursor2 = None
        try:
            cursor2 = self.collection.find(self.json.dumps(row2))
        except Exception as e:
            self.log('ERROR', 'Lookup failed! DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
        res = None
        for r in cursor2:
            res = r
            break
        return res

    def lookup_all(self, row, columns=None):
        row2 = dict()
        if columns is not None:
            for col in columns:
                row2[col] = row[col]
        else:
            if self.pkey is not None:
                row2[self.pkey] = row[self.pkey]
            else:
                for col in self.column_names:
                    row2[col] = row[col]
        cursor2 = None
        try:
            cursor2 = self.collection.find(self.json.dumps(row2))
        except Exception as e:
            self.log('ERROR', 'Lookup failed! DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
        res = []
        for r in cursor2:
            res.append(r)
        return res

    def update(self, row, columns=None):
        row2 = dict()
        if columns is not None:
            for col in columns:
                row2[col] = row[col]
        else:
            if self.pkey is not None:
                row2[self.pkey] = row[self.pkey]
            else:
                for col in self.column_names:
                    row2[col] = row[col]
        count = 0
        try:
            res = self.collection.update_many(self.json.dumps(row2), self.json.dumps(row))
            count = res.matched_count
        except Exception as e:
            self.log('ERROR', 'Update failed! DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
        self.log('INFO', 'Update Successful, row = ' + str(row))
        if self.counters is not None:
            self.counters['updates'] += count

    def update_one(self, row, columns=None):
        row2 = dict()
        if columns is not None:
            for col in columns:
                row2[col] = row[col]
        else:
            if self.pkey is not None:
                row2[self.pkey] = row[self.pkey]
            else:
                for col in self.column_names:
                    row2[col] = row[col]
        try:
            self.collection.update_one(self.json.dumps(row2), self.json.dumps(row))
        except Exception as e:
            self.log('ERROR', 'Update failed! DBerror: "' + e.message +'"')
            if self.counters is not None:
                self.counters['errors'] += 1
        self.log('INFO', 'Update Successful, row = ' + str(row))
        if self.counters is not None:
            self.counters['updates'] += count

    def finalize(self):
        super(MongoDBTable, self).finalize()
        self.cursor.close()
