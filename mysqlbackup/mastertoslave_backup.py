"""
by chaochao
数据库热备脚本。
this the the instant backup setter script of mysql databases;
salve databases can duplicate the master databases when dml manipulation happens.
"""
import datetime
import os

import pymysql

DEBUG_MODE = True


db = {
    # """
    # this is the account of the slave
    # """,
    'db_slave': {
        'host': '172.16.11.34',
        'user': 'admin',
        'passwd': 'dPe~w~qz.l',
        'port': 3306,
        'name': 'techcloud'
    },
    # """
    # this is the master account, we need to use this account to get the status of the master
    # we need to know the bin log and the position .
    # 'mysqld-relay-bin.000002', 1036,
    # mysql-bin.000015
    # """
    'db_master': {
        'host': '172.16.12.229',
        'user': 'admin',
        'passwd': 'jsptpd',
        'port': 3306,
        'name': 'techcloud'
    }
}

backup = {
    'master': {
        'one': 'techcloud'
    },
    'slave': {
        'one': 'techcloud'
    },
    'master_backup_path': '/data/wwwroot/scripts/db_backup/',
    'hour': 1,
    'weekday': 7,
}
period = {
    'master_slave_monitor': 60,
    'master_backup': 1,
    'backup_time': {'hour': 1}
}


# noinspection PyDefaultArgument
class DBMonitor(object):
    slave_check_sql = "show slave status;"
    slave_stop_sql = "stop slave;"
    slave_start_sql = "start slave;"
    master_check_sql = "show master status;"

    def __init__(self):
        self.slave_config = db['db_slave']
        self.master_config = db['db_master']

    @staticmethod
    def master_slave_monitor():
        monitor = DBMonitor()
        status = monitor.check_slave_status()
        if status:
            print("normal")
        else:
            print("abnormal")
            DBMonitor.master_backup()

        monitor.get_master_status()

    @staticmethod
    def master_backup():
        """
        dump datafiles to destination path with the suffix of date.
        :return: None.
        """
        if datetime.datetime.now().hour == backup['hour'] and datetime.datetime.now().weekday() == backup['weekday']:
            master = db['db_master']
            del master['port']
            slave = db['db_slave']
            del slave['port']
            for key, database in backup['master'].items():
                copy_cmd = "mysqldump -h{} -u{} -p{} --opt {}| mysql -h{} -u{} -p{} -C {}" . format(master['host'], master['user'], master['passwd'], database, slave['host'], slave['user'], slave['passwd'], backup['slave'][key])
                backup_cmd = "mysqldump -h{} -u{} -p{} {} > {}{}.sql".format(slave['host'], slave['user'], slave['passwd'], backup['slave'][key], backup['master_backup_path'], datetime.datetime.now().strftime('%Y-%m-%d'))
                os.system(copy_cmd)
                os.system(backup_cmd)

    @staticmethod
    def get_db(db_config={}):
        """
        python database connection instance with configed data.
        :param db_config: db config information.
        :return: the database instance and the cursor.
        """
        db_instance = None
        db_cursor = None
        if db_config:
            db_instance = pymysql.connect(db_config['host'], db_config['user'], db_config['passwd'], db_config['name'])
            db_cursor = db_instance.cursor()
        return db_instance, db_cursor

    def check_slave_status(self, db_config={}):
        """
        check the status of slave database.
        :param db_config: the database configuration
        :return: boolean True: normal, False :abnormal
        """
        sql = DBMonitor.slave_check_sql
        if not db_config:
            db_config = self.slave_config
        results = DBMonitor.execute_sql(db_config, sql)
        content = {'slave_io_running': None, 'slave_sql_running': None, 'last_error': None}
        if results:
            result = results[0]
            content['slave_io_running'] = result[10]
            content['slave_sql_running'] = result[11]
            content['last_error'] = result[19]
            if content['slave_io_running'] == 'Yes' and content['slave_sql_running'] == 'Yes':
                print('backup stable', content)
                return True
            else:
                return False

    def slave_job(self, job="start", db_config={}):
        """
        start the back_up job of slave,
        if "start" is not given, stop the job.
        :param job: "start" else others.
        :param db_config: salve db config.
        :return: the result of sql execution.
        """
        if not db_config:
            db_config = self.slave_config

        if job == "start":
            sql = DBMonitor.slave_start_sql
        else:
            sql = DBMonitor.slave_stop_sql
        results = DBMonitor.execute_sql(db_config, sql)
        return results

    def get_master_status(self, db_config={}):
        """
        the status of master
        the programe need to know the file and the position of current database;
        :param db_config:
        :return: the position of current data file.
        """
        sql = DBMonitor.master_check_sql
        if not db_config:
            db_config = self.master_config
        data = {'File': None, 'Postion': None}
        results = DBMonitor.execute_sql(db_config, sql)

        if results:
            result = results[0]
            data['File'] = result[0]
            data['Postion'] = result[1]
        print(data)
        return data

    @staticmethod
    def execute_sql(db_config={}, sql=''):
        """
        sql execution.
        :param db_config:
        :param sql: the sql statement.
        :return: the result of execution.
        """
        database, cursor = DBMonitor.get_db(db_config)
        results = None
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            print('sql mistake-->', e.__str__())
        finally:
            database.close()
            return results


if __name__ == "__main__":
    # DBMonitor.master_backup()
    # the entrance
    DBMonitor.master_slave_monitor()
    # print(datetime.datetime.now().weekday())
