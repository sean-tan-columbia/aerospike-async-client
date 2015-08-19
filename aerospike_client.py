
# Copyright (c) 2015 Jinxiong Tan
# GNU General public licence

import aerospike as aero
import multiprocessing
import time
import datetime
import sys


# Usage example:
# records = [Record('key_1', {'bin': 'value_1'}), Record('key_2', {'bin': 'value_2'}), Record('key_3', {'bin': 'value_3'})]
# aerospike_client = aerospike_client.AsyncClient([(host_1:port_1), (host_2:port_2)], 'namespace', 'set', 604800)
# success_count, failure_records = aerospike_client.put(records)


class Record():

    def __init__(self, key, bins):
        """
        :param key: Aerospike key, should be a string
        :param bins: Aerospike bins, should be a dictionary
        :return: None
        """
        if type(bins) is dict:
            self.key = key
            self.bins = bins
        else:
            raise TypeError('Wrong types for bins')


class Client(object):

    def __init__(self, cluster, namespace, set_name, ttl, retry_limit, logger):
        self._cluster = cluster
        self._namespace = namespace
        self._set_name = set_name
        self._ttl = ttl
        self._retry_limit = retry_limit
        self._logger = logger

    def put(self, records):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def _log(self, content):
        log = '{timestamp}: {content}'.format(timestamp=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), content=content)
        if self._logger is None:
            print log
        else:
            self._logger.logging(log)


class SyncClient(Client):

    def __init__(self, cluster, namespace, set_name, ttl, retry_limit=3, logger=None):
        """
        :param cluster: Aerospike cluster, should have the following format, [(host_1: port_1), (host_2: port_2), ..., (host_n: port_n)]
        :param namespace: Aerospike namespace
        :param set_name: Aerospike set
        :param ttl: time to live for records
        :return: None
        """
        super(SyncClient, self).__init__(cluster, namespace, set_name, ttl, retry_limit, logger)
        self.aerospike_dao = []
        for node in cluster:
            self.aerospike_dao.append(_AerospikeDao(node, namespace, set_name, ttl))

    def put(self, records):
        failure_records = []
        total = len(records)
        put_count = 0

        self._log('Loading records to {0}'.format(self._cluster))
        for record in records:
            if not isinstance(record, Record):
                raise Exception('Wrong type for aerospike object')
            if put_count % 1000 == 0 and put_count > 0:
                self._log('Finished {0}%'.format(int(float(put_count)/total*100)))
            for aerospike_dao in self.aerospike_dao:
                for attempt in xrange(1 + self._retry_limit):
                    try:
                        aerospike_dao.put(record.key, record.bins)
                    except Exception as e:
                        print e
                    else:
                        break
                else:
                    failure_records.append(record.key)
            put_count += 1
        self._log('Finished 100%')
        return len(records) - len(failure_records), failure_records

    def get(self, key):
        try:
            bins = self.aerospike_dao[0].get(key)
        except Exception as e:
            print e
            return None
        else:
            return bins

    def close(self):
        for aerospike_dao in self.aerospike_dao:
            aerospike_dao.close()


class AsyncClient(Client):

    def __init__(self, cluster, namespace, set_name, ttl, retry_limit=3, logger=None, pool_size=4, queue_size=256):
        """
        :param cluster: Aerospike cluster, should have the following format, [(host_1: port_1), (host_2: port_2), ..., (host_n: port_n)]
        :param namespace: Aerospike namespace
        :param set_name: Aerospike set
        :param ttl: time to live for records
        :param retry_limit: limit for retrying times for failure records
        :param pool_size: number of processes to load records
        :param queue_size: the maximum capacity of blocking queue, by default it is set to 256
        :return: None
        """
        super(AsyncClient, self).__init__(cluster, namespace, set_name, ttl, retry_limit, logger)

        self._pool_size = pool_size
        self._queue_size = queue_size
        self._task_queue = multiprocessing.JoinableQueue()
        self._failure_queue = multiprocessing.Queue()

    def put(self, records):
        """
        :param records: Record object collection
        :return: success record count and collection of failure records (after retries)
        """

        processors = [_Processor(self._cluster, self._namespace, self._set_name, self._ttl, self._task_queue, self._failure_queue, self._retry_limit) for i in xrange(self._pool_size)]
        for processor in processors:
            processor.start()

        total = len(records)
        put_count = 0
        self._log('Loading records to {0}'.format(self._cluster))
        for record in records:
            while True:
                if self._task_queue.qsize() < self._queue_size:
                    break
                time.sleep(0.1)
            if not isinstance(record, Record):
                raise Exception('Wrong type for aerospike object')
            if put_count % 1000 == 0 and put_count > 0:
                self._log('Finished {0}%'.format(int(float(put_count)/total*100)))
            for node_index in xrange(len(self._cluster)):
                self._task_queue.put(_Put(node_index, record))
            put_count += 1
        self._log('Finished 100%')

        self._task_queue.join()
        for i in xrange(self._pool_size):
            self._task_queue.put(None)
        self._failure_queue.put(None)

        failure_records = []
        while True:
            failure_record = self._failure_queue.get()
            if failure_record is not None:
                failure_records.append(failure_record)
            else:
                break

        for processor in processors:
            processor.join()
            processor.terminate()

        return len(records) * len(self._cluster) - len(failure_records), failure_records

    def get(self, key):
        pass

    def close(self):
        pass


class _Processor(multiprocessing.Process):

    def __init__(self, cluster, namespace, set_name, ttl, task_queue, failure_queue, retry_limit):
        """
        :param task_queue: process-shared queue to contain tasks
        :param failure_queue: process-shared queue to contain failure records after retries
        :return: None
        """
        super(_Processor, self).__init__()
        self._task_queue = task_queue
        self._failure_queue = failure_queue
        self._retry_limit = retry_limit

        self.aerospike_dao = []
        for node in cluster:
            self.aerospike_dao.append(_AerospikeDao(node, namespace, set_name, ttl))

    def run(self):
        while True:
            next_task = self._task_queue.get()
            if next_task is None:
                self._task_queue.task_done()
                break
            result = next_task(self)
            if (not result) and next_task.retry_counter < self._retry_limit:
                next_task.retry()
                self._task_queue.put(next_task)
            elif not result:
                self._failure_queue.put(next_task.record.key)

            # task_done() should be called after appending records to failure queue since processes should be blocked until all failure records are captured
            self._task_queue.task_done()
        return

    def close(self):
        for dao in self.aerospike_dao:
            dao.close()

    def __del__(self):
        self.close()


class _Put():

    def __init__(self, dao_index, record):
        """
        :param dao_index: unique index for each node's aerospike-dao
        :param record: record to put
        :return: None
        """
        self.dao_index = dao_index
        self.record = record
        self.retry_counter = 0

    def retry(self):
        self.retry_counter += 1

    def __call__(self, processor):
        return processor.aerospike_dao[self.dao_index].put(self.record.key, self.record.bins)

    def __str__(self):
        return 'key={key},bins={bins}'.format(key=self.record.key, bins=self.record.bins)


class _AerospikeDao():

    def __init__(self, host, namespace, set_name, ttl):
        """
        :param host:
        :param namespace:
        :param set_name:
        :param ttl:
        :return:
        """
        self._namespace = namespace
        self._set_name = set_name
        self._ttl = ttl
        for attempt in xrange(3):
            try:
                self._aerospike_client = aero.client({'hosts': [host]}).connect()
            except Exception as e:
                print e
            else:
                break
        else:
            raise Exception('[Error] 3 failed attempts for connecting to {host}'.format(host=host))

    def put(self, key, bins):
        """
        :param key:
        :param bins:
        :return:
        """
        try:
            self._aerospike_client.put((self._namespace, self._set_name, key), bins, meta={'ttl': self._ttl})
        except Exception as e:
            print e
            return False
        else:
            return True

    def get(self, key):
        """
        :param key:
        :return:
        """
        try:
            (key, meta, bins) = self._aerospike_client.get((self._namespace, self._set_name, key))
        except Exception as e:
            print e
            return None
        else:
            return bins

    def close(self):
        self._aerospike_client.close()
