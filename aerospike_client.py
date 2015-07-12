# Author: Jinxiong Tan
# Created on 07/10/2015
# Licence: N/A

import aerospike as aero
import multiprocessing
import time
import datetime
import sys


class Record():

    def __init__(self, key, bins):
        """
        :param key:
        :param bins:
        :return:
        """
        self.key = key
        self.bins = bins


class AsyncClient():

    def __init__(self, cluster, namespace, set_name, ttl, pool_size=4, retry_limit=3, logger=None):
        """
        :param cluster:
        :param namespace:
        :param set_name:
        :param ttl:
        :param pool_size:
        :param retry_limit:
        :return:
        """
        self._cluster = cluster
        self._namespace = namespace
        self._set_name = set_name
        self._ttl = ttl

        self._pool_size = pool_size
        self._retry_limit = retry_limit
        self._logger = logger

        self._task_queue = multiprocessing.JoinableQueue()
        self._failure_queue = multiprocessing.Queue()
        self._processors = [Processor(cluster, namespace, set_name, ttl, self._task_queue, self._failure_queue, self._retry_limit) for i in xrange(self._pool_size)]

    def put(self, records):

        for processor in self._processors:
            processor.start()

        total = len(records)
        put_count = 0
        for record in records:
            if put_count == 0:
                self._log('Loading records to {0}'.format(self._cluster))
            elif put_count % 1000 == 0:
                self._log('Finished {0}%'.format(int(float(put_count)/total*100)))
            elif put_count == total - 1:
                 self._log('Finished 100%')
            for node_index in xrange(len(self._cluster)):
                self._task_queue.put(Put(node_index, record))
            put_count += 1

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

        for processor in self._processors:
            processor.join()
            processor.terminate()

        return len(records) - len(failure_records), failure_records

    def _log(self, content):
        log = '{timestamp}: {content}'.format(timestamp=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), content=content)
        if self._logger is None:
            print log
        else:
            self._logger.logging(log)


class Processor(multiprocessing.Process):

    def __init__(self, cluster, namespace, set_name, ttl, task_queue, failure_queue, retry_limit):
        """
        :param task_queue:
        :param failure_queue:
        :return:
        """
        super(Processor, self).__init__()
        self._task_queue = task_queue
        self._failure_queue = failure_queue
        self._retry_limit = retry_limit

        self.aerospike_dao = []
        for node in cluster:
            self.aerospike_dao.append(AerospikeDao(node, namespace, set_name, ttl))

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


class Put():

    def __init__(self, dao_index, record):
        """
        :param dao_index:
        :param record:
        :return:
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


class AerospikeDao():

    def __init__(self, node, namespace, set_name, ttl):
        """
        :param node:
        :param namespace:
        :param set_name:
        :param ttl:
        :return:
        """
        self._namespace = namespace
        self._set_name = set_name
        self._ttl = ttl
        for attempt in xrange(2):
            try:
                self._aerospike_client = aero.client({'hosts': [node]}).connect()
            except Exception as e:
                print e
            else:
                break
        else:
            raise Exception('[Error] 3 failed attempts for connecting to {node}'.format(node=node))

    def put(self, key, bins):
        """
        :param key:
        :param bins:
        :return:
        """
        try:
            self._aerospike_client.put((self._namespace, self._set_name, key), bins, meta={'ttl': self._ttl})
            return True
        except Exception as e:
            print e
            return False

    def get(self, key):
        """
        :param key:
        :return:
        """
        try:
            (key, meta, bins) = self._aerospike_client.get((self._namespace, self._set_name, key))
            return bins
        except Exception as e:
            print e
            return None

    def close(self):
        self._aerospike_client.close()


def print_progress_bar(total, done):
    percentile = float(done)/float(total)
    sys.stdout.write('\r[{0}{1}]\t{2}%'.format('#'*int(percentile*50), ' '*int((1-percentile)*50), int(percentile*100)))
