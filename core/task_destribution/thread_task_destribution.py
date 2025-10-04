import os
import logging
import random
import threading
from functools import wraps
from queue import Queue, Empty
from datetime import datetime
from time import sleep as wait


class ThreadingBase:
    """
    Base class for multi-threaded scraping and result processing.
    """

    def __init__(self, th_num=10, logger=None):
        """
        Initialize threading environment.

        :param th_num: number of consumer threads (default 10)
        :param logger: logging.Logger instance
        """

        self.logger = logger

        self.logger.info(f'starting')
        # total number of threads
        self.th_num = th_num

        # progress counters
        self.f_counter = 0
        self.s_counter = 0
        self.t_counter = 0

        # queue for scraping distribution tasks
        self.tasks = Queue()

        # queue for saving scraping results in parallel
        self.results = Queue()

        # identifier that would stop all consumers if critical exception occurs
        self.is_stopped = False

        # semaphore object that would manage consumer
        self.block = threading.Semaphore(th_num)

        # size of saving batch
        self.save_result_limit = 100

        # start time for progress monitoring
        self.start_time = datetime.now()

    def exception(method):
        """
        Decorator for handling exceptions in methods.

        :param method: wrapped function
        :return: wrapper
        """

        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if method.__annotations__.get('return') is not None:
                if callable(method.__annotations__['return']):
                    result = method.__annotations__['return']()
                else:
                    result = method.__annotations__['return']
            else:
                result = None

            try:
                result = method(self, *args, **kwargs)
            except Exception as e:
                self.logger.exception(f'exception in "{method.__name__}" => {e}')
                result = e if result is None else result
            finally:
                return result

        return wrapper

    def progress_logger(method):
        """
        Decorator for logging task execution progress.

        :param method: wrapped function
        :return: wrapper
        """

        def wrapper(self, *args, **kwargs):
            try:
                result = method(self, *args, **kwargs)
                if result is False:
                    self.f_counter += 1
                elif result is None:
                    self.f_counter += 1
                elif result is True:
                    self.s_counter += 1
                return result
            except Exception as e:
                self.logger.exception(f'exception occurred during progress logging:: {e}')
                self.f_counter += 1
            finally:
                if self.t_counter > 0:
                    self.logger.info(f'success:: {self.s_counter} / failed:: {self.f_counter} / total:: {self.t_counter} progress:: {round((self.s_counter + self.f_counter) / self.t_counter * 100, 2)}% / exec time:: {str(datetime.now() - self.start_time)}')

        return wrapper

    def scraping_consumer(self, method):
        """
        Single thread worker
        :param method:
        :return:
        """
        while self.tasks.qsize() != 0 and not self.is_stopped:
            try:
                self.block.acquire()
                task = self.tasks.get(timeout=0.1)
                method(task)
            except Empty:
                self.logger.info(f'task queue is empty')
                break
            except Exception as e:
                self.logger.error(f'consumer exception occurred:: {e}')
                break
            finally:
                self.block.release()

        self.logger.info(f'consumer finished procession')

    def save_results(self, method, force_save=False):
        """
        Method performs batch saving in a separate thread
        :param method:
        :param force_save:
        :return:
        """
        to_save = list()

        if not force_save:
            while True:
                self.logger.info(f'results queue size:: {self.results.qsize()}')
                # check current result queue size
                if self.results.qsize() >= self.save_result_limit:
                    self.logger.info(f'start saving results')
                    for _ in range(self.save_result_limit):
                        if self.results.qsize() != 0:
                            res = self.results.get()
                            to_save.append(res)

                if to_save:
                    if method(to_save) is False:
                        self.logger.error(f'saving finished with errors')
                        self.is_stopped = True
                    else:
                        self.logger.info(f'saving completed')

                    to_save.clear()

                if self.tasks.qsize() == 0 and self.results.qsize() == 0:
                    self.logger.info(f'breaking saving loop')
                    break

                elif self.tasks.qsize() == 0 and self.results.qsize() != 0:
                    self.logger.info(f'tasks queue is empty:: processing last results')
                    for _ in range(self.save_result_limit):
                        if self.results.qsize() != 0:
                            res = self.results.get()
                            to_save.append(res)
                wait(1)
        else:
            # force_save=True
            # save all existed data inside self.results queue
            if self.results.qsize() != 0:
                self.logger.info(f'current results size:: {self.results.qsize()}')

                # extract all results
                while self.results.qsize() != 0:
                    r = self.results.get()
                    to_save.append(r)

                if method(to_save) is False:
                    self.logger.error(f'saving finished with errors')
                else:
                    self.logger.info(f'saving completed')
            else:
                self.logger.info(f'nothing to save')

    @progress_logger
    @exception
    def scraping_executor(self, url):
        pass

    @exception
    def saving_executor(self, results):
        pass

    @exception
    def execution_pipeline(self, input_data):
        """
        Main execution pipeline.

        :param input_data: list of tasks
        :return: True on success, False on failure
        """

        # reassign counters
        self.t_counter = len(input_data)
        self.s_counter = 0
        self.f_counter = 0
        self.is_stopped = False

        # create new tasks queue
        [self.tasks.put(data) for data in input_data]

        del input_data
        self.logger.info('properties ids tasks prepared')

        # initialize consumers
        pool = [threading.Thread(target=self.scraping_consumer, kwargs={"method": self.scraping_executor}) for _ in range(self.th_num)]

        # initialize additional consumer thread for saving results
        pool.append(threading.Thread(target=self.save_results, kwargs={'method': self.saving_executor, 'force_save': False}))

        self.logger.info('threads prepared')

        # launching threads
        for th in pool:
            th.start()

        # waiting till threads are finished
        for th in pool:
            th.join()

        if not self.results.empty():
            self.logger.info("Result queue is not empty... Starting last saving")

            to_save = []
            while not self.results.empty():
                to_save.append(self.results.get())

            self.saving_executor(to_save)
            self.logger.info("Save result is ended")

        if self.tasks.qsize() != 0 and self.is_stopped is True:
            self.logger.error(f'execution pipeline finished with errors')
            return False

        self.logger.info('pipeline procession completed')
        return True

    @exception
    def run(self, tasks):
        """
        Run execution pipeline with shuffled tasks.

        :param tasks: list of input tasks
        :return: True on success, False on failure
        """
        random.shuffle(tasks)

        if not self.execution_pipeline(tasks):
            self.logger.critical(f'pipeline execution failed')
            return False

        self.logger.info('pipeline execution completed')
        return True


if __name__ == '__main__':
    ex = ThreadingBase()
    ex.run()