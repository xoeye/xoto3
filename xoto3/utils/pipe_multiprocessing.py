"""A Process Pool implementation that works without shared memory."""

import typing as ty
from math import ceil

from multiprocessing import Process, Pipe
from queue import Queue, Empty
from threading import Thread
from uuid import uuid4
from logging import getLogger

from .iter import grouper_it


logger = getLogger(__name__)

DEFAULT_CHUNKSIZE_CAP = 10000


# things that go through pipes


class ProcessPoolFunction:
    def __init__(self, func, execution_id: str):
        self.func = func
        self.execution_id = execution_id

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class ProcessPoolFunctionInputChunk:
    def __init__(self, items_chunk: list, execution_id: str):
        self.func_inputs = items_chunk
        self.execution_id = execution_id


class ProcessPoolFunctionResult:
    def __init__(self, output_chunk: list, execution_id: str):
        self.output_chunk = output_chunk
        self.execution_id = execution_id


class Die:
    pass


# Core implementation


class ChunkQueueIterable(ty.Iterable):
    """Acts as an iterable over a queue."""

    def __init__(self):
        self.queue: Queue = Queue()
        self.chunks_received = 0
        self.chunks_sent = -1  # we don't need this number up front
        self.done = False

    def __iter__(self):
        while self.chunks_sent < 0 or self.chunks_received < self.chunks_sent:
            try:
                chunk = self.queue.get(timeout=1.0)
                self.chunks_received += 1
                for item in chunk:
                    yield item
            except Empty:
                pass
        self.done = True

    def give_chunk(self, chunk):
        self.queue.put(chunk)


def run_process_on_conn(conn, i):
    """The actual process that does your dirty work for you."""
    # logger.debug('Started running process %d', i)
    runnables = dict()
    while True:
        # logger.debug('Process %d waiting on next item across pipe', i)
        pipe_item = conn.recv()
        if isinstance(pipe_item, Die):
            break
        if isinstance(pipe_item, ProcessPoolFunction):
            # we've been asked to do something new! fun!
            # logger.debug(f'Adding runnable {pipe_item}')
            runnables[pipe_item.execution_id] = pipe_item.func
        else:
            # otherwise, this is a chunk of inputs for the runnable
            assert isinstance(pipe_item, ProcessPoolFunctionInputChunk)
            # logger.debug('Process %d <---- input chunk of size %d for id %s',
            #              i, len(pipe_item.func_inputs), pipe_item.execution_id)
            func = runnables[pipe_item.execution_id]
            output = [func(inp) for inp in pipe_item.func_inputs]
            # logger.debug('Process %d --->>> chunk for id %s', i, pipe_item.execution_id)
            conn.send(ProcessPoolFunctionResult(output, pipe_item.execution_id))
            # logger.debug('Process %d sent output chunk for id %s', i, pipe_item.execution_id)

    conn.send(Die())  # send this to the corresponding receiver thread
    logger.debug("Process %d received sentinel value and is exiting", i)


class PipedProcessPool:
    """A Process Pool for environments (like AWS Lambdas) that don't offer
    shared memory.

    This is a fairly naive reimplementation of Pool using Pipes
    instead of a multiprocessing Queue, because Pipes don't require
    shared memory (specifically, semaphores).

    Because it's built on top of Pipe, there is no easy way to
    implement backpressure, so there is no memory conservation here -
    your input and your output must both fit in memory simultaneously.

    This has been verified to be nearly as performant as the built-in
    Pool.

    """

    def __init__(self, size=10):
        """When the pool is created, we non-lazily spin up the processes
        and the threads that read their output from their pipes.
        """
        logger.debug("Beginning to create process pool of size %d", size)
        self.size = size
        pipes = [Pipe() for i in range(size)]
        self.child_conns = [pipe[1] for pipe in pipes]
        self.parent_conns = [pipe[0] for pipe in pipes]
        self.processes = [
            Process(target=run_process_on_conn, args=(self.child_conns[i], i), daemon=True)
            for i in range(size)
        ]
        for proc in self.processes:
            proc.start()
        self.next_proc = 0  # we round-robin the input chunks to processes

        # one result thread per process
        self.result_threads = [
            Thread(target=self.receive_result_chunks, args=(i,)) for i in range(size)
        ]
        for thread in self.result_threads:
            thread.daemon = True
            thread.start()
        self.chunk_queues: ty.Dict[str, ChunkQueueIterable] = dict()
        self.closed = size == 0
        logger.debug("Created process pool of size %d", size)

    def __del__(self):
        for proc in self.processes:
            proc.terminate()

    def receive_result_chunks(self, process_num: int):
        """A Thread that reads from a given pipe"""
        process_result_output_pipe = self.parent_conns[process_num]
        while True:
            result = process_result_output_pipe.recv()
            if isinstance(result, Die):
                break
            # logger.debug(f'Thread {process_num} <<<--- for exec id {result.execution_id}')
            self.chunk_queues[result.execution_id].give_chunk(result.output_chunk)
        # logger.debug('Exiting thread on Die sentinel value because '
        #              f'process {process_num} will never send anything else')

    def close(self):
        self.closed = True
        logger.info("Closing the pool")
        for pconn in self.parent_conns:
            pconn.send(Die())
        for proc in self.processes:
            proc.join()
        for thread in self.result_threads:
            thread.join()

    def __enter__(self):
        return self

    def ___exit__(self):
        # logger.debug('Called exit')
        self.close()

    def map(self, func, iterable, chunksize: int = 0) -> ty.Iterable:
        """Greedy implementation, in that it consumes your input iterable
        regardless of how far behind the process consumers might be getting.

        Your function and your iterable must both be pickleable.

        However, it does return a result iterable immediately, so you can start
        consuming results right away.

        Does not preserve ordering (for now), so don't rely on it.

        Sorry, maybe someday I'll make this cooler.
        """
        if self.closed:
            raise ValueError("Pool is closed")
        # clean up stale execution queues
        for eid in list(self.chunk_queues.keys()):
            if self.chunk_queues[eid].done:
                del self.chunk_queues[eid]

        execution_id = uuid4().hex
        self.chunk_queues[execution_id] = ChunkQueueIterable()
        # logger.info(f'Created a new execution id {execution_id}')

        def chunking_thread_func():
            """A thread that consumes the input iterable, chunks it, and
            round-robins it to the process pipes."""
            # prepare each process to execute the function associated with this ID
            for pipe in self.parent_conns:
                pipe.send(ProcessPoolFunction(func, execution_id))

            nonlocal chunksize
            if not chunksize:
                chunksize = ceil(len(list(iterable)) / self.size)
                if chunksize > DEFAULT_CHUNKSIZE_CAP:
                    chunksize = DEFAULT_CHUNKSIZE_CAP

            chunks_sent = 0
            # send the chunks
            for chunk in grouper_it(chunksize, iterable):
                chunk = list(chunk)
                # logger.debug('map chunk %d ----> proc %d (size %d, id %s)',
                #              chunks_sent, self.next_proc, len(chunk), execution_id)
                input_chunk = ProcessPoolFunctionInputChunk(chunk, execution_id)
                self.parent_conns[self.next_proc].send(input_chunk)

                self.next_proc += 1  # round-robining!
                if self.next_proc == len(self.processes):
                    self.next_proc = 0
                chunks_sent += 1

            # tell the receiving queue how to know when to stop
            self.chunk_queues[execution_id].chunks_sent = chunks_sent

        chunking_thread = Thread(target=chunking_thread_func)
        chunking_thread.daemon = True
        chunking_thread.start()

        # return an iterable that will not end until all results have been received
        return self.chunk_queues[execution_id]
