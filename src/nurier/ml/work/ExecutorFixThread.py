from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


class ExecutorFixThread:

    executor: ThreadPoolExecutor
    fix_thread: int
    waiting: float

    def __init__(self, fix_thread: int, thread_name: str):
        self.executor = ThreadPoolExecutor(max_workers=fix_thread, thread_name_prefix=thread_name)
        self.fix_thread = fix_thread
        self.waiting = 0.0

    def process(self, *args):
        self.set_waiting_count(True)
        self.executor.submit(self.work, *args)
        self.set_waiting_count(False)

    def set_waiting_count(self, plus: bool):
        if plus:
            self.waiting += 1.0
        else:
            self.waiting -= 1.0

    def get_waiting_count(self):
        return self.waiting

    def log_waiting_count(self):
        return f" ({self.waiting}) "

    def work(self, *args):
        pass
