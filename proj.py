import sys
from threading import Thread
from random import randint
from rpyc.utils.server import ThreadedServer
from rpyc import Service, connect
from time import perf_counter

class KeyboardThread(Thread):  # threaded input lifted from a stackexchange qst.

    def __init__(self, input_cbk = None, name='keyboard-input-thread'):
        self.input_cbk = input_cbk
        super(KeyboardThread, self).__init__(name=name)
        self.start()

    def run(self):
        while True:
            self.input_cbk(input())  # waits to get input + Return

def command_callback(command):
    print(command)

class ProcessService(Service):

    # this isn't actually used anywhere other than testing, for print string formatting
    statedict = {"DO-NOT-WANT": "WANTED", "WANTED": "HELD", "HELD": "DO-NOT-WANT"}

    def __init__(self, procid, verbose=False):
        self.verbose = verbose
        self.procs = {}  # dict of proc_ids : ports (doesn't include self)
        self.state = "DO-NOT-WANT"
        self.procid = procid
        self.lamp_clock = 0 # lamport
        self.clock = perf_counter()  # only for counting its own timeouts
        # list of procids: release_cs empties this list and sends deferred acks
        self.deferred_actions = []

        self.do_not_want_timeout = [5, 5]  # default values
        self.held_timeout = [10, 10]

        # for testing purposes
        # self.do_not_want_timeout = (randint(1,10), randint(10,30))  # default values
        # self.held_timeout = (randint(5,15), randint(15,35))

        self.current_timeout = randint(*self.do_not_want_timeout)
        self.acks = []

        if self.verbose:
            print(f"instantiating process {self.procid}")

    def request_cs(self):
        # run when a process decides it wants the CS. immediate acks are recorded
        for procid in self.procs:
            if self.send_request(procid):
                self.acks[procid] = True

    def release_cs(self):
        # run deferred responses
        while self.deferred_actions:
            next = self.deferred_actions.pop()
            self.send_ack(next)

    def send_ack(self, procid):
        connect("localhost", self.procs[procid]).root.receive_ack(self.procid)

    def statechange(self, newstate):
        if self.verbose:
            print(f"proc {self.procid} changing state to {newstate}")
        self.clock = perf_counter()
        self.lamp_clock += 1
        if newstate == "WANTED":
            self.state = "WANTED"
            self.request_cs()
            self.current_timeout = float("inf")

        elif newstate == "DO-NOT-WANT":
            self.release_cs()
            self.state = "DO-NOT-WANT"
            self.current_timeout = randint(*self.do_not_want_timeout)

        elif newstate == "HELD":
            self.state = "HELD"
            self.current_timeout = randint(*self.held_timeout)
            self.acks = {procid: False for procid in self.procs}

        else:
            raise ValueError("Malformed state")

    def tick(self):
        t = perf_counter() - self.clock
        if t > self.current_timeout:
            if self.verbose:
                print(f"timeout reached in proc {self.procid}")
            if self.state == "DO-NOT-WANT":
                self.statechange("WANTED")
            elif self.state == "HELD":
                self.statechange("DO-NOT-WANT")

        if self.state == "WANTED" and all(self.acks.values()):
            self.statechange("HELD")

    def send_request(self, procid):
        self.lamp_clock += 1
        c = connect("localhost", self.procs[procid])
        return c.root.receive_request(self.procid, self.lamp_clock + 1)

    def exposed_receive_ack(self, sending_proc):
        self.acks[sending_proc] = True

    def exposed_receive_request(self, sending_procid, timestamp):
        if self.state == "DO-NOT-WANT" or (self.state == "WANTED" and timestamp < self.lamp_clock):
            out = True
        else:
            out = False
            self.deferred_actions.append(sending_procid)

        self.lamp_clock = max(self.lamp_clock+1, timestamp)
        return out

    def exposed_init_fellow_procs(self, procs):
        self.procs = dict(procs)
        self.procs.pop(self.procid)  # only other processes
        self.acks = {procid: False for procid in self.procs}

    def __del__(self):
        print(f"destroying {self.procid}")


if __name__ == "__main__":
    num_procs = int(sys.argv[1])
    procs = dict()  # dict of id: server object
    for i in range(num_procs):  # create the nodes
        proc = ThreadedServer(ProcessService(i, verbose=False), port=0)
        procs[i] = proc  # we assume localhost for everything

    proc_port_dict = {key: proc.port for key, proc in procs.items()}
    threads = []  # so we have threads running threadedservers it's kind of a mess
    for proc in procs.values():
        t = Thread(target=proc.start)  # using rpyc for this actually makes so little sense
        threads.append(t)
        t.start()

    for pid, port in proc_port_dict.items():
        c = connect("localhost", port)
        # since we're using port 0 rather than specifying, we need to tell the processes
        # the ports for every other process
        # turn into tuples to pass because otherwise they're mutable and rpyc complains
        c.root.init_fellow_procs(tuple(proc_port_dict.items()))
        c.close()

    kthread = KeyboardThread(command_callback)
    tick_interval = 1  # in seconds

    while procs:
        for server in procs.values():
            server.service.tick()

        for thread in threads:
            thread.join(timeout=(tick_interval/len(procs)))
