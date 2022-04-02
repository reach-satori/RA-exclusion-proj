import sys
from threading import Thread
from random import randint
from rpyc.utils.server import ThreadedServer
from rpyc import Service, connect
from time import perf_counter

class ProcessService(Service):
    def __init__(self, procid):
        self.procs = {}
        self.state = "DO-NOT-WANT"
        self.procid = procid
        self.lamp_clock = 0
        self.clock = perf_counter()  # this one is only for rough times, for timeouts

        self.wanted_timeout = (5, 5)  # default values
        self.held_timeout = (10, 10)
        self.current_timeout = randint(*self.wanted_timeout)

        print(f"instantiating {self.procid}")

    def request_cs(self):
        pass

    def release_cs(self):
        pass

    def tick(self):
        t = perf_counter() - self.clock
        if t > self.current_timeout():
            if self.state == "DO_NOT_WANT":
                self.request_cs()
            elif self.state == "HELD":
                self.release_cs()

    def on_connect(self, conn):
        self._conn = conn

    def exposed_init_fellow_procs(self, procs):
        self.procs = procs
        self.procs.pop(self.procid) # only other processes

    def exposed_connect_to(self, id, port):
        c = connect("localhost", port)
        print(f"connected to port {port} from proc {self.procid} to {id}")
        c.close()

    def __del__(self):
        print(f"destroying {self.procid}")


if __name__ == "__main__":
    num_procs = int(sys.argv[1])
    procs = dict()  # dict of id: server object
    for i in range(num_procs): # create the nodes
        service = ProcessService(i)
        proc = ThreadedServer(service, port=0)
        procs[i] = proc  # we assume localhost for everything

    proc_port_dict = {key: proc.port for key, proc in procs.items()}

    threads = []
    for proc in procs.values():
        t = Thread(target=proc.start)  # using rpyc for this actually makes no sense
        threads.append(t)
        t.start()

    for id, port in proc_port_dict.items():
        c = connect("localhost", port)
        # since we're using port 0 rather than specifying, we need to tell the processes
        # the ports for every other process
        c.root.init_fellow_procs(proc_port_dict)
        c.close()

    c = connect("localhost", proc_port_dict[2])
    c.root.connect_to(id, proc_port_dict[1])

    while procs:
        for server in procs.values():
            proc.service.tick()

        for thread in threads:
            thread.join(timeout=1)
