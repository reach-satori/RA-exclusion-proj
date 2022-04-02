import sys
from threading import Thread

from rpyc.utils.server import ThreadedServer
from rpyc import Service, connect

class ProcessService(Service):
    def __init__(self, procid):
        self.procs = {}
        self.state = "DO-NOT-WANT"
        self.procid = procid
        self.clock = 0
        print(f"instantiating {self.procid}")

    def on_connect(self, conn):
        self._conn = conn

    def exposed_init_fellow_procs(self, procs):
        self.procs = procs

    def exposed_connect_to(self, id, port):
        c = connect("localhost", port)
        print(f"connected to port {port} from proc {self.procid} to {id}")
        c.close()

    def __del__(self):
        print(f"destroying {self.procid}")


if __name__ == "__main__":
    num_procs = int(sys.argv[1])
    procs = dict()  # dict of id: server object
    for i in range(num_procs):
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
        c.root.init_fellow_procs(proc_port_dict)
        c.close()

    c = connect("localhost", proc_port_dict[2])
    c.root.connect_to(id, proc_port_dict[1])

    while procs:
        for thread in threads:
            thread.join(timeout=1)
