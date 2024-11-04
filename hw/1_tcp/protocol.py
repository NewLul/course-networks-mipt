import socket
from math import ceil
from threading import Thread
from time import sleep

class MyTCPProtocol:
    def send_low(self, data: bytes):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recv_low(self, n: int):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def generate_package(self, data: bytes):
        # = 258 bytes total
        # 1 byte      - type - [0 - not the first in the package, 1 - the first in the package, 2 - the last in the package, 3 - return]
        # 1 bytes    - id
        # 1 bytes    - data length
        # 255 bytes - data
        for i in range(ceil(len(data) / self.maximum_segment_size)):
            result = bytes()
            if i == 0:
                result += (1).to_bytes(1)
            elif i + 1 == ceil(len(data) / self.maximum_segment_size):
                result += (2).to_bytes(1)
            else:
                result += (0).to_bytes(1)
            result += self.next_id.to_bytes(1)
            id = self.next_id
            self.next_id = (self.next_id + 1) % self.max_id
            length = min(len(data), (i + 1) * self.maximum_segment_size) -  i * self.maximum_segment_size
            result += length.to_bytes(1)
            result += data[i * self.maximum_segment_size : min(len(data), (i + 1) * self.maximum_segment_size)]
            if length != 255:
                result += (0).to_bytes(255 - length)
            yield id, result

    def generate_return_package(self, id: int) -> bytes:
        result = (3).to_bytes(1)
        result += id.to_bytes(1)
        result += (0).to_bytes(256)
        return result

    def read_package(self, data: bytes) -> tuple[int, int, bytes]:
        package_type = int.from_bytes(data[:1])
        id = int.from_bytes(data[1:2])
        length = int.from_bytes(data[2:3])
        with open("data/test_info", "a") as f:
            f.write(str(self) + " got package with " + str(package_type) + " " + str(id) + " " + str(length) + '\n')
        return package_type, id, data[3:3 + length] if package_type != 2 else None

    def handle(self, package_type, id, data):
        if package_type == 3:
            while self.window_locked:
                sleep(0.1)
            self.window_locked = True
            self.window.pop(id)
            if len(self.window) == 0:
                self.window_it = self.next_id
            else:
                while self.window_it not in self.window:
                    self.window_it += 1
            self.window_locked = False
        else:
            self.receive[id] = [package_type, 0, data]
            with open("data/test_info", "a") as f:
                f.write(str(self) + " is returning after accepting " + str(package_type) + " " + str(id) + " " + str(data) + '\n')
            self.send_low(self.generate_return_package(id))

    def window_send_loop(self):
        while not self.end:
            sleep(0.1)
            if self.window_locked:
                continue
            self.window_locked = True
            for _, data in reversed(self.window.items()):
                with open("data/test_info", "a") as f:
                    f.write(str(self) + " is sending  " + str(_) + " " + str(data) + '\n')
                self.send_low(data)
            self.window_locked = False
        
        with open("data/test_info", "a") as f:
            f.write("Sending is over for " + str(self) + '\n')

    def handle_loop(self):
        while not self.end:
            with open("data/test_info", "a") as f:
                f.write(str(self) + "is handling + \n")
            try:
                sleep(0.1)
                package_type, id, data = self.read_package(self.recv_low(258))
                self.handle(package_type, id, data)
            except:
                continue
        with open("data/test_info", "a") as f:
            f.write("Handling is over for " + str(self) + '\n')

    def __init__(self, *, local_addr, remote_addr):
        self.next_id = 0
        self.max_id = 2**8 # id is in range [0, max_id)
        self.maximum_segment_size = 255

        self.window_locked = False
        self.window_size = 2**6
        self.window = {} # int -> bytes
        self.window_it = 0 # first element of window 
        
        self.receive = {} # int -> [int, int, bytes] package type, iterator, data
        self.receive_it = 0 #ferst element of receive

        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

        self.udp_socket.setblocking(False)
        self.end = False

        self.sender_thread = Thread(target=self.window_send_loop)
        self.receiver_thread = Thread(target=self.handle_loop)
        self.sender_thread.start()
        self.receiver_thread.start()

    def send(self, data: bytes):
        for id, package in self.generate_package(data):
            while (id - self.window_it + self.max_id) % self.max_id > self.window_size:
                sleep(0.2)
            while self.window_locked == True:
                sleep(0.05)
            self.window_locked = True
            self.send_low(package)
            self.window[id] = package
            self.window_locked = False
        return len(data)

    def recv(self, n: int):
        result = bytes()
        while len(result) < n:
            cur = self.receive_it
            while cur not in self.receive:
                sleep(0.2)
            if len(result) + len(self.receive[cur][2]) - self.receive[cur][1] <= n:
                used = len(self.receive[cur][2]) - self.receive[cur][1]
            else:
                used = n - len(result)
            result += self.receive[cur][2][self.receive[cur][1]:self.receive[cur][1] + used]
            self.receive[cur][1] += used
            if self.receive[cur][1] == len(self.receive[cur][2]):
                self.receive_it = (self.receive_it + 1) % self.max_id
        return result

    def close(self):
        self.end = True
        
        self.sender_thread.join()
        self.receiver_thread.join()
        
        self.udp_socket.close()

