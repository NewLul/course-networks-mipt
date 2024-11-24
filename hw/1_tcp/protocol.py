import socket
from math import ceil
import struct
from threading import Thread
from time import sleep

class MyTCPProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.next_id = 0

        self.id_bytes = 4
        self.max_id = 2 ** (self.id_bytes * 8)

        self.maximum_segment_size = 2 ** 13

        self.package_size = (1 + self.id_bytes + self.maximum_segment_size)

        self.window_locked = False
        self.window_size = 2**10
        self.window = {}
        self.window_it = 0

        self.receive = {} # id -> [package_type, already_read, data]
        self.receive_it = 0

        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

        self.udp_socket.setblocking(False)
        self.end = False

        self.sender_thread = Thread(target=self.window_send_loop)
        self.receiver_thread = Thread(target=self.handle_loop)
        self.sender_thread.start()
        self.receiver_thread.start()

    def send_low(self, data: bytes):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recv_low(self, n: int):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def generate_package(self, data: bytes):
        # package_type -> 1, 0, 2, 3
        # id 
        # data ...

        for i in range(ceil(len(data) / self.maximum_segment_size)):
            package_type = 0
            if i == 0:
                package_type = 1
            elif i + 1 == ceil(len(data) / self.maximum_segment_size):
                package_type = 2
            id = self.next_id
            self.next_id = (self.next_id + 1) % self.max_id
            result = struct.pack('!BI', package_type, id)
            result += data[
                i * self.maximum_segment_size : 
                min(len(data), (i + 1) * self.maximum_segment_size)
            ]
            yield id, result

    def generate_return_package(self, id: int) -> bytes:
        result = (3).to_bytes(1)
        result += id.to_bytes(self.id_bytes)
        return result

    def read_package(self, data: bytes) -> tuple[int, int, bytes]:
        package_type, id = struct.unpack('!BI', data[:1+self.id_bytes])
        return (
            package_type,
            id,
            data[1 + self.id_bytes: ] if package_type != 3 else None,
        )

    def handle(self, package_type, id, data):
        if package_type == 3:
            while self.window_locked:
                sleep(0.00001)
            self.window_locked = True
            self.window.pop(id, None)
            if len(self.window) == 0:
                self.window_it = self.next_id
            else:
                while self.window_it not in self.window:
                    self.window_it += 1

            self.window_locked = False
        else:
            self.receive[id] = [package_type, 0, data]
            self.send_low(self.generate_return_package(id))

    def window_send_loop(self):
        while not self.end:
            sleep(0.00001)
            if self.window_locked:
                continue
            self.window_locked = True
            for _, data in reversed(self.window.items()):
                if data[1] == 0:
                    self.send_low(data[0])
                    data[1] = 1000
                else:
                    data[1] -= 1
            self.window_locked = False

    def handle_loop(self):
        while not self.end:
            try:
                package_type, id, data = self.read_package(
                    self.recv_low(self.package_size)
                )
                self.handle(package_type, id, data)
            except:
                sleep(0.00001)

    def send(self, data: bytes):
        for id, package in self.generate_package(data):
            while (id - self.window_it + self.max_id) % self.max_id > self.window_size:
                sleep(0.00001)
            while self.window_locked == True:
                sleep(0.00001)
            self.window_locked = True
            self.send_low(package)
            self.window[id] = [package, 1000]
            self.window_locked = False
        return len(data)

    def recv(self, n: int):
        result = bytes()
        while len(result) < n:
            cur = self.receive_it
            while cur not in self.receive:
                sleep(0.00001)
            if len(result) + len(self.receive[cur][2]) - self.receive[cur][1] <= n:
                used = len(self.receive[cur][2]) - self.receive[cur][1]
            else:
                used = n - len(result)
            result += self.receive[cur][2][
                self.receive[cur][1] : self.receive[cur][1] + used
            ]
            self.receive[cur][1] += used
            if self.receive[cur][1] == len(self.receive[cur][2]):
                self.receive_it = (self.receive_it + 1) % self.max_id
        return result

    def close(self):
        self.end = True

        self.sender_thread.join()
        self.receiver_thread.join()

        self.udp_socket.close()
