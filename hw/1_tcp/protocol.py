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

        self.receive = {}
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
#        with open("data/test_info", "a") as f:
#            f.write(str(self) + " got package BEFORE READ " + str(data) + '\n')
        package_type, id = struct.unpack('!BI', data[:1+self.id_bytes])
#        with open("data/test_info", "a") as f:
#            f.write(str(self) + " got package AFTER READ " + str(package_type) + " " + str(id) + '\n')
        return (
            package_type,
            id,
            data[1 + self.id_bytes: ] if package_type != 3 else None,
        )

    def handle(self, package_type, id, data):
#        with open("data/test_info", "a") as f:
#                f.write(str(self) + " got package with " + str(package_type) + " " + str(id) + '\n')
        if package_type == 3:
            while self.window_locked:
#                with open("data/test_info", "a") as f:
#                    f.write(str(self) + " window is locked\n")
                sleep(0.00001)
            self.window_locked = True
#            with open("data/test_info", "a") as f:
#                    f.write(str(self) + " LOCKING WINDOW IN HANDLING PACKAGE\n")
#            with open("data/test_info", "a") as f:
#                    f.write(str(self) + " BEFORE POP\n")
            self.window.pop(id, None)
#            with open("data/test_info", "a") as f:
#                    f.write(str(self) + " AFTER POP\n")
            if len(self.window) == 0:
                self.window_it = self.next_id
            else:
                while self.window_it not in self.window:
                    self.window_it += 1

            self.window_locked = False
#            with open("data/test_info", "a") as f:
#                    f.write(str(self) + " UNLOCKING WINDOW IN HANDLING PACKAGE\n")
        else:
            self.receive[id] = [package_type, 0, data]
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " is returning after accepting " + str(package_type) + " " + str(id) + " " + str(self.receive) + '\n')
            self.send_low(self.generate_return_package(id))

    def window_send_loop(self):
        while not self.end:
            sleep(0.00001)
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " window is  " + str(self.window) + '\n')
            if self.window_locked:
                continue
            self.window_locked = True
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " LOCKING WINDOW IN SENDING LOOP\n")
            for _, data in reversed(self.window.items()):
#                with open("data/test_info", "a") as f:
#                    f.write(str(self) + " is sending  " + str(_) + " " + str(data[0]) + '\n')
                if data[1] == 0:
                    self.send_low(data[0])
                else:
                    data[1] -= 1
            self.window_locked = False
            
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " UNLOCKING WINDOW IN SENDING LOOP\n")

#        with open("data/test_info", "a") as f:
#            f.write("Sending is over for " + str(self) + '\n')

    def handle_loop(self):
        while not self.end:
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " is handling\n")
            try:
                package_type, id, data = self.read_package(
                    self.recv_low(self.package_size)
                )
                self.handle(package_type, id, data)
            except:
                sleep(0.00001)

#        with open("data/test_info", "a") as f:
#            f.write("Handling is over for " + str(self) + '\n')

    def send(self, data: bytes):
        for id, package in self.generate_package(data):
            while (id - self.window_it + self.max_id) % self.max_id > self.window_size:
                sleep(0.00001)
            while self.window_locked == True:
                sleep(0.00001)
            self.window_locked = True
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " LOCKING WINDOW IN SENDING NEW PACKAGE\n")
            self.send_low(package)
            self.window[id] = [package, 1000]
            self.window_locked = False
#            with open("data/test_info", "a") as f:
#                f.write(str(self) + " UNLOCKING WINDOW IN SENDING NEW PACKAGE\n")
        return len(data)

    def recv(self, n: int):
        result = bytes()
        while len(result) < n:
            cur = self.receive_it
            while cur not in self.receive:
                sleep(0.00001)
#            with open("data/test_info", "a") as f:
#                    f.write(str(self) + " is trying to receive " + str(n) + " " + str(len(result)) + " bytes; cur = " + str(cur) + " receive = " + str(self.receive) + '\n')
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
#        with open("data/test_info", "a") as f:
#            f.write(str(self) + " have received " + str(len(result)) + " bytes -> " + str(result) + '\n')
        return result

    def close(self):
        self.end = True

        self.sender_thread.join()
        self.receiver_thread.join()

#        with open("data/test_info", "a") as f:
#            f.write(str(self) + " THREADS ARE CLOSED\n")

        self.udp_socket.close()
