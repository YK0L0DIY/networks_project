import socket
import select

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(('0.0.0.0', 9000))

timeout = 5
while True:
    while True:
        ready = select.select([client_socket], [], [], timeout)

        if ready[0]:
            data = client_socket.recv(4096)
            print(data)
        else:
            break

    client_socket.send(b'10')
