# servidor concorrente sem o Select
# neste momento
import socket
import threading

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Associamos o socket a um ipv4 e do tipo TCP

sock.bind(("localhost", 9000))

sock.listen(5)  # o argumento é o numero de elementos em fila de espera, escolhi 5 porque penso que seja suficiente

global connections
connections = []


def handler(clientsocket, address):
    while True:
        data = clientsocket.recv(1024)  # vamos receber no maximo 1024Bytes de cada vez
        print("Data from ", address, data.decode("utf-8"))
        if not data:  # se nao houver nada para ler fechamos a ligação com o socket
            connections.remove(clientsocket)
            clientsocket.close()
            break


while True:
    clientsocket, address = sock.accept()
    print("New Connection from ", address)
    cThread = threading.Thread(target=handler, args=(clientsocket, address))
    cThread.daemon = True  # Esta linha de codigo permite fecharmos o programa mesmo que haja alguma thread ativa
    cThread.start()
    connections.append(clientsocket)
