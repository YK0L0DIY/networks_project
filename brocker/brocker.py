############################################################################################################################
#                                      ********SERVIDOR TCP COM SELECT**********
#
#  1 **    Para entender este codigo devemos partir do principio que o client sempre que enviar uma mensagem envia 10caracteres
#       iniciais (header) indicando o tamanho da mensagem sendo o header sempre um numero em modo string.
#
#  2 **    Devemos também entender como codigo para matar um sensor a mensagem do tipo "kill sensor <ID>"
#
#  3 **    Problemas ainda  discutir em grupo sobre como vamos enviar as informações dos sensores e como os vamos guardar, i.e.,
#       se vamos usar classe sensor dic de lista de atributos dos sensores etc
#
#
#  4 **    Neste momento o server falta decidir como vamos identificar os sensores e as suas propriadades para depois
#       podermos desliga-los e fazer "updates" de firmware
#
#  5 **    Falta ainda vermos como o servidor vai fazer o registo dos sensores (devemos tentar establecer isto primeiro
#       antes de sabermos como vamos solucionar o ponto 3
#
############################################################################################################################


import socket
import select
import pickle
import sys

HEADER_LENGTH = 10


class Brocker:
    server_socket = None
    sockets_list = []
    clients = {}  # é um dicionario com informação do tipo de cliente (admin ou public_client ou sensor)
    sensor_dic = {}  # key é o id e value é o socket

    def __init__(self, brocker_ip='0.0.0.0', brocker_port='9000', n_conects='5'):

        try:
            self.server_socket = socket.socket(socket.AF_INET,
                                               socket.SOCK_STREAM)  # Associamos o socket a um ipv4 e do tipo TCP
            # Para podermos usar sempre a mesma porta sem ter de esperar pelo garbage collector do SO
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.server_socket.bind((brocker_ip, int(brocker_port)))
            self.server_socket.listen(int(n_conects))

            self.sockets_list.append(self.server_socket)

        except Exception as err:
            print("socket creation failed with error %s" % err)
            exit(1)

        print(f'Successful created brocker {self.server_socket.getsockname()}')

    def decode_mesage(user, message):  # podia ser a função que ia ver o que queriamos fazer com a mensagem
        if (user['data'].decode('utf-8') == "admin" and message.lower()[
                                                        :11] == "kill sensor"):  # partindo do principio que uma mensagem de kill sensor seria do tipo kill sensor 45
            # temos de ver qual a forma pela qual vamos optar para desligar um sensor ( necessitariamos de saber o socket dele.
            return False
        return False

    def receive_message(self, client_socket):
        try:
            message_header = client_socket.recv(HEADER_LENGTH)

            if not len(message_header):
                return False

            message_length = int(message_header.decode('utf-8').strip())

            dict = pickle.loads(client_socket.recv(message_length))
            print(dict)
            try:
                return dict['sensor_id']
            except:
                print('any server id')

        except:
            print("Closed Connection from User = {} ".format(
                self.clients[client_socket]))

            # remover da lista de sockets e da lista de clients
            self.sockets_list.remove(client_socket)

            del self.clients[client_socket]
            return

    def run_brocker(self):
        while True:
            read_sockets, _, exception_sockets = select.select(self.sockets_list, [],
                                                               self.sockets_list)  # Ficamos á espera que haja alguma coisa para ler dos clients

            for notified_socket in read_sockets:
                if notified_socket == self.server_socket:  # isto significa que alguem se conectou pela primeira vez

                    client_socket, client_address = self.server_socket.accept()

                    user = self.receive_message(client_socket)
                    if user is False:  # se alguem se disconectar continuamos á escuta dos sockets
                        continue

                    self.sockets_list.append(client_socket)

                    self.clients[
                        client_socket] = user  # ficamos com a imformação que aquele socket representa um admin ou public_client / ou um sensor
                    print("New Connection from {}, UserType = {} ".format(client_address, user))

                else:  # não é uma nova conexão verificamos a existencia de mensagens
                    self.receive_message(notified_socket)


if __name__ == "__main__":
    print(sys.argv)
    #               borcker ip, brocker port, number of connections
    brocker = Brocker(sys.argv[1], sys.argv[2], sys.argv[3])
    brocker.run_brocker()
