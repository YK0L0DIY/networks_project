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
import logging

HEADER_LENGTH = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class Brocker:
    server_socket = None
    sockets_list = []  # escuta
    clients = {}  # é um dicionario com informação do tipo de cliente (admin ou public_client ou sensor)
    sensor_id = {}  # key é o socket e value é o id
    sensor_reading = {}  # id -> toda a info do sensor
    locations = {}  # vai ter as localizações  onde há sensores

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
            logger.error("socket creation failed with error %s" % err)
            exit(1)

        logger.info(f'Successful created brocker {self.server_socket.getsockname()}')

    def decode_mesage(user, message):  # podia ser a função que ia ver o que queriamos fazer com a mensagem
        if (user['data'].decode('utf-8') == "admin" and message.lower()[
                                                        :11] == "kill sensor"):  # partindo do principio que uma mensagem de kill sensor seria do tipo kill sensor 45
            # temos de ver qual a forma pela qual vamos optar para desligar um sensor ( necessitariamos de saber o socket dele.
            return False
        return False

    def send_info(self, client_socket, type, data):
        msg = {'type': type, 'data': data}
        msg = pickle.dumps(msg)
        info = bytes(f"{len(msg):<{HEADER_LENGTH}}", 'utf-8') + msg

        client_socket.send(info)

    # TODO create function to pickl and send to clients
    def kill(self, client_socket):
        self.send_info(client_socket, 'kill', {})

    # nfuncao sera do client admin e o brocker tera um recend ou retransmit
    def send_file(self, client_socket, file_name):
        with open(file_name, 'r', encoding='utf-8') as file:
            data = {'version': 1, 'file_name': file_name, 'content': file.read()}
            self.send_info(client_socket, 'update', data)

    def add_new_reading(self, client_socket, data):
        socket_id = self.sensor_id[client_socket]  # vamos buscar o id do sensor
        tipoDeLeitura = self.sensor_reading[socket_id]['type']  # vamos bucar o tipo de leituras do sensor
        localDaLeitura = self.sensor_reading[socket_id]['location']  # vamos buscar a localização
        self.locations[localDaLeitura][tipoDeLeitura].append(data)
        # fica adiconada a leitura no fim da lista(depois para irmos buscar esta leitura basta fazer [len(array)-1]
        logger.info("A lista de leituras do tipo " + tipoDeLeitura + " tem agr " +
                    str(self.locations[localDaLeitura][tipoDeLeitura]))

    def add_new_locatio(self, location):
        self.locations[location] = {}  # adicionar uma localizão como key

    def add_new_sensor(self, client_socket,
                       data):  # aqui basciamente estamos a adicionar um novo sensor á nossa base de dados de sensores
        socket_id = self.sensor_id[client_socket]  # vamos ver o id do sensor
        self.sensor_reading[socket_id] = {'leituras': None, 'version': 1, 'type': data['sensor_type'],
                                          'location': data[
                                              'sensor_location']}  # prenchemos já o que sabemos desse sensor quando envia o registo
        if not data[
                   'sensor_location'] in self.locations:  # Se a localizção ainda nao tiver leituras  adicionamos essa localização para depois podermos adicionar leituras lá
            self.add_new_locatio(data['sensor_location'])

        if not data['sensor_type'] in self.locations[data[
            'sensor_location']]:  # se ja existir essa localização mas não existir um array com esse tipo de leituras criamos um novo array
            self.locations[data['sensor_location']][data['sensor_type']] = []

        # TODO verificar se a localizacao ja existe , se nao exitir adiconar, se já vereficar se a lista de leituras
        #  daquele tipo ja exite,se ja nao se faz nada se nao adiciona-se essa lista de leitruas a essa localizacao
        logger.info(self.sensor_reading)

    def receive_message(self, client_socket, new_user=False):
        try:
            message_header = client_socket.recv(HEADER_LENGTH)

            if not len(message_header):
                raise ValueError

            message_length = int(message_header.decode('utf-8').strip())

            dict = pickle.loads(client_socket.recv(message_length))

            if new_user:
                if dict['type'] == 'sensor_registry':
                    self.sensor_id[client_socket] = dict['data']['id']
                    self.add_new_sensor(client_socket, dict['data'])
                else:
                    self.clients[client_socket] = dict['data']['id']

                return dict['data']['id']

            logger.info(f'reading info from {self.sensor_id[client_socket]}', dict['data'])
            # dict =  {'type': 'sensor_reading', 'data': {'leitura': x}}
            # agora queremos adicionar as leituras aos arrays respetivos
            if dict['type'] == 'sensor_reading':  # se for uma leitura é guardar essa leitura no lugar certo
                self.add_new_reading(client_socket, dict['data']['leitura'])

        except Exception as err:
            try:
                logger.info("Closed Connection from user = {} ".format(self.clients[client_socket]))
            except:
                logger.info("Closed Connection from user = {} ".format(self.sensor_id[client_socket]))

            # remover da lista de sockets e da lista de clients
            self.sockets_list.remove(client_socket)

            if client_socket in self.clients:
                del self.clients[client_socket]
            else:
                del self.sensor_id[client_socket]

            client_socket.close()
            return

    def run_brocker(self):
        while True:
            read_sockets, _, exception_sockets = select.select(self.sockets_list, [],
                                                               self.sockets_list)  # Ficamos á espera que haja alguma coisa para ler dos clients

            for notified_socket in read_sockets:
                if notified_socket == self.server_socket:  # isto significa que alguem se conectou pela primeira vez

                    client_socket, client_address = self.server_socket.accept()

                    user = self.receive_message(client_socket, new_user=True)

                    self.sockets_list.append(client_socket)

                    logger.info("New Connection from {}, user = {} ".format(client_address, user))

                else:  # não é uma nova conexão verificamos a existencia de mensagens
                    self.receive_message(notified_socket)

            for x in exception_sockets:
                logger.error(x)


if __name__ == "__main__":

    try:
        #               borcker ip, brocker port, number of connections
        brocker = Brocker(sys.argv[1], sys.argv[2], sys.argv[3])
    except:
        brocker = Brocker()

    brocker.run_brocker()
