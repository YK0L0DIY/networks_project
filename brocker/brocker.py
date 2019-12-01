import socket
import select
import pickle
import sys
import logging

HEADERSIZE = 10

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
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Para podermos usar sempre a mesma porta sem ter de esperar pelo garbage collector do SO
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.server_socket.bind((brocker_ip, int(brocker_port)))
            self.server_socket.listen(int(n_conects))

            self.sockets_list.append(self.server_socket)

        except Exception as err:
            logger.error("Socket creation failed with error %s" % err)
            exit(1)

        logger.info(f'Successful created brocker {self.server_socket.getsockname()}')

    def send_info(self, client_socket, type, data):
        try:
            msg = {'type': type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

            client_socket.send(info)
        except Exception as err:
            logger.error(err)

    def add_new_reading(self, client_socket, data):
        socket_id = self.sensor_id[client_socket]  # vamos buscar o id do sensor
        tipoDeLeitura = self.sensor_reading[socket_id]['type']  # vamos bucar o tipo de leituras do sensor
        localDaLeitura = self.sensor_reading[socket_id]['location']  # vamos buscar a localização
        self.sensor_reading[socket_id]['last_read'] = data
        self.locations[localDaLeitura][tipoDeLeitura].append(data)
        # fica adiconada a leitura no fim da lista(depois para irmos buscar esta leitura basta fazer [len(array)-1]
        # logger.info("A lista de leituras do tipo " + tipoDeLeitura + " tem agr " + str(
        #    self.locations[localDaLeitura][tipoDeLeitura]))

    def add_new_locatio(self, location):
        self.locations[location] = {}

    def add_new_sensor(self, client_socket, data):
        socket_id = self.sensor_id[client_socket]  # vamos ver o id do sensor
        self.sensor_reading[socket_id] = {'version': 1, 'type': data['sensor_type'],
                                          'location': data['sensor_location']}
        if not data[
                   'sensor_location'] in self.locations:  # Se a localizção ainda nao tiver leituras  adicionamos essa localização para depois podermos adicionar leituras lá
            self.add_new_locatio(data['sensor_location'])

        if not data['sensor_type'] in self.locations[data[
            'sensor_location']]:  # se ja existir essa localização mas não existir um array com esse tipo de leituras criamos um novo array
            self.locations[data['sensor_location']][data['sensor_type']] = []

        logger.info(self.sensor_reading)

    def get_last_reading(self, client_socket, data):
        logger.info("Requered last read from sensor " + data['sensor'])

        if len(self.sensor_id) == 0:
            self.send_info(client_socket, 'response',
                           {'status': 400, 'error': 'There are 0 sensors'})

        try:
            value = self.sensor_reading[data['sensor']]['last_read']
        except:
            value = None

        if value == None:
            self.send_info(client_socket, 'response',
                           {'status': 400, 'error': 'Problem in the sensor name or data not found'})
        else:
            self.send_info(client_socket, 'response', {'status': 200, 'value': value})
        return

    def get_all_sensors(self, client_socket, data):
        logger.info('Listing all sensors')
        list_of_sensors = []
        for x in self.sensor_id:
            id = self.sensor_id[x]
            type = self.sensor_reading[id]['type']
            local = self.sensor_reading[id]['location']
            version = self.sensor_reading[id]['version']
            sensor = f"{id} type: {type} location: {local} version: {version}"
            list_of_sensors.append(sensor)

        if not list_of_sensors:
            self.send_info(client_socket, 'response', {'status': 400, 'error': 'There are 0 sensors'})
        else:
            self.send_info(client_socket, 'response', {'status': 200, 'sensors': list_of_sensors})
        return

    def send_update(self, sensor_socket, data):
        self.send_info(sensor_socket, 'update',
                       {'file_name': data['file_name'], 'content': data['content'], 'version': data['version']})

    def update(self, client_socket, data):
        sensor_type = data['sensor_type']
        logger.info(f"Updating sensors of type {data['sensor_type']}")
        try:
            for socket in self.sensor_id:
                id = self.sensor_id[socket]
                if self.sensor_reading[id]['type'] == sensor_type and \
                        int(self.sensor_reading[id]['version']) < int(data['version']):
                    self.send_update(socket, data)
                    self.sensor_reading[id]['version'] = data['version']

            self.send_info(client_socket, 'response', {'status': 200})

        except:
            self.send_info(client_socket, 'response',
                           {'status': 400, 'error': f'Problem updating the sensor of type {sensor_type}'})
        return

    def kill_sensor(self, client_socket):
        self.send_info(client_socket, 'kill', {})

    def kill_sensors(self, client_socket, data):

        killed = []
        for x in data['sensors']:
            if x:
                for socket in self.sensor_id:
                    if self.sensor_id[socket] == x:
                        self.kill_sensor(socket)
                        killed.append(x)

        if len(killed) == len(data['sensors']):
            logger.info('Killed all sensor pretended')
            self.send_info(client_socket, 'response', {'status': 200})
        else:
            self.send_info(client_socket, 'response',
                           {'status': 400, 'error': 'Had some error killing sensors verify your input'})
        return

    def decode_message(self, client_socket, message):
        if message['type'] == 'sensor_reading':  # se for uma leitura é guardar essa leitura no lugar certo
            self.add_new_reading(client_socket, message['data']['leitura'])

        elif message['type'] == 'listar_locais':
            # TODO muda para ser uma funcao
            locais_a_enviar = []
            for local in self.locations:
                if self.locations[local][message['data']['poluente']]:
                    locais_a_enviar.append(local)
            if len(locais_a_enviar) > 0:
                self.send_info(client_socket, 'Lista_Locais', locais_a_enviar)
            else:
                self.send_info(client_socket, 'Lista_Locais', "Não existem locais com esse tipo de poluente.")

        elif message['type'] == 'get_last_reading':
            self.get_last_reading(client_socket, message['data'])

        elif message['type'] == 'get_all_sensors':
            self.get_all_sensors(client_socket, message['data'])

        elif message['type'] == 'update':
            self.update(client_socket, message['data'])

        elif message['type'] == 'kill_sensors':
            self.kill_sensors(client_socket, message['data'])

    def receive_message(self, client_socket, new_user=False):
        try:
            message_header = client_socket.recv(HEADERSIZE)

            if not len(message_header):
                raise ValueError

            message_length = int(message_header.decode('utf-8').strip())

            message = pickle.loads(client_socket.recv(message_length))
            if new_user:
                if message['type'] == 'sensor_registry':
                    self.sensor_id[client_socket] = message['data']['id']
                    self.add_new_sensor(client_socket, message['data'])
                else:
                    self.clients[client_socket] = message['data']['id']

                return message['data']['id']

            # logger.info(f'reading info from {self.sensor_id[client_socket]}' + str(message['data']))

            self.decode_message(client_socket, message)

        except:
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
