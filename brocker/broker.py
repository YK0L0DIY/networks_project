import socket
import select
import pickle
import sys
import logging
import yaml

HEADER = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class Broker:
    server_socket = None
    sockets_list = []  # escuta
    clients = {}  # é um dicionario com informação do tipo de cliente (admin ou public_client ou sensor)
    sensor_id = {}  # key é o socket e value é o id
    sensor_reading = {}  # id -> toda a info do sensor
    locations = {}  # vai ter as localizações  onde há sensores

    def __init__(self, broker_ip='0.0.0.0',
                 broker_port='9000',
                 n_connections='5'):

        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            self.server_socket.bind((broker_ip, int(broker_port)))
            self.server_socket.listen(int(n_connections))

            self.sockets_list.append(self.server_socket)

        except Exception as err:
            logger.error("Socket creation failed with error %s" % err)
            exit(1)

        logger.info(f'Successful created broker {self.server_socket.getsockname()}')

    @staticmethod
    def send_info(client_socket, data_type, data):
        try:
            msg = {'type': data_type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            client_socket.send(info)
        except Exception as err:
            logger.error(err)

    def publish_subscribe(self, client_socket, message):
        try:
            if message['local'] in self.locations:
                self.locations[message['local']]['sub_clients'].append(client_socket)
            else:
                self.send_info(client_socket, 'subMessage', {'status': 400, 'value': "Esse local não tem sensores."})
        except Exception as err:
            logger.error(err)

    def add_new_reading(self, client_socket, data):
        try:
            socket_id = self.sensor_id[client_socket]  # vamos buscar o id do sensor
            tipo_de_leitura = self.sensor_reading[socket_id]['type']  # vamos bucar o tipo de leituras do sensor
            local_da_leitura = self.sensor_reading[socket_id]['location']  # vamos buscar a localização
            lengh_arr = len(self.locations[local_da_leitura][tipo_de_leitura])

            if lengh_arr > 0 and (self.locations[local_da_leitura][tipo_de_leitura][lengh_arr - 1] != data):
                data_dict = {'local': local_da_leitura, 'newRead': data , 'type':tipo_de_leitura}
                for client in self.locations[local_da_leitura]['sub_clients']:
                    self.send_info(client, 'subMessage', {'status': 200, 'value': data_dict})

            self.sensor_reading[socket_id]['last_read'] = data
            self.locations[local_da_leitura][tipo_de_leitura].append(data)

        except Exception as err:
            logger.error(err)

        # fica adiconada a leitura no fim da lista(depois para irmos buscar esta leitura basta fazer [len(array)-1]
        # logger.info("A lista de leituras do tipo " + tipo_de_leitura + " tem agr " + str(
        #    self.locations[local_da_leitura][tipo_de_leitura]))

    def add_new_location(self, location):
        self.locations[location] = {}
        self.locations[location]['sub_clients'] = []

    def add_new_sensor(self, client_socket, data):
        socket_id = self.sensor_id[client_socket]  # vamos ver o id do sensor
        self.sensor_reading[socket_id] = {'version': 1, 'type': data['sensor_type'],
                                          'location': data['sensor_location']}

        # If location not exists
        if not data['sensor_location'] in self.locations:
            self.add_new_location(data['sensor_location'])

        # If location exists but the type not
        if not data['sensor_type'] in self.locations[data['sensor_location']]:
            self.locations[data['sensor_location']][data['sensor_type']] = []

        logger.info(self.sensor_reading)

    def get_last_reading(self, client_socket, data):
        logger.info("Required last read from sensor " + data['sensor'])

        if len(self.sensor_id) == 0:
            self.send_info(client_socket, 'response', {'status': 400, 'error': 'There are 0 sensors'})

        try:
            value = self.sensor_reading[data['sensor']]['last_read']

        except Exception as error:
            logger.error(error)
            value = None

        if value is None:
            self.send_info(client_socket, 'response', {'status': 400,
                                                       'error': 'Problem in the sensor name or data not found'})
        else:
            self.send_info(client_socket, 'response', {'status': 200, 'value': value})

        return

    def get_all_sensors(self, client_socket):
        logger.info('Listing all sensors')
        list_of_sensors = []

        for x in self.sensor_id:
            sensor_id = self.sensor_id[x]
            sensor_type = self.sensor_reading[sensor_id]['type']
            local = self.sensor_reading[sensor_id]['location']
            version = self.sensor_reading[sensor_id]['version']
            sensor = f"{sensor_id} type: {sensor_type} location: {local} version: {version}"
            list_of_sensors.append(sensor)

        if not list_of_sensors:
            self.send_info(client_socket, 'response', {'status': 400, 'error': 'There are 0 sensors'})
        else:
            self.send_info(client_socket, 'response', {'status': 200, 'sensors': list_of_sensors})

        return

    def send_update(self, sensor_socket, data):
        self.send_info(sensor_socket, 'update', {'file_name': data['file_name'],
                                                 'content': data['content'],
                                                 'version': data['version']})

    def update(self, client_socket, data):
        sensor_type = data['sensor_type']
        logger.info(f"Updating sensors of type {data['sensor_type']}")

        try:
            for socket_id in self.sensor_id:
                sensor_id = self.sensor_id[socket_id]

                if self.sensor_reading[sensor_id]['type'] == sensor_type and \
                        int(self.sensor_reading[sensor_id]['version']) < int(data['version']):
                    self.send_update(socket_id, data)
                    self.sensor_reading[sensor_id]['version'] = data['version']

            self.send_info(client_socket, 'response', {'status': 200})

        except Exception as error:
            logger.error(error)
            self.send_info(client_socket, 'response', {'status': 400,
                                                       'error': f'Problem updating the sensor of type {sensor_type}'})
        return

    def kill_sensor(self, client_socket):
        self.send_info(client_socket, 'kill', {})

    def kill_sensors(self, client_socket, data):
        killed = []
        for x in data['sensors']:
            if x:
                for socket_id in self.sensor_id:
                    if self.sensor_id[socket_id] == x:
                        self.kill_sensor(socket_id)
                        killed.append(x)

        if len(killed) == len(data['sensors']):
            logger.info('Killed all sensor pretended')
            self.send_info(client_socket, 'response', {'status': 200})
        else:
            self.send_info(client_socket, 'response', {'status': 400,
                                                       'error': 'Had some error killing sensors verify your input'})
        return

    def list_locals(self, client_socket, message):
        locais_a_enviar = []
        for local in self.locations:
            if message['data']['poluente'] in self.locations[local]:
                locais_a_enviar.append(local)

        if len(locais_a_enviar) > 0:
            self.send_info(client_socket, 'lista_locais', {'status': 200, 'value': locais_a_enviar})

        else:
            self.send_info(client_socket, 'lista_locais', {'status': 400,
                                                           'value': "Não existem locais com esse tipo de poluente."})

    def local_read(self, client_socket, message):
        last_readings = {}
        try:
            if message['data']['local'] in self.locations:

                for poluente in self.locations[message['data']['local']]:
                    if poluente != 'sub_clients':
                        size_array = len(self.locations[message['data']['local']][poluente])
                        last_readings[poluente] = self.locations[message['data']['local']][poluente][size_array - 1]

            if len(last_readings) > 0:
                self.send_info(client_socket, 'leituras_local', {'status': 200, 'value': last_readings})

            else:
                self.send_info(client_socket, 'leituras_local', {'status': 400,
                                                                 'value': "Não existem medições de poluentes em " +
                                                                          message['data']['local']})
        except Exception as err:
            logger.error(err)

    def decode_message(self, client_socket, message):
        if message['type'] == 'sensor_reading':
            self.add_new_reading(client_socket, message['data']['leitura'])

        elif message['type'] == 'listar_locais':
            self.list_locals(client_socket, message)

        elif message['type'] == 'leituras_local':
            self.local_read(client_socket, message)

        elif message['type'] == 'get_last_reading':
            self.get_last_reading(client_socket, message['data'])

        elif message['type'] == 'sub':
            self.publish_subscribe(client_socket, message['data'])

        elif message['type'] == 'get_all_sensors':
            self.get_all_sensors(client_socket)

        elif message['type'] == 'update':
            self.update(client_socket, message['data'])

        elif message['type'] == 'kill_sensors':
            self.kill_sensors(client_socket, message['data'])
        elif message['type'] == 'kill_sensors':
            self.kill_sensors(client_socket, message['data'])

    def receive_message(self, client_socket, new_user=False):
        try:
            message_header = client_socket.recv(HEADER)

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

            self.decode_message(client_socket, message)

        except Exception as disconnect:
            logger.error(disconnect)
            try:
                logger.info("Closed Connection from user = {} ".format(self.clients[client_socket]))
            except Exception as not_client:
                logger.error("Closed Connection from a sensor %s" % not_client)
                logger.info("Closed Connection from user = {} ".format(self.sensor_id[client_socket]))

            # remover da lista de sockets e da lista de clients
            self.sockets_list.remove(client_socket)

            if client_socket in self.clients:
                del self.clients[client_socket]
            else:
                del self.sensor_id[client_socket]

            client_socket.close()
            return

    def run_broker(self):
        while True:
            read_sockets, _, exception_sockets = select.select(self.sockets_list, [],
                                                               self.sockets_list)

            for notified_socket in read_sockets:
                if notified_socket == self.server_socket:

                    client_socket, client_address = self.server_socket.accept()

                    user = self.receive_message(client_socket, new_user=True)

                    self.sockets_list.append(client_socket)

                    logger.info("New Connection from {}, user = {} ".format(client_address, user))

                else:  # simple msg
                    self.receive_message(notified_socket)

            for x in exception_sockets:
                logger.error(x)


if __name__ == "__main__":
    try:
        broker = Broker(broker_ip=sys.argv[1],
                        broker_port=sys.argv[2],
                        n_connections=sys.argv[3])

    except Exception as no_args:
        logger.error("No input, reading from file %s" % no_args)

        with open('config.yaml') as conf:
            configs = yaml.load(conf, Loader=yaml.FullLoader)
            broker = Broker(broker_ip=configs['broker_ip'],
                            broker_port=configs['broker_port'],
                            n_connections=configs['n_connections'])

    broker.run_broker()
