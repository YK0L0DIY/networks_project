import socket
import select
import pickle
import sys
import logging
from datetime import datetime

import yaml

HEADER = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class Broker:
    server_socket = None
    sockets_list = []
    clients = {}
    sensor_id = {}
    sensor_reading = {}
    locations = {}  # all locations and data for specific location

    def __init__(self, broker_ip,
                 broker_port,
                 n_connections):

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

    def send_info(self, client_socket, data_type, data):
        """
        Send the pretended message for a specific socket.
        :param client_socket: Client socket
        :param data_type: Data type.
        :param data: Data to send.
        """
        try:
            msg = {'type': data_type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            client_socket.send(info)
        except Exception as err:
            if data_type == 'subMessage':
                local = data['value']['local']
                del self.locations[local]['sub_clients'][client_socket]
            logger.error(err)

    def publish_subscribe(self, client_socket, message):
        """
        Function to subscribe to a city by a client
        :param client_socket: Client socket that want the subscription
        :param message: Dict with the local of the subscription
        """
        try:
            if message['local'] in self.locations:
                self.locations[message['local']]['sub_clients'].append(client_socket)
            else:
                self.send_info(client_socket, 'subMessage', {'status': 400, 'value': "This local don't have sensors."})
        except Exception as err:
            logger.error(err)

    def add_new_reading(self, client_socket, data):
        """
        Function to add new reading to by a sensor socket
        :param client_socket: Sensor socket that r«has the reading
        :param data: Dict with the information
        """
        try:
            socket_id = self.sensor_id[client_socket]
            sensor_type = self.sensor_reading[socket_id]['type']
            reading_local = self.sensor_reading[socket_id]['location']
            length_arr = len(self.locations[reading_local][sensor_type])

            if length_arr > 0 and (self.locations[reading_local][sensor_type][length_arr - 1] != data):
                data_dict = {'local': reading_local, 'newRead': data, 'type': sensor_type}
                for client in self.locations[reading_local]['sub_clients']:
                    self.send_info(client, 'subMessage', {'status': 200, 'value': data_dict})

            self.sensor_reading[socket_id]['last_read'] = data
            self.locations[reading_local][sensor_type].append(
                {"sensor_id": socket_id,
                 "value": data,
                 "date": datetime.now().strftime('%Y-%m-%d'),
                 "hour": datetime.now().strftime('%H:%M')})

        except Exception as err:
            logger.error(err)

    def add_new_location(self, location):
        """
        Creates a new location
        :param location: Location to  be add
        """
        self.locations[location] = {}
        self.locations[location]['sub_clients'] = []

    def add_new_sensor(self, client_socket, data):
        """
        Add new sensor
        :param client_socket: Sensor socket
        :param data: Dict with the parameters
        """
        socket_id = self.sensor_id[client_socket]
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
        """
        Fucntion to get last reading by sensor
        :param client_socket: Sensor socket
        :param data: Dict with parameters
        :return:
        """
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
        """
        List of all sensors
        :param client_socket: Socket of the client that requests the list
        """
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

    def send_update(self, sensor_socket, data):
        """
        Send update to a specific sensor by socket
        :param sensor_socket: Sensor socket
        :param data: Dict with data ( file_name, content, version)
        """
        self.send_info(sensor_socket, 'update', {'file_name': data['file_name'],
                                                 'content': data['content'],
                                                 'version': data['version']})

    def update(self, client_socket, data):
        """
        Update a list of sensors if their version is not latest
        :param client_socket: Socket of the client that sent the update
        :param data: Data to be sent
        """
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

    def delete_sensor_data(self, sensor_id, sensor_local, sensor_type):
        """
        Delete a sensor from system
        :param sensor_id: Sensor id
        :param sensor_local: Sensor location
        :param sensor_type: Sensor type
        """
        try:
            logger.info('Info before killing sensor' + str(self.locations[sensor_local][sensor_type]))

            new = [x for x in self.locations[sensor_local][sensor_type] if x["sensor_id"] != sensor_id]

            self.locations[sensor_local][sensor_type] = new

            logger.info('Info after killing the sensor' + str(new))

        except Exception as error:
            logger.error(error)

    def kill_sensor(self, client_socket):
        """
        Send Kill for the sensor socket
        :param client_socket: Sensor socket
        """
        self.send_info(client_socket, 'kill', {})

    def kill_sensors(self, client_socket, data):
        """
        Kill sensors
        :param client_socket: Client socket that request the kill
        :param data: Dict with a list of sensors
        """
        killed = []
        for x in data['sensors']:
            if x:
                for socket_id in self.sensor_id:
                    if self.sensor_id[socket_id] == x:
                        pol_type = self.sensor_reading[x]['type']
                        location = self.sensor_reading[x]['location']
                        self.kill_sensor(socket_id)
                        self.delete_sensor_data(x, location, pol_type)
                        killed.append(x)

        if len(killed) == len(data['sensors']):
            logger.info('Killed all sensor pretended')
            self.send_info(client_socket, 'response', {'status': 200})
        else:
            self.send_info(client_socket, 'response',
                           {'status': 400, 'error': 'Had some error killing sensors verify your input'})

    def list_locals(self, client_socket, message):
        """
        List all locations
        :param client_socket: Socket of the request
        :param message: Dict with thr pollutant
        """
        locals_to_send = []
        for local in self.locations:
            if message['data']['poluente'] in self.locations[local]:
                locals_to_send.append(local)

        if len(locals_to_send) > 0:
            self.send_info(client_socket, 'lista_locais', {'status': 200, 'value': locals_to_send})

        else:
            self.send_info(client_socket, 'lista_locais', {'status': 400,
                                                           'value': "There are no locals for this pollutant"})

    def local_read(self, client_socket, message):
        """
        Get last reading by place
        :param client_socket: Client requested
        :param message: Dict with the location
        """
        last_readings = {}
        try:
            if message['data']['local'] in self.locations:

                for pollutant in self.locations[message['data']['local']]:
                    if pollutant != 'sub_clients':
                        size_array = len(self.locations[message['data']['local']][pollutant])
                        last_readings[pollutant] = self.locations[message['data']['local']][pollutant][size_array - 1][
                            'value']

            if len(last_readings) > 0:
                self.send_info(client_socket, 'leituras_local', {'status': 200, 'value': last_readings})

            else:
                self.send_info(client_socket, 'leituras_local',
                               {'status': 400, 'value': "There are 0 reading in " + message['data']['local']})

        except Exception as err:
            logger.error(err)

    def local_time_read(self, client_socket, message):
        """
        Get a reading by data and time
        :param client_socket: Socket of the request
        :param message: Dict with date and hour
        """
        local = message['local']
        date = message['date']
        hour = message['hour']
        readings = {"date": date, "hour": hour, "local": local}
        try:
            if local in self.locations:
                for poluente in self.locations[local]:
                    if poluente != 'sub_clients':
                        for index in range(len(self.locations[local][poluente])):
                            if self.locations[local][poluente][index]["date"] == date and \
                                    self.locations[local][poluente][index]["hour"] == hour:
                                readings[poluente] = self.locations[local][poluente][index]["value"]
            else:
                self.send_info(client_socket,
                               'local_time_read',
                               {'status': 400, 'value': "The place, \"" + local + "\" not exists.\n"})

            if len(readings) > 3:
                self.send_info(client_socket, 'local_time_read', {'status': 200, 'value': readings})

            else:
                self.send_info(client_socket, 'local_time_read',
                               {'status': 400, 'value': "No data for day: " + date + " time: " + hour + "\n"})

        except Exception as err:
            logger.error(err)

    def decode_message(self, client_socket, message):
        """
        Decode the request type
        :param client_socket: Socket that send the request
        :param message: Data from socket
        """
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

        elif message['type'] == 'local_time_read':
            self.local_time_read(client_socket, message['data'])

        elif message['type'] == 'test_connection':
            logger.debug('Connection tested')

        else:
            logger.error('BEING HACKED')

    def receive_message(self, client_socket, new_user=False):
        """
        Receive message from the socket
        :param client_socket: Socket to receive message
        :param new_user: If the option is a new connection
        :return: Id for the new user
        """
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
        """
        Main function to run the broker
        """
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
