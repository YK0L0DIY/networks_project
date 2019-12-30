import socket
import sys
import pickle
import logging
import yaml

HEADER = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class ClientAdmin:
    server_socket = None
    admin_id = None
    options = {
        '0': 'get last reading from sensor',
        '1': 'get list sensors',
        '2': 'send update to sensors',
        '3': 'kill sensor',
        '4': 'close admin'
    }

    def __init__(self, broker_ip='0.0.0.0',
                 broker_port='9000',
                 admin_id='admin'):

        self.admin_id = admin_id

        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Socket successfully created")
        except socket.error as err:
            logger.error("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.server_socket.connect((broker_ip, int(broker_port)))
        except Exception as err:
            logger.error("socket creation failed with error %s" % err)
            exit(1)

        data = {'id': self.admin_id}
        self.send_info('client_connected', data)

        logger.info(
            f"Successful created client_admin {self.server_socket.getsockname()}"
            f" and connected to broker {broker_ip}:{broker_port}")

    def receive_message(self):
        try:
            message_header = self.server_socket.recv(HEADER)

            if not len(message_header):
                raise ValueError

            message_length = int(message_header.decode('utf-8').strip())

            response = pickle.loads(self.server_socket.recv(message_length))

            return response
        except Exception as err:
            logger.error(err)

    def get_last_reading(self):
        sensor = input('From which sensor?\n-> ')
        self.send_info('get_last_reading', {'sensor': sensor})

        response = self.receive_message()
        if response['data']['status'] == 200:
            print('Last reading from sensor: ' + str(response['data']['value']))
        else:
            logger.error(response['data']['error'])
        return

    def get_list_of_sensors(self):
        self.send_info('get_all_sensors', {})
        response = self.receive_message()
        if response['data']['status'] == 200:
            for x in response['data']['sensors']:
                print(x)
        else:
            logger.error(response['data']['error'])
        return

    def send(self, file_name, version, sensor_type):
        try:
            with open(file_name, 'r', encoding='utf-8') as file:
                data = {'version': version, 'file_name': file_name, 'content': file.read(), 'sensor_type': sensor_type}
                self.send_info('update', data)

        except Exception as err:
            logger.error(err)
            print('File not exist do you want to create it? [y/n]')
            option = input('\n ->')
            if option == 'y':
                info = input('content:\n')
                data = {'version': version, 'file_name': file_name, 'content': info, 'sensor_type': sensor_type}
                self.send_info('update', data)
                return 0

            else:
                return 1
        return 0

    def send_file(self):
        sensor_type = input("sensor type?\n-> ")
        file_name = input("file name?\n-> ")
        version = input("version?\n-> ")
        r = self.send(file_name, version, sensor_type)
        if r == 0:
            response = self.receive_message()
            if response['data']['status'] == 200:
                print('Sensors of ' + sensor_type + ' updated')
            else:
                logger.error(response['data']['error'])
        return

    def kill_sensors(self):
        sensors = input('Which sensors?[ids separated by spaces]\n-> ')
        self.send_info('kill_sensors', {'sensors': sensors.split(' ')})
        response = self.receive_message()
        if response['data']['status'] == 200:
            print('Killed all sensors')
        else:
            logger.error(response['data']['error'])
        return

    def close(self):
        self.server_socket.close()
        exit(0)

    def run_client_admin(self):
        try:
            while True:
                for x in self.options:
                    print(x, self.options[x])
                command = input('-> ')

                if command == '0':
                    self.get_last_reading()
                elif command == '1':
                    self.get_list_of_sensors()
                elif command == '2':
                    self.send_file()
                elif command == '3':
                    self.kill_sensors()
                elif command == '4':
                    self.close()
                else:
                    logger.error('Invalid input')

        except Exception as err:
            logger.error(err)

    def send_info(self, data_type, data):
        try:
            msg = {'type': data_type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            self.server_socket.send(info)
            print("Data sent to broker")

        except Exception as err:
            print("sending error %s" % err)
            exit(1)

        return


if __name__ == "__main__":

    try:
        client = ClientAdmin(broker_ip=sys.argv[1],
                             broker_port=sys.argv[2],
                             admin_id=sys.argv[3])
    except Exception as no_args:
        logger.error("No input, reading from file %s" % no_args)

        with open('config.yaml') as conf:
            configs = yaml.load(conf, Loader=yaml.FullLoader)
            client = ClientAdmin(broker_ip=configs['broker_ip'],
                                 broker_port=configs['broker_port'],
                                 admin_id=configs['admin_id'])

    client.run_client_admin()
c