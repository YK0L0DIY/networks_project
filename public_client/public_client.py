import logging
import os
import pickle
import signal
import socket
import sys
import threading
import time

import yaml

HEADER = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

sem = threading.Semaphore()


class Client:
    client_socket = None
    client_id = None

    def __init__(self,
                 broker_ip,
                 broker_port,
                 client_id):

        self.client_id = client_id

        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Socket successfully created")
        except socket.error as err:
            logger.info("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.client_socket.connect((broker_ip, int(broker_port)))
        except Exception as err:
            logger.info("Socket creation failed with error %s" % err)
            exit(1)

        data = {'id': self.client_id}
        self.send_info('client_connected', data)

        logger.info(
            f"Successful created client {self.client_socket.getsockname()}"
            f" and connected to broker {broker_ip}:{broker_port}")

    def receive_message(self):
        while True:
            sem.acquire()
            message_header = self.client_socket.recv(HEADER)

            if message_header:
                message_length = int(message_header.decode('utf-8').strip())

                response = pickle.loads(self.client_socket.recv(message_length))

                if response['type'] == 'lista_locais':
                    if response['data']['status'] == 200:
                        for x in response['data']['value']:
                            print(x)

                    elif response['data']['status'] == 400:
                        print(response['data']['value'])

                if response['type'] == 'leituras_local':
                    if response['data']['status'] == 200:
                        for key in response['data']['value']:
                            print(key + ": " + str(response['data']['value'][key]))

                    elif response['data']['status'] == 400:
                        print(response['data']['value'])

                if response['type'] == 'subMessage':
                    if response['data']['status'] == 200:
                        print("Sub Info for " + response['data']['value']['local'] + "= ",
                              response['data']['value']['newRead'], response['data']['value']['type'])

                    elif response['data']['status'] == 400:
                        print(response['data']['value'])

                if response['type'] == 'local_time_read':
                    if response['data']['status'] == 200:
                        print("Readings from " + response['data']['value']['local'] + " at "
                              + response['data']['value']['date'] + " " + response['data']['value']['hour'] + ":\n")

                        for pollutant in response['data']['value']:
                            if pollutant != 'date' and pollutant != 'local' and pollutant != 'hour':
                                print(response['data']['value'][pollutant], " " + pollutant)

                    elif response['data']['status'] == 400:
                        print(response['data']['value'])

                sem.release()
                time.sleep(0.25)

    def menu(self):
        while True:
            sem.acquire()
            try:
                print(
                    "Menu:\n"
                    "0. List sensor by type.\n"
                    "1. Get last reading by location.\n"
                    "2. Get reading by date and time.\n"
                    "3. Mode publish-subscribe.\n"
                    "4. Exit.\n")

                option = int(input('->'))

                if option == 0:
                    print("What type of pollutant? (ex: CO2;NO2...")
                    pollutant = input('->')
                    self.send_info('listar_locais', {'poluente': pollutant})

                elif option == 1:
                    print("Where do you want to receive the latest readings?\n")
                    local = input('->')
                    self.send_info('leituras_local', {'local': local})

                elif option == 2:
                    print("Local?\n")
                    local = input('->')
                    print("Date?\nEx: 2019-12-17\n")
                    date = input('->')
                    print("Hour?\nEx: 23:27\n")
                    hour = input('->')
                    self.send_info('local_time_read', {'local': local, 'date': date, 'hour': hour})

                elif option == 3:
                    print("Where do you want to Subscribe?\n")
                    local = input('->')
                    self.send_info("sub", {'local': local})

                elif option == 4:
                    self.client_socket.close()
                    os.kill(0, signal.SIGSTOP)
                    exit(0)
                else:
                    print('Invalid input\n')

            except Exception as err:
                logger.info("Invalid input\n")
                logger.debug(err)
            finally:
                sem.release()
                time.sleep(0.25)

    def test_connection(self):
        """
        Function to test the connection with broker
        """
        while 1:
            self.send_info('test_connection', '')
            time.sleep(2)

    def run_client(self):
        """
        Main function to run client
        """
        process = os.fork()
        if process > 0:
            self.test_connection()
        else:
            process = os.fork()
            if process > 0:
                self.receive_message()
            else:
                self.menu()

    def send_info(self, type_message, data):
        """
        Send message to broker
        :param type_message: Message type
        :param data: Data to be send
        """
        try:
            msg = {'type': type_message, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            self.client_socket.send(info)

        except Exception as err:
            print('\n')
            logger.error("Broker not exist %s" % err)
            os.kill(0, signal.SIGSTOP)
            exit(0)


if __name__ == "__main__":

    try:
        client = Client(broker_ip=sys.argv[1],
                        broker_port=sys.argv[2],
                        client_id=sys.argv[3])

    except Exception as no_args:
        logger.error("No input, reading from file %s" % no_args)

        with open('config.yaml') as conf:
            configs = yaml.load(conf, Loader=yaml.FullLoader)
            client = Client(broker_ip=configs['broker_ip'],
                            broker_port=configs['broker_port'],
                            client_id=configs['id'])

    client.run_client()
