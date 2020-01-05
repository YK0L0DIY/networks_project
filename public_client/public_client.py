import logging
import os
import pickle
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

    # construtor oq ual cria a coencao com o brocker
    def __init__(self, broker_ip='0.0.0.0', broker_port='9000', id='client'):
        self.client_id = id

        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Socket successfully created")
        except socket.error as err:
            logger.info("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.client_socket.connect((broker_ip, int(broker_port)))
        except Exception as err:
            logger.info("socket creation failed with error %s" % err)
            exit(1)
        data = {'id': self.client_id}
        self.send_info('client_connected', data)

        logger.info(
            f"Successful created client {self.client_socket.getsockname()} and conected to brocker {broker_ip}:{broker_port}")

    def receive_message(self):
        while True:
            sem.acquire()
            message_header = self.client_socket.recv(HEADER)

            if message_header:
                message_length = int(message_header.decode('utf-8').strip())

                dict = pickle.loads(self.client_socket.recv(message_length))
                # logger.info(dict)  # sera uma lista de locais que teem sensores daquele tipo

                # logger.info("A receber dados: \n")
                if dict['type'] == 'lista_locais':
                    if dict['data']['status'] == 200:
                        for x in dict['data']['value']:
                            print(x)
                    elif dict['data']['status'] == 400:
                        print(dict['data']['value'])
                if dict['type'] == 'leituras_local':
                    if dict['data']['status'] == 200:
                        for key in dict['data']['value']:
                            print(key + ": " + str(dict['data']['value'][key]))
                    elif dict['data']['status'] == 400:
                        print(dict['data']['value'])
                if dict['type'] == 'subMessage':
                    if dict['data']['status'] == 200:
                        print("Sub Info for " + dict['data']['value']['local'] + "= ",
                              dict['data']['value']['newRead'])
                    elif dict['data']['status'] == 400:
                        print(dict['data']['value'])
                if dict['type'] == 'local_time_read':
                    if dict['data']['status'] == 200:
                            print("Readings from " + dict['data']['value']['local'] + " at " + dict['data']['value']['date'] +" "+ dict['data']['value']['hour']+":\n")
                            for poluent in dict['data']['value']:
                                if poluent != 'date' and poluent != 'local' and poluent != 'hour':
                                    print(dict['data']['value'][poluent]," "+poluent)
                    elif dict['data']['status'] == 400:
                        print(dict['data']['value'])

                sem.release()
                time.sleep(0.25)

    def menu(self):
        while True:
            sem.acquire()
            try:
                print(
                    "Menu:\n"
                    "0. Listar locais onde existem sensores de determinado tipo.\n"
                    "1. Obter última leitura de um local.\n"
                    "2. Obter última leitura de um local com data e hora\n"
                    "3. Modo publish-subscribe.\n"
                    "4. Exit\n")
                escolha = int(input('->'))
                if escolha == 0:
                    print("Qual o tipo de poluente? (ex: CO2;NO2...")
                    poluente = input('->')
                    self.send_info('listar_locais', {'poluente': poluente})

                elif escolha == 1:
                    print("Qual o local onde quer receber as últimas leituras?\n")
                    local = input('->')
                    self.send_info('leituras_local', {'local': local})

                elif escolha == 2:
                    print("Qual o local?\n")
                    local = input('->')
                    print("Qual a data da leitura?\nEx: 2019-12-17\n")
                    date = input('->')
                    print("Qual a hora da leitura?\nEx: 23:27\n")
                    hour = input('->')
                    self.send_info('local_time_read', {'local': local,'date': date,'hour': hour})

                elif escolha == 3:
                    print("Qual o local que quer subescrever?\n")
                    local = input('->')
                    self.send_info("sub", {'local': local})

                elif escolha == 4:
                    self.client_socket.close()
                    exit(0)
                else:
                    print('Escolha invalida\n')

            except Exception as err:
                logger.info("Invalid input\n")
            finally:
                sem.release()
                time.sleep(0.25)

    def test_connection(self):
        while 1:
            self.send_info('test_connection', '')
            time.sleep(5)

    def run_client(self):

        process = os.fork()
        if process > 0:
            self.test_connection()
        else:
            process = os.fork()
            if process > 0:
                self.receive_message()
            else:
                self.menu()

    def send_info(self, type, data):
        try:
            msg = {'type': type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            self.client_socket.send(info)

        except Exception as err:
            print('\n')
            logger.error("Broker not exist %s" % err)
            exit(1)

        return


if __name__ == "__main__":

    try:
        #               borcker ip, brocker port   id cliente
        client = Client(broker_ip=sys.argv[1],
                        broker_port=sys.argv[2],
                        id=sys.argv[3])
    except Exception as no_args:
        logger.error("No input, reading from file %s" % no_args)

        with open('config.yaml') as conf:
            configs = yaml.load(conf, Loader=yaml.FullLoader)
            client = Client(broker_ip=configs['broker_ip'],
                            broker_port=configs['broker_port'],
                            id=configs['id'])
    client.run_client()
