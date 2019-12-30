import socket
import sys
import pickle
import logging
import os
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
                        print("Sub Info for " + dict['data']['value']['local'] + " = ",
                              dict['data']['value']['newRead'],dict['data']['value']['type'])
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
                    "2. Modo publish-subscribe.\n"
                    "3. Exit\n")
                escolha = int(input())
                if escolha == 0:
                    print("Qual o tipo de poluente? (ex: CO2;NO2...")
                    poluente = input()
                    self.send_info('listar_locais', {'poluente': poluente})

                elif escolha == 1:
                    print("Qual o local onde quer receber as últimas leituras?\n")
                    local = input()
                    self.send_info('leituras_local', {'local': local})

                elif escolha == 2:
                    print("Qual o local que quer subescrever?\n")
                    local = input()
                    self.send_info("sub", {'local': local})

                elif escolha == 3:
                    self.client_socket.close()
                    exit(0)

            except Exception as err:
                logger.info(" Escolha uma 1, 2 ou 3")
            finally:
                sem.release()
                time.sleep(0.25)

    def run_client(self):

        process = os.fork()
        if process > 0:
            self.menu()
        else:
            self.receive_message()

    def send_info(self, type, data):
        try:
            msg = {'type': type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            self.client_socket.send(info)
            # logger.info("Data sent to broker")


        except Exception as err:
            logger.info("sending error %s" % err)
            exit(1)

        return


if __name__ == "__main__":

    try:
        #               borcker ip, brocker port   id cliente
        client = Client(sys.argv[1], sys.argv[2], sys.argv[3])
    except:
        client = Client()
    client.run_client()
