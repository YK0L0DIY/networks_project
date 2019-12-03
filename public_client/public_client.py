import socket
import sys
import pickle
import logging
import os

HEADERSIZE = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class Client:
    client_socket = None
    client_id = None


    # construtor oq ual cria a coencao com o brocker
    def __init__(self, brocker_ip='0.0.0.0', brocker_port='9000', id='client'):
        self.client_id=id

        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Socket successfully created")
        except socket.error as err:
            logger.info("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.client_socket.connect((brocker_ip, int(brocker_port)))
        except Exception as err:
            logger.info("socket creation failed with error %s" % err)
            exit(1)
        data = {'id':self.client_id}
        self.send_info('client_connected',data)

        logger.info(
            f"Successful created sensor {self.client_socket.getsockname()} and conected to brocker {brocker_ip}:{brocker_port}")




    def receive_message(self):
        while True:
            message_header = self.client_socket.recv(HEADERSIZE)

            if len(message_header):
                message_length = int(message_header.decode('utf-8').strip())

                dict = pickle.loads(self.client_socket.recv(message_length))
                if dict['lista_locais']['data']['status']==200:
                    for x in dict['lista_locais']['data']['value']:
                        print(x)
                elif dict['lista_locais']['data']['status']==400:
                    print(dict['lista_locais']['data']['value'])
                #TODO O RESTO
                    

                logger.info(dict['data'])  # sera uma lista de locais que teem sensores daquele tipo
                return


    def menu(self):
        while True:
            try:

                print(
                    "*** Menu ***\n0. Listar locais onde existem sensores de determinado tipo.\n1. Obter última leitura de um local.\n2. Modo publish-subscribe.\n************")
                escolha = int(input())
                if escolha == 0:
                    print("Qual o tipo de poluente? (ex: CO2;NO2...")
                    poluente = input()
                    self.send_info('listar_locais', {'poluente': poluente})


                    return
                elif escolha == 1:
                    print("Qual o local onde quer receber as últimas leituras?\n")
                    local = input()
                    self.send_info('leituras_local', {'local': local})

                    return
                elif escolha == 2:
                    print("Qual o local que quer subescrever?\n")
                    local = input()
                    self.send_info("sub", {'locfal': local})
                    return
                else:
                    print(" Escolha uma 1, 2 ou 3\n")

            except Exception as err:
                logger.info("socket creation failed with error %s" % err)
                exit(1)

    def run_client(self):
        process = os.fork()
        if process > 0:
            self.menu()
        else:
            self.receive_message()







    def send_info(self, type,data):
        try:
            msg = {'type': type,'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

            self.client_socket.send(info)
            logger.info("Data sent to broker")

        except Exception as err:
            logger.info("sending error %s" % err)
            exit(1)

        return


if __name__ == "__main__":

    try:
        #               borcker ip, brocker port   id cliente
        client = Client(sys.argv[1], sys.argv[2],sys.argv[3])
    except:
        client = Client()
    client.run_client()
