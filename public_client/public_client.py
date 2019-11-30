import socket
import select
import sys
import pickle
import random
HEADERSIZE = 10


class Client:
    client_socket = None


    # construtor oq ual cria a coencao com o brocker
    def __init__(self, brocker_ip='0.0.0.0', brocker_port='9000'):

        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Socket successfully created")
        except socket.error as err:
            print("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.client_socket.connect((brocker_ip, int(brocker_port)))
        except Exception as err:
            print("socket creation failed with error %s" % err)
            exit(1)
        data = {'id':'client'}
        self.send_info('client_connected',data)

        print(
            f"Successful created sensor {self.client_socket.getsockname()} and conected to brocker {brocker_ip}:{brocker_port}")

    def run_client(self):
        try:
            print("*** Menu ***\n1. Listar locais onde existem sensores de determinado tipo.\n2. Obter última leitura de um local.\n3. Modo publish-subscribe.\n************")
            escolha = int(input())
            if escolha == 1:
                print("Qual o local?\n")
                local=input()
                print("Qual o tipo de poluente? (ex: CO2;NO2...")
                poluente=input()
                self.send_info('listar_locais',{'local':local,'poluente':poluente})

                message_header = self.client_socket.recv(HEADERSIZE)

                if not len(message_header):
                    return False

                message_length = int(message_header.decode('utf-8').strip())

                dict = pickle.loads(self.client_socket.recv(message_length))
                print(dict)#sera uma lista de locais que teem sensores daquele tipo
                self.run_client()
                return
            elif escolha == 2:
                print("Qual o local onde quer receber as últimas leituras?\n")
                local = input()
                message_header = self.client_socket.recv(HEADERSIZE)

                if not len(message_header):
                    return False

                message_length = int(message_header.decode('utf-8').strip())

                dict = pickle.loads(self.client_socket.recv(message_length))
                print(dict)#será uma lista das temperaturas desse local.
                self.run_client()
                return
            elif escolha == 3:
                print("Qual o local que quer subescrever?\n")
                local = input()
                return
            else:
                print(" Escolha uma 1, 2 ou 3\n")
                self.run_client()
        except Exception as err:
            print("socket creation failed with error %s" % err)
            exit(1)





    def send_info(self, type,data):
        try:
            msg = {'type': type,'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

            self.client_socket.send(info)
            print("Data sent to broker")

        except Exception as err:
            print("sending error %s" % err)
            exit(1)

        return


if __name__ == "__main__":

    try:
        #               borcker ip, brocker port
        client = Client(sys.argv[1], sys.argv[2])
    except:
        client = Client()

    client.run_client()
