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

HEADER_LENGTH = 10
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Associamos o socket a um ipv4 e do tipo TCP
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Para podermos usar sempre a mesma porta sem ter de esperar pelo garbage collector do SO

server_socket.bind(("localhost", 9000))

server_socket.listen(5) #o argumento é o numero de elementos em fila de espera, escolhi 5 porque penso que seja suficiente

global sockets_list,clients
sockets_list=[server_socket] #o server socket tem de fazer parte da lista de conexões para podermos "escutar" novas ligações
clients={}# é um dicionario com informação do tipo de cliente (admin ou public_client ou sensor)
sensor_dic={}# key é o id e value é o socket

def decode_mesage(user,message): # podia ser a função que ia ver o que queriamos fazer com a mensagem
    if (user['data'].decode('utf-8') == "admin" and message.lower()[:11] == "kill sensor"):  # partindo do principio que uma mensagem de kill sensor seria do tipo kill sensor 45
        #temos de ver qual a forma pela qual vamos optar para desligar um sensor ( necessitariamos de saber o socket dele.
        return False
    return False

def receive_message(client_socket):
    try:
        message_header = client_socket.recv(HEADER_LENGTH) #recebemos a informação do tamanho da mensagem primeiro

        if not len(message_header): #se não recebermos nada é porque o client fechou a connecção
            return False
        message_length = int(message_header.decode('utf-8').strip()) # convertemos o hearder num numero inteiro
        return {"header":message_header, "data": client_socket.recv(message_length)} #a mensagem é um dicionario com o tramanho da mensagem e a propia mensagem
    except:# se entrarmos aqui o client fechou a conexão violentamente
        return False



while True:
    read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list) #Ficamos á espera que haja alguma coisa para ler dos clients

    for notified_socket in read_sockets:
        if notified_socket == server_socket: #isto significa que alguem se conectou pela primeira vez

            client_socket, client_address = server_socket.accept()

            user = receive_message(client_socket)
            if user is False:#se alguem se disconectar continuamos á escuta dos sockets
                continue

            sockets_list.append(client_socket)

            clients[client_socket] = user # ficamos com a imformação que aquele socket representa um admin ou public_client / ou um sensor
            print("New Connection from {}, UserType = {} ".format(client_address,user["data"].decode('utf-8')))

        else: #não é uma nova conexão

            message = receive_message(notified_socket)
            message = message["data"].decode('utf-8')
            if message is False: #client disconnected
                print("Closed Connection from User = {} ".format(clients[notified_socket]["data"].decode('utf-8')))

                #remover da lista de sockets e da lista de clients

                sockets_list.remove(notified_socket)

                del clients[notified_socket]
                continue

            user = clients[notified_socket] #vamos buscar a identidade do client
            print("Received message from {}: \n Message: {} ".format(user['data'].decode('utf-8'),message))
            decode_mesage(user,message)
