import socket
import select
import sys
import pickle
import random
HEADERSIZE = 10


class Sensor:
    sensor_socket = None
    version = 0

    # construtor oq ual cria a coencao com o brocker
    def __init__(self, brocker_ip='0.0.0.0', brocker_port='9000', sensor_id='test1',
                 sensor_location='lisb', sensor_type='CO2', ):

        try:
            self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Socket successfully created")
        except socket.error as err:
            print("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.sensor_socket.connect((brocker_ip, int(brocker_port)))
        except Exception as err:
            print("socket creation failed with error %s" % err)
            exit(1)

        data = {'id': sensor_id, 'sensor_type': sensor_type, 'sensor_location': sensor_location}
        self.send_info('sensor_registry', data)

        print(
            f"Successful created sensor {self.sensor_socket.getsockname()} and conected to brocker {brocker_ip}:{brocker_port}")

    def run_sensor(self):
        timeout = 3
        while True:
            while True:
                ready = select.select([self.sensor_socket], [], [], timeout)

                if ready[0]:
                    message_header = self.sensor_socket.recv(HEADERSIZE)

                    if not len(message_header):
                        return False

                    message_length = int(message_header.decode('utf-8').strip())

                    dict = pickle.loads(self.sensor_socket.recv(message_length))

                    if dict['type'] == 'kill':
                        print('Killing sensor')
                        self.sensor_socket.close()
                        exit(0)

                    elif dict['type'] == 'update':
                        if self.version < dict['data']['version']:
                            self.version = dict['data']['version']
                            with open(dict['data']['file_name'], 'w', encoding='utf-8') as file:
                                file.write(dict['data']['content'])
                                print(f"Version updated to {self.version}")

                    # print(dict)
                else:
                    break

            self.send_info('sensor_reading', {'leitura': random.randrange(20, 50, 3)})

    # TODO enviar valores +- corretors ou consistentes
    def send_info(self, type, data):
        try:
            msg = {'type': type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

            self.sensor_socket.send(info)

        except Exception as err:
            print("sneding error %s" % err)
            exit(1)

        return


if __name__ == "__main__":
    ## todo verificar argumentos e nao provicae execoes
    try:
        #               borcker ip, brocker port, sensor id, location ,sensor type
        sensor = Sensor(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    except:
        sensor = Sensor()

    sensor.run_sensor()
