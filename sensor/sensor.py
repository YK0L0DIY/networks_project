import socket
import select
import sys
import pickle

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

        msg = {'type': 'sensor_registry', 'sensor_id': sensor_id, 'sensor_type': sensor_type,
               'sensor_location': sensor_location}
        msg = pickle.dumps(msg)
        info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

        self.sensor_socket.send(info)

        print(
            f"Successful created sensor {self.sensor_socket.getsockname()} and conected to brocker {brocker_ip}:{brocker_port}")

    def run_sensor(self):
        timeout = 10
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
                        if self.version < dict['version']:
                            self.version = dict['version']
                            with open(dict['file_name'], 'w', encoding='utf-8') as file:
                                file.write(dict['data'])
                                print(f"Version updated to {self.version}")

                    # print(dict)
                else:
                    break

            self.send_info()

    # TODO enviar valores +- corretors ou consistentes
    def send_info(self):
        msg = {'type': 'sensor_reading', 'data': 10}
        msg = pickle.dumps(msg)
        info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

        self.sensor_socket.send(info)


if __name__ == "__main__":
    # print(sys.argv)
    try:
        #               borcker ip, brocker port, sensor id, location ,sensor type
        sensor = Sensor(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        sensor.run_sensor()
    except:
        sensor = Sensor()
        sensor.run_sensor()
