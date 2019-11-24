import socket
import select
import sys
import pickle
import json

HEADERSIZE = 10


class Sensor:
    sensor_socket = None
    version = 1

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

        # ciclo principal do sensor muito a modificar TODO { receber ficheiros, receber kill}

    def run_sensor(self):
        timeout = 10
        while True:
            while True:
                ready = select.select([self.sensor_socket], [], [], timeout)

                if ready[0]:
                    data = self.sensor_socket.recv(4096)
                    print(data)
                else:
                    break

            self.send_info()

    def send_info(self):
        msg = {'type': 'sensor_reading', 'data': 10}
        msg = pickle.dumps(msg)
        info = bytes(f"{len(msg):<{HEADERSIZE}}", 'utf-8') + msg

        self.sensor_socket.send(info)


if __name__ == "__main__":
    print(sys.argv)
    #               borcker ip, brocker port, sensor id, location ,sensor type
    sensor = Sensor(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    sensor.run_sensor()
