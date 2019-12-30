import socket
import select
import sys
import pickle
import random
import logging
import yaml

HEADER = 10

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class Sensor:
    sensor_socket = None
    version = 0

    def __init__(self, broker_ip='0.0.0.0',
                 broker_port='9000',
                 sensor_id='test1',
                 sensor_location='lisboa',
                 sensor_type='CO2',
                 timeout=10):

        try:
            self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Socket successfully created")
        except socket.error as err:
            logger.error("socket creation failed with error %s" % err)
            exit(1)

        try:
            self.sensor_socket.connect((broker_ip, int(broker_port)))
        except Exception as err:
            logger.error("socket creation failed with error %s" % err)
            exit(1)

        data = {'id': sensor_id, 'sensor_type': sensor_type, 'sensor_location': sensor_location}
        self.send_info('sensor_registry', data)

        self.timeout = timeout
        self.sensor_type = sensor_type

        logger.info(f"Successful created sensor {self.sensor_socket.getsockname()}"
                    f" and conected to brocker {broker_ip}:{broker_port}")

    def reading(self):
        if self.sensor_type == 'CO2':
            return random.randrange(20, 50, 3)
        elif self.sensor_type == 'PM2.5':
            return random.randrange(0, 5, 0.2)
        elif self.sensor_type == 'PM10':
            return random.randrange(0, 50, 2)
        elif self.sensor_type == 'NO2':
            return random.randrange(0, 16, 2)
        elif self.sensor_type == 'O3':
            return random.randrange(0, 16, 1)
        else:
            return random.randrange(20, 50, 3)

    def run_sensor(self):
        while True:
            while True:
                ready = select.select([self.sensor_socket], [], [], self.timeout)

                if ready[0]:
                    message_header = self.sensor_socket.recv(HEADER)

                    if not len(message_header):
                        return False

                    message_length = int(message_header.decode('utf-8').strip())

                    response = pickle.loads(self.sensor_socket.recv(message_length))

                    if response['type'] == 'kill':
                        logger.info('Killing sensor')
                        self.sensor_socket.close()
                        exit(0)

                    elif response['type'] == 'update':
                        if int(self.version) < int(response['data']['version']):
                            self.version = response['data']['version']
                            with open(response['data']['file_name'], 'w', encoding='utf-8') as file:
                                file.write(response['data']['content'])
                                logger.info(f"Version updated to {self.version}")

                else:
                    break

            self.send_info('sensor_reading', {'leitura': self.reading()})

    def send_info(self, msg_type, data):
        try:
            msg = {'type': msg_type, 'data': data}
            msg = pickle.dumps(msg)
            info = bytes(f"{len(msg):<{HEADER}}", 'utf-8') + msg

            self.sensor_socket.send(info)

        except Exception as send_err:
            logger.error("Sending error %s" % send_err)
            exit(1)

        return


if __name__ == "__main__":
    try:
        sensor = Sensor(broker_ip=sys.argv[1],
                        broker_port=sys.argv[2],
                        sensor_id=sys.argv[3],
                        sensor_location=sys.argv[4],
                        sensor_type=sys.argv[5],
                        timeout=int(sys.argv[6]))

    except Exception as input_err:
        logger.error("No input, reading from file %s" % input_err)

        with open('config.yaml') as conf:
            configs = yaml.load(conf, Loader=yaml.FullLoader)
            sensor = Sensor(broker_ip=configs['broker_ip'],
                            broker_port=configs['broker_port'],
                            sensor_id=configs['sensor_id'],
                            sensor_location=configs['sensor_location'],
                            sensor_type=configs['sensor_type'],
                            timeout=configs['timeout'])

    sensor.run_sensor()
