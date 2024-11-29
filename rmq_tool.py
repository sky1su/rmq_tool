#!/usr/bin/env python3
# (c) 2024 sky1su

import argparse
import json

import pika
from jsonschema import ValidationError, validate
from pika.exceptions import AMQPConnectionError


class rmq_tool():
    config = {
        'RABBITMQ_USERNAME': '',
        'RABBITMQ_PASSWORD': '',
        'RABBITMQ_HOST': '',
        'RABBITMQ_PORT': 5672,
        'RABBITMQ_VHOST': '',
        'RABBITMQ_EXCHANGE': '',
        'RABBITMQ_QUEUE': '',
        'DATA_FILE': 'data.json',
        'MODE': '',
        'LIMIT_MESSAGES': 0,
        'CHUNK_SIZE': 0,
    }
    counter = 0
    rmq_channel = object

    def __init__(self, config_path):
        config = self.__get_config(config_path)
        if self.__config_validate(config):
            self.__set_config(config)
        else:
            print('Неверный файл конфигурции, смотри сообщения выше.')
            exit(1)

    def __del__(self):
        self.rmq_channel.close()

    def get_mode(self):
        return self.config['MODE']

    def __config_validate(self, config):
        schema = {
            "type": "object",
            "title": "RabbitMQ Configuration Schema",
            "properties": {
                "RABBITMQ_USERNAME": {"type": "string"},
                "RABBITMQ_PASSWORD": {"type": "string"},
                "RABBITMQ_HOST": {"type": "string"},
                "RABBITMQ_PORT": {"type": "integer", "minimum": 1},
                "RABBITMQ_VHOST": {"type": "string"},
                "RABBITMQ_EXCHANGE": {"enum": ['', 'amq.direct', 'amq.fanout']},
                "RABBITMQ_QUEUE": {"type": "string"},
                "DATA_FILE": {"type": "string"},
                "MODE": {"enum": ["dump", "push"]},
                "LIMIT_MESSAGES": {"type": "integer", "minimum": 0},
            },
            "required": [
                "RABBITMQ_USERNAME",
                "RABBITMQ_PASSWORD",
                "RABBITMQ_HOST",
                "RABBITMQ_QUEUE",
                "MODE"
            ]
        }
        try:
            validate(config, schema)
            return True
        except ValidationError as e:
            print(e)
            return False

    def __get_config(self: object, config_path: object) -> object:
        with open(config_path) as config_file:
            config = json.load(config_file)
            return config
        return None

    def __set_config(self, config):
        self.config = {
            'RABBITMQ_USERNAME': config.get('RABBITMQ_USERNAME'),
            'RABBITMQ_PASSWORD': config.get('RABBITMQ_PASSWORD'),
            'RABBITMQ_HOST': config.get('RABBITMQ_HOST'),
            'RABBITMQ_PORT': config.get('RABBITMQ_PORT', 5672),
            'RABBITMQ_VHOST': config.get('RABBITMQ_VHOST', '/'),
            'RABBITMQ_EXCHANGE': config.get('RABBITMQ_EXCHANGE', ''),
            'RABBITMQ_QUEUE': config.get('RABBITMQ_QUEUE'),
            'DATA_FILE': config.get('DATA_FILE', 'data.json'),
            'MODE': config.get('MODE', 'dump'),
            'LIMIT_MESSAGES': config.get('LIMIT_MESSAGES', 0),
            'CHUNK_SIZE': config.get('CHUNK_SIZE', 100)
        }

    def __rmq_connection(self):
        credentials = pika.PlainCredentials(self.config['RABBITMQ_USERNAME'],
                                            self.config['RABBITMQ_PASSWORD'])
        parameters = pika.ConnectionParameters(host=self.config['RABBITMQ_HOST'],
                                               virtual_host=self.config['RABBITMQ_VHOST'],
                                               port=self.config['RABBITMQ_PORT'],
                                               credentials=credentials,
                                               socket_timeout=5)
        try:
            connection = pika.BlockingConnection(parameters)
            self.rmq_channel = connection.channel()
        except AMQPConnectionError as e:
            if "timeout" in str(e).lower():
                print(f"Ошибка подключения: время ожидания истекло ({parameters.socket_timeout} секунд).")
                exit(1)

    def __mq_process_message(self, ch, method, properties, body):
        try:
            message = json.loads(body)
            with open(self.config['DATA_FILE'], 'a') as file:
                json.dump(message, file, ensure_ascii=False)
                file.write('\n')
        except Exception as e:
            print(f"Произошла ошибка при обработке сообщения: {e}")
        self.counter += 1
        print(f'записано сообщений {self.counter}')

        if self.config['LIMIT_MESSAGES'] == 0:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            if self.counter >= self.config['LIMIT_MESSAGES']:
                ch.stop_consuming()
                print(f"Получено {self.counter} сообщений. Остановка работы.")
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def set_mode(self, mode):
        if mode in ('dump', 'push'):
            self.config['MODE'] = mode

    def set_data_file(self, data_file):
        self.config['DATA_FILE'] = data_file

    def mq_dump(self):
        self.__rmq_connection()
        self.rmq_channel.basic_consume(on_message_callback=self.__mq_process_message,
                                       queue=self.config['RABBITMQ_QUEUE'],
                                       auto_ack=False)
        print('Ожидание сообщений. Для выхода нажмите Ctrl+C')
        try:
            self.rmq_channel.start_consuming()
        except KeyboardInterrupt:
            print("Прервано пользователем")

    def mq_push(self):
        self.__rmq_connection()
        try:
            with open(self.config['DATA_FILE'], 'r') as file:
                while True:
                    chunk = [json.loads(line.strip()) for line in file.readlines(self.config['CHUNK_SIZE'])]
                    if not chunk or (self.counter >= self.config['LIMIT_MESSAGES'] != 0):
                        break
                    for message in chunk:
                        self.rmq_channel.basic_publish(exchange=self.config['RABBITMQ_EXCHANGE'],
                                                       routing_key=self.config['RABBITMQ_QUEUE'],
                                                       body=json.dumps(message)
                                                       )
                    self.counter += len(chunk)
                    print(
                        f"Отправлено сообщений в очередь {self.config['RABBITMQ_QUEUE']}@"
                        f"{self.config['RABBITMQ_HOST']}: {self.counter} ")

        except Exception as e:
            print(f"Ошибка при чтении или отправке сообщения: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='rmq_tool предназначена для работы с данными в rabbitmq')
    parser.add_argument('-с', '--config',
                        help='Путь к конфигурационному файлу',
                        required=False,
                        default='config.json'
                        )
    parser.add_argument('-m', '--mode',
                        help='Переопределение режима работы. может быть быть dump или push',
                        required=False
                        )
    parser.add_argument('-f', '--file',
                        help='Переопределяет файл данных',
                        required=False
                        )

    args = parser.parse_args()

    mq_tool = rmq_tool(args.config)

    if args.mode == 'dump':
        mq_tool.set_mode('dump')
    if args.mode == 'push':
        mq_tool.set_mode('push')

    if args.file:
        mq_tool.set_data_file(args.file)

    if mq_tool.get_mode() == 'dump':
        mq_tool.mq_dump()
        exit(0)
    if mq_tool.get_mode() == 'push':
        mq_tool.mq_push()
        exit(0)
    else:
        exit(0)
