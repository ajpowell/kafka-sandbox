from quixstreams import Application
import json

app = Application(
    broker_address='192.168.2.210:9092',
    loglevel='DEBUG',
    consumer_group='weather_reader',
    auto_offset_reset='latest',  # On start get latest message or if go from bookmark, if available
                                 # use 'earliest' to start from beginning of all messages
)


def main():
    with app.get_consumer() as consumer:
        consumer.subscribe(['weather_data_demo'])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print('waiting...')
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode('utf8')
                value = json.loads(msg.value())
                offset = msg.offset()

                print('{} {} {}'.format(offset, key, value))
                consumer.store_offsets(msg)  # Remember where we got to...'bookmark'


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
