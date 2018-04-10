import asyncio
import traceback
from pathlib import Path

import yaml
from asyncirc.protocol import IrcProtocol
from asyncirc.server import Server
from influxdb import InfluxDBClient
from requests import RequestException


def load_config():
    conf_file = Path("config.yaml")
    with conf_file.open() as f:
        conf = yaml.safe_load(f)

    return conf


config = load_config()


def on_luser_start(futures):
    async def _handle(proto, message):
        try:
            server_fut = futures['server_count']
            normal_fut = futures['normal_users']
            invis_fut = futures['invisible_users']
        except LookupError:
            return

        words = message.parameters[-1].split()

        if not normal_fut.done():
            normal_fut.set_result(int(words[2]))

        if not invis_fut.done():
            invis_fut.set_result(int(words[5]))

        if not server_fut.done():
            server_fut.set_result(int(words[8]))

    return _handle


def on_opers_online(data):
    async def _handle(proto, message):
        try:
            fut = data['opers']
        except LookupError:
            return

        if not fut.done():
            fut.set_result(int(message.parameters[1]))

    return _handle


def on_chans_formed(data):
    async def _handle(proto, message):
        try:
            fut = data['channels']
        except LookupError:
            return

        if not fut.done():
            fut.set_result(int(message.parameters[1]))

    return _handle


def on_global_users(data):
    async def _handle(proto, message):
        try:
            count_fut = data['user_count']
            max_fut = data['user_max']
        except LookupError:
            return

        words = message.parameters[-1].split()
        if not count_fut.done():
            count_fut.set_result(int(words[3]))

        if not max_fut.done():
            max_fut.set_result(int(words[5]))

    return _handle


async def run():
    client = InfluxDBClient.from_dsn(config['database']['url'], verify_ssl=True)
    databases = [db['name'] for db in client.get_list_database()]

    if client._database not in databases:
        client.create_database(client._database)

    irc_conf = config['irc']
    servers = [Server(irc_conf['server'], irc_conf['port'], irc_conf.get('ssl', False))]
    interval = irc_conf['interval']
    conn_commands = irc_conf.get('connect_commands', [])
    async with IrcProtocol(servers, irc_conf['nick'], irc_conf['user']) as proto:
        await proto.connect()
        await proto.wait_for('376')

        futures = {}

        proto.register('251', on_luser_start(futures))
        proto.register('252', on_opers_online(futures))
        proto.register('254', on_chans_formed(futures))
        proto.register('266', on_global_users(futures))

        for cmd in conn_commands:
            proto.send(cmd)

        while True:
            futures.update(
                (name, proto.loop.create_future())
                for name in (
                    'opers', 'channels', 'user_count', 'user_max', 'server_count', 'normal_users', 'invisible_users'
                )
            )

            proto.send("LUSERS")
            try:
                await asyncio.wait_for(asyncio.gather(*futures.values()), interval)
            except asyncio.TimeoutError:
                pass
            else:
                data = {
                    key: await fut
                    for key, fut in futures.items()
                }

                print(data)

                tags = config['database']['tags']

                body = [
                    {
                        'measurement': 'user_count',
                        'fields': {
                            "oper_count": data['opers'],
                            "user_count": data['user_count'],
                            "user_max": data['user_max'],
                            "visible_users": data["normal_users"],
                            "invisible_users": data["invisible_users"],
                        }
                    },
                    {
                        'measurement': 'channel_count',
                        'fields': {
                            "channels": data['channels'],
                        }
                    },
                    {
                        'measurement': 'server_count',
                        'fields': {
                            "server_count": data['server_count'],
                        }
                    },
                ]

                try:
                    client.write_points(body, tags=tags)
                except RequestException:
                    traceback.print_exc()

                futures.clear()

                await asyncio.sleep(interval)


def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run())
    finally:
        loop.stop()

    loop.close()


if __name__ == '__main__':
    main()
