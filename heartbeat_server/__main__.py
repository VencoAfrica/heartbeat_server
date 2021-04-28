import argparse
import asyncio
import getpass
import json
import os
import sys

from heartbeat_server import load_config, main


supervisor_content = """[program:heartbeat]
command={py_cmd} -m heartbeat_server --serve
user={user}
autostart=true
autorestart=true
directory={location}
stderr_logfile={log_location}
"""

config_content = {
	"name": "echo server",
	"tcp": {"port": 18901,},
	"udp": {"port": 18902,},
	"redis_server_url": ""
}


def get_supervisor_confdir():
    ''' copied from frappe '''
    possiblities = ('/etc/supervisor/conf.d', '/etc/supervisor.d/', '/etc/supervisord/conf.d', '/etc/supervisord.d')
    for possiblity in possiblities:
        if os.path.exists(possiblity):
            return possiblity
    raise Exception("Supervisor installation not found")

def setup_supervisor():
    ''' create supervisor config '''
    py_cmd = sys.executable
    if "/env/bin/" not in py_cmd:
        raise Exception("Please run using a python executable from a virtual env")

    location = os.getcwd()
    log_location = os.path.join(location, 'logs', 'service.log')
    config_filename = 'supervisor.conf'

    with open(config_filename, 'w') as fhandle:
        fhandle.write(supervisor_content.format(
            py_cmd=py_cmd,
            location=location,
            user=getpass.getuser(),
            log_location=log_location
        ))

    supervisor_conf = os.path.join(get_supervisor_confdir(), 'heartbeat.conf')
    # Check if symlink exists, If not then create it.
    if not os.path.islink(supervisor_conf):
        os.symlink(os.path.abspath(config_filename), supervisor_conf)
    return config_filename

def setup_config_json():
    location = os.path.abspath("config.json")
    exists = os.path.exists(location)
    if not exists:
        with open(location, 'w') as fhandle:
            json.dump(config_content, fhandle, indent=2)
    return location, exists


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heartbeat Server")
    parser.add_argument('--setup', action='store_true', help='Setup supervisor')
    parser.add_argument('--serve', action='store_true', help='Start server')

    args = parser.parse_args()
    if args.setup:
        ouput = setup_supervisor()
        print("Supervisor setup in file: %s\n" % os.path.abspath(ouput))
        config_location, exists = setup_config_json()
        if exists:
            msg = "Using config file: %s\n" % config_location
        else:
            msg = ("Config file created at %s. "
                   "Please edit with appropriate values\n") % config_location
        print(msg)
    elif args.serve:
        # load config and run server
        config = load_config()
        asyncio.run(main({'config': config}))
