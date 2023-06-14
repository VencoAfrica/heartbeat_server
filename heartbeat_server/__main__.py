import argparse
import asyncio

from . import load_config, heartbeat_server
from .logs import create_logs_folder
from .supervisor import setup_supervisor
from .config import setup_config_json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heartbeat Server")
    parser.add_argument('--setup', action='store_true', help='Setup supervisor')
    parser.add_argument('--serve', action='store_true', help='Start server')
    parser.add_argument('--user', help='User to run supervisor script with')

    args = parser.parse_args()

    if args.setup:
        create_logs_folder()
        setup_supervisor(user=args.user,
                         config_filename="config.json",
                         log_file='app.log')
        setup_config_json()

    if args.serve:
        config = load_config()
        asyncio.run(heartbeat_server(config))
