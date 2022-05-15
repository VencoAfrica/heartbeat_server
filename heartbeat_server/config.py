import os
import json


config_content = {
    "tcp": {
        "port": 18901,
    },
    "udp": {
        "port": 18902,
    }
}


def setup_config_json(config_file='config.json'):
    location = os.path.abspath(config_file)
    exists = os.path.exists(location)
    if not exists:
        with open(location, 'w') as fhandle:
            json.dump(config_content, fhandle, indent=2)
    return location, exists
