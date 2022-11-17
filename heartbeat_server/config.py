import os
import json


config_content = {
  "ccu": {
    "host": "0.0.0.0",
    "port": 18901,
    "name": "ccu"
  },
  "db": {
    "name": "heartbeat.db"
  },
  "hes": {
    "server_url": "http://localhost:8001/api/method/meter_client.api.v1.receive_readings",
    "auth_token": "9dc488d5f0eed02:8a73efd1a4508dc"
  },
  "log": {
    "file": "app.log",
    "level": 10,
    "_comment": "logging.DEBUG"
  },
  "bulk_write_requests": {
    "host": "0.0.0.0",
    "port": 18902
  },
  "redis": {
    "host": "0.0.0.0",
    "port": 6379,
    "db": 0
  }
}


def setup_config_json(config_file='config.json'):
    location = os.path.abspath(config_file)
    exists = os.path.exists(location)
    if not exists:
        with open(location, 'w') as fhandle:
            json.dump(config_content, fhandle, indent=2)
    return location, exists
