# heartbeat_server

TCP server written in Python. Used to register the heartbeat from electric meter.

  

To use the server, follow the steps below:

  

 1. Install python version >= 3.7  ([https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/](https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/)

2. Setup a virtual environment or activate an existing one:
```
> python3 -m venv env
> . env/bin/activate

```
Install Supervisor
```
apt-get install supervisor
```

Restart Supervisor
```
service supervisor restart
```

3.  Install app:
```
> git clone https://github.com/manqala/hearbeat_server.git
(env)> pip install -e heartbeat_server
```
4. Setup app configuration and edit 'config.json' as required
```
(env)> python -m heartbeat_server --setup
(env)> nano config.json
(env)> sudo supervisorctl restart all
```
5.  If running directly without supervisor, start app using:
```
(env)> python -m heartbeat_server --serve
```

#### Notes:
- Sample config.json:
```json
{
        "name": "echo server",
        "tcp":
        {
                "port": 18901,
        },
        "udp":
        {
                "port": 18902,
        },
        "redis_server_url": "redis://test1:bb5qFU9xFMPCWpEJoKOe60zSN1e6LOkT@redis-10661.c259.us-central1-2.gce.cloud.redislabs.com:10661"
}
```

- App activities are logged in ./logs/app.log
- App service activities (errors caught by supervisor) are logged in ./logs/service.log
