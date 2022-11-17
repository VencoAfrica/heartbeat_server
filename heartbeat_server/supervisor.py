import sys
import os
import getpass


supervisor_content = """[program:heartbeat]
command={py_cmd} -m heartbeat_server --serve
user={user}
autostart=true
autorestart=true
directory={location}
stderr_logfile={log_location}
"""


def get_supervisor_confdir():
    supervisor_paths = ('/etc/supervisor/conf.d',
                        '/etc/supervisor.d/',
                        '/etc/supervisord/conf.d',
                        '/etc/supervisord.d')
    for path in supervisor_paths:
        if os.path.exists(path):
            return path
    raise Exception("Supervisor installation not found")


def setup_supervisor(user=None,
                     config_filename='supervisor.conf',
                     log_file='app.log'):
    py_cmd = sys.executable
    if "/env/bin/" not in py_cmd:
        raise Exception("Please run using a "
                        "python executable from a "
                        "virtual env")

    location = os.getcwd()
    log_location = os.path.join(location, 'logs', log_file)

    with open(config_filename, 'w') as fhandle:
        fhandle.write(supervisor_content.format(
            py_cmd=py_cmd,
            location=location,
            user=user or getpass.getuser(),
            log_location=log_location
        ))

    supervisor_conf = os.path.join(get_supervisor_confdir(), 'heartbeat.conf')

    if not os.path.islink(supervisor_conf):
        os.symlink(os.path.abspath(config_filename), supervisor_conf)

    return config_filename
