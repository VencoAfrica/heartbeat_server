import os


def create_logs_folder(logs_folder = 'logs'):
    if os.path.exists(logs_folder):
        if not os.path.isdir(logs_folder):
            raise Exception("A file with name %s already exists. "
                            "Please run command in different folder"
                            % logs_folder)
        return False
    else:
        os.mkdir(logs_folder)
        return True
