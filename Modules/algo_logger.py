import os
import logging
import datetime
import sys

def initiate_log_file():
    timestamp = datetime.date.today().strftime('%Y%m%d')
    path = " FULL PATH TO FOLDER WITH LOG FILES"

    if not os.path.exists(rf'{path}\{timestamp[:-2]}'):
        os.makedirs(rf'{path}\{timestamp[:-2]}')

    filename = rf'{path}\{timestamp[:-2]}\{timestamp}_{os.getlogin()}.log'

    if os.path.isfile(filename):
        suffix = 1
        while True:
            suffix += 1
            new_filename = f'{filename.split(".log")[0]}_{suffix}.log'
            if os.path.isfile(new_filename):
                continue
            else:
                filename = new_filename
                break

    return filename


def initiate_logger(run_file):
    filename = initiate_log_file()
    logging.basicConfig(filename=filename,
                        filemode='a',
                        level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(name)s - [ %(message)s ]",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(run_file)
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)

    return logger
