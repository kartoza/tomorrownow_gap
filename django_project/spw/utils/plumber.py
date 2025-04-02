# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Plumber functions.
"""
import logging
import traceback
import os
import time
import csv
import subprocess
import requests
from uuid import uuid4

from core.settings.utils import absolute_path
from spw.models import RModel, RModelOutput
from spw.utils.process import (
    write_pidfile,
    kill_process_by_pid
)


logger = logging.getLogger(__name__)
PLUMBER_PORT = os.getenv('PLUMBER_PORT', 8282)


def plumber_health_check(port = PLUMBER_PORT, max_retry=5):
    """
    Check whether API is up and running.

    This will be called from worker.
    :param max_retry: maximum retry of checking
    :return: True if successful number of check is less than max_retry
    """
    request_url = f'http://0.0.0.0:{port}/statistical/echo'
    retry = 0
    req = None
    time.sleep(1)
    while (req is None or req.status_code != 200) and retry < max_retry:
        try:
            req = requests.get(request_url)
            if req.status_code != 200:
                time.sleep(2)
            else:
                break
        except Exception as ex:  # noqa
            logger.error(ex)
            time.sleep(2)
        retry += 1
    if retry < max_retry:
        logger.info('Plumber API is up and running!')
    return retry < max_retry


def spawn_r_plumber(index = None, port = PLUMBER_PORT):
    """Run a Plumber API server."""
    command_list = (
        [
            'R',
            '-e',
            (
                "pr <- plumber::plumb("
                f"'/home/web/plumber_data/plumber.R'); "
                f"args <- list(host = '0.0.0.0', port = {port}); "
                "do.call(pr$run, args)"
            )
        ]
    )
    logger.info('Starting plumber API')
    process = None
    try:
        # redirect stdout and stderr
        with open('/proc/1/fd/1', 'w') as fd:
            process = subprocess.Popen(
                command_list,
                stdout=fd,
                stderr=fd
            )
        # sleep for 10 seconds to wait the API is up
        time.sleep(10)
        # we can also use polling to echo endpoint for health check
        plumber_health_check(port=port)
        # write process pid to /tmp/
        write_pidfile(
            process.pid,
            '/tmp/plumber.pid' if index is None else
            f'/tmp/plumber_{index}.pid'
        )
    except Exception as ex:  # noqa
        logger.error(ex)
        logger.error(traceback.format_exc())
        if process:
            process.terminate()
            process = None
    return process


def kill_r_plumber_process(index = None):
    """Kill plumber process by PID stored in file."""
    pid_path = os.path.join(
        '/',
        'tmp',
        'plumber.pid' if index is None else f'plumber_{index}.pid'
    )
    kill_process_by_pid(pid_path)


def execute_spw_model(
        data_url: str, filename: str, lat: float = 0.0, lon: float = 0.0,
        place_name: str = None, port = PLUMBER_PORT):
    """Execute SPW model given the data_filepath.

    :param data_url: CSV file URL containing the data.
    :type data_url: str
    :param filename: CSV filename
    :type filename: str
    :param lat: Latitude of data query
    :type lat: float
    :param lon: Longitude of data query
    :type lon: float
    :param place_name: Place name (optional)
    :type place_name: str
    :return: dictionary of spw model output
    :rtype: dict
    """
    request_url = f'http://plumber:{port}/spw/generic'
    data = {
        'data_url': data_url,
        'filename': filename,
        'lat': lat,
        'lon': lon,
        'place_name': place_name if place_name else 'Default'
    }
    response = requests.post(request_url, data=data)
    content_type = response.headers['Content-Type']
    error = None
    if content_type == 'application/json':
        if response.status_code == 200:
            return True, response.json()
        else:
            logger.error(
                f'Plumber error response: {str(response.json())}')
            error = response.json()
    else:
        logger.error(f'Invalid response content type: {content_type}')
        error = f'Invalid response content type: {content_type}'
    return False, error


def write_plumber_file(file_path = None):
    """Write R codes to plumber.R."""
    r_file_path = file_path if file_path else os.path.join(
        '/home/web/plumber_data',
        'plumber.R'
    )
    template_file = absolute_path(
        'spw', 'utils', 'plumber_template.R'
    )
    with open(template_file, 'r') as f:
        lines = f.readlines()
    model = RModel.objects.order_by('-version').first()
    if model:
        lines.append('\n')
        lines.append('#* Generic Model\n')
        lines.append('#* @post /spw/generic\n')

        lines.append('function(data_url, filename, lat, lon, place_name) {\n')
        lines.append(f'  metadata <- list(version={model.version}, '
                     'lat=lat, lon=lon, place_name=place_name, '
                     'generated_on=format(Sys.time(), '
                     '"%Y-%m-%d %H:%M:%S %Z"))\n')
        lines.append('  time_start <- Sys.time()\n')
        lines.append(
            '  data_filename <- paste(\'/tmp/\', filename, sep="")\n')
        lines.append('  download.file(data_url, data_filename)\n')
        code_lines = model.code.splitlines()
        for code in code_lines:
            lines.append(f'  {code}\n')
        lines.append('  metadata[\'total_execution_time\'] '
                     '<- Sys.time() - time_start\n')
        lines.append('unlink(data_filename)\n')
        # add output
        model_outputs = RModelOutput.objects.filter(
            model=model
        )
        output_list = ['metadata=metadata']
        for output in model_outputs:
            output_list.append(f'{output.type}={output.variable_name}')
        output_list_str = ','.join(output_list)
        lines.append(
            f'  list({output_list_str})\n'
        )
        lines.append('}\n')
    with open(r_file_path, 'w') as f:
        for line in lines:
            f.write(line)
    return r_file_path


def write_plumber_data(headers, csv_data, dir_path='/home/web/plumber_data'):
    """
    Write csv data to file in plumber_data.

    :param headers: list of header name
    :param csv_data: list of row
    :return: file path of exported csv
    """
    r_data_path = os.path.join(
        dir_path,
        f'{str(uuid4())}.csv'
    )
    with open(r_data_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(headers)
        writer.writerows(csv_data)
    return r_data_path


def remove_plumber_data(data_filepath):
    """
    Remove csv data file.

    :param data_filepath: filepath to the csv file
    """
    try:
        if os.path.exists(data_filepath):
            os.remove(data_filepath)
    except Exception as ex:
        logger.error(ex)
