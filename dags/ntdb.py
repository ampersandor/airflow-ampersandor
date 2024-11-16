import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timedelta
from ftplib import FTP
import fnmatch

import requests
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
# from airflow.hooks.postgres_hook import PostgresHook
import os
import ftplib
from plugins import slack

user_pw = 'znekf'
port_num = '22'
user_id = 'dhkim4'
node_ip = '10.12.168.78'

cmd_sshpass = 'sshpass -p {0} ssh -p{1} {2}@{3} \'{4}\''
cmd_decompress_on = 'eccccho hi'
cmd_ssh_decompress = cmd_sshpass.format(user_pw, port_num, user_id, node_ip, cmd_decompress_on)

@task
def make_download_file_list(filelist_path, ncbi_ftp_url):
    """
    Make text file which has ntDB file list.

    Args:
        download_info_dict (dict) : dictionary about information for making downloading command.
    """
    
    # Connect to the FTP server
    ftp = FTP(ncbi_ftp_url)
    ftp.login()
    
    # Navigate to the directory you want to list
    ftp.cwd('/blast/db')

    # List files
    file_list = ftp.nlst()
    
    # Filter files based on the pattern
    filtered_files = fnmatch.filter(file_list, "nt.*.tar.gz")

    # Make the list of files
    with open(filelist_path, 'w') as f:
        for filename in filtered_files:
            print(filename)
            f.write('/blast/db/' + filename + '\n')
        f.write('/blast/db/taxdb.tar.gz')
    # Close the FTP connection
    ftp.quit()

@task
def broadcast_file_list(to_node: int, node_ip: str, filelist_path: str):
    """
    Broadcast file list to other nodes.

    Args:
        to_node (list) : list of nodes to broadcast file list.
    """
    user_id = 'bioinfo'
    user_passwd = '7890uiop'
    port_num = '3030'
    download_list_path = '/BiO{}/Database/ntdb_version/list.txt'.format(to_node)

    broadcast_file_cmd = f"""
        scp -P {port_num} 
        {filelist_path} 
        {user_id}@{node_ip}:{download_list_path}
    """
    cmd = f"sshpass -p {user_passwd} '{broadcast_file_cmd}"

    code = os.system(cmd)
    if code != 0:
        raise Exception("Broadcasting file list is failed.")
           
    logging.info("Broadcasting file list is done.")
    
@task
def download_ntdb_in_a_node(to_node: int, node_ip: str, timestamp: str):

    user_id = 'bioinfo'
    user_passwd = '7890uiop'
    port_num = '3030'
    aspera_bin_path = '/opt/bioinfo/.aspera/connect/bin/ascp'
    download_filelist_path = '/BiO{}/Database/ntdb_version/list.txt'.format(to_node)
    ssh_key = '/opt/bioinfo/.aspera/connect/etc/asperaweb_id_dsa.openssh'
    ntdb_download_path = '/BiO{}/Database/ntdb_version/{}/blast'.format(to_node, timestamp)

    download_ntdb_cmd = f"""
        {aspera_bin_path}
        --file-list={download_filelist_path}
        -i {ssh_key}
        -T
        -l1200M
        --mode=recv
        --user=anonftp
        --host=ftp.ncbi.nlm.nih.gov
        {ntdb_download_path}
    """
    cmd = f"sshpass -p {user_passwd} ssh -p{port_num} {user_id}@{node_ip} '{download_ntdb_cmd}'"

@task
def unzip_ntdb_in_a_node(to_node: int, node_ip: str, timestamp: str):

    user_id = 'bioinfo'
    user_passwd = '7890uiop'
    port_num = '3030'
    ntdb_download_path = '/BiO{}/Database/ntdb_version/{}/blast'.format(to_node, timestamp)

    download_ntdb_cmd = f"""
        {aspera_bin_path}
        --file-list={download_filelist_path}
        -i {ssh_key}
        -T
        -l1200M
        --mode=recv
        --user=anonftp
        --host=ftp.ncbi.nlm.nih.gov
        {ntdb_download_path}
    """
    cmd = f"sshpass -p {user_passwd} ssh -p{port_num} {user_id}@{node_ip} '{download_ntdb_cmd}'"



with DAG(
        dag_id = f"NTDB_Update",
        start_date=datetime(2023, 8, 25), 
        schedule= "0 0 * * 5",  # every week (saturday 00:00)
        max_active_runs=1,
        tags=['NTDB', 'weekly', "Extract"],
        catchup=False,
        default_args={
            "retries": 0,
            "retry_delay": timedelta(minutes=3),
            "on_failure_callback" : slack.on_failure_callback,
            "on_success_callback" : slack.on_success_callback
        }
    ) as dag:
        timestamp = datetime.now().strftime('%Y%m%d')

        make_download_file_list("./list.txt", "ftp.ncbi.nlm.nih.gov")
        broadcast_file_list(1, "10.10.101.33", "./list.txt")
        download_ntdb_in_a_node(1, "10.10.101.33", timestamp)
        