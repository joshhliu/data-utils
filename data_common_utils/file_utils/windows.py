"""
A module for interacting with windows file shares.
"""

import tempfile
from smb.SMBConnection import SMBConnection
import boto3


class WindowsShareClient():
    """
    simple class for interacting with windows file shares
    """
    def __init__(self,windows_username,
                 windows_password,
                 server_ip,
                 server_name=None,
                 process_name='etl-process'):
        """
        """
        server_name = server_name if server_name else server_ip
        self.conn =  SMBConnection(
                    windows_username,
                    windows_password,
                    process_name,
                    server_name,
                    use_ntlm_v2=True,
                    is_direct_tcp=True)
        self.conn.connect(server_ip, 445)
    def list_shares(self):
        """
        list shares available on a windows server
        """
        response = self.conn.listShares(timeout=30)
        share_list = [x.name for x in response]
        print(share_list)
        return share_list
    def upload_file(self, share, local_file, remote_folder, remote_file_name = None):
        """
        uploade a file to a windows share
        """
        with open(local_file, 'rb') as file_obj:
            target_file = remote_file_name if remote_file_name else local_file
            return self.conn.storeFile(share, f'{remote_folder}/{target_file}', file_obj)

    def list_files(self, share, remote_folder):
        """
        list files avalailable on a windows share and folder.
        returns a list of file names. 
        """
        response = self.conn.listPath(share, remote_folder)
        file_list = [x.filename for x in response]
        print(file_list)
        return file_list
    def close(self):
        """
        close the connection to the windows file server
        """
        self.conn.close()


class S3WindowsInteractions():
    """
    simple class for moving files from s3 
    """
    def __init__(self, windows_username, windows_password, server_ip, server_name=None):
        """
        """
        self.s3 = boto3.client('s3')
        self.win_conn = WindowsShareClient(
            windows_username=windows_username,
            windows_password=windows_password,
            server_name=server_name,
            server_ip=server_ip)
    def s3_to_windows(self, s3_bucket, s3_key, share, remote_folder, remote_file_name):
        """
        moves download tfile to windows share
        """
        with tempfile.NamedTemporaryFile(delete=False,mode='wb') as fp:
            self.s3.download_fileobj(s3_bucket, s3_key, fp)
            temp_file_name = fp.name
        self.win_conn.upload_file(
            share=share,
            local_file=temp_file_name,
            remote_folder=remote_folder,
            remote_file_name=remote_file_name)
    def windows_to_s3(self):
        """
        to be done
        """
        pass
