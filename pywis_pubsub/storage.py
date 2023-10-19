###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

from abc import ABC, abstractmethod
from datetime import datetime
import logging
from pathlib import Path
import shutil
from typing import Generator, Tuple

LOGGER = logging.getLogger(__name__)


class Storage(ABC):
    # @abstractmethod
    def __init__(self, defs):
        self.type = defs.get('type')
        self.options = defs.get('options')

    @abstractmethod
    def setup(self) -> bool:
        """
        Setup harness

        :returns: `bool` of setup result
        """

        raise NotImplementedError()

    @abstractmethod
    def teardown(self) -> bool:
        """
        Teardown setup

        :returns: `bool` of setup result
        """

        raise NotImplementedError()

    @abstractmethod
    def exists(self, filename: Path) -> bool:
        """
        Verify whether data already exists

        :param filename: `Path` of storage object/file

        :returns: `bool` of whether the filepath exists in storage
        """

        raise NotImplementedError()

    @abstractmethod
    def list_content_by_date(self, basepath: str,
                             recursive: bool = False) -> Generator[Tuple[str, datetime]]:  # noqa
        """
        List storage paths starting

        :param basepath: basepath
        :param recursive: whether to list recursively (default=`False`)

        :returns: `generator` of contents
        """

        raise NotImplementedError()

    @abstractmethod
    def save(self, data: bytes, filename: Path,
             content_type: str = 'application/octet-stream') -> bool:
        """
        Save data to storage

        :param data: `bytes` of data
        :param filename: `str` of filename
        :param content_type: media type (default is `application/octet-stream`)

        :returns: `bool` of save result
        """

        raise NotImplementedError()

    @abstractmethod
    def delete(self, filename: Path) -> bool:
        """
        Deletes data from storage

        :param filename: `str` of filename

        :returns: `bool` of delete result
        """

        raise NotImplementedError()


class FileSystem(Storage):
    def setup(self) -> bool:

        basedir = Path(self.options['basedir'])
        LOGGER.debug(f'Creating directory {basedir}')
        basedir.mkdir(parents=True, exist_ok=True)

        return True

    def teardown(self) -> bool:
        LOGGER.debug(f"Deleting directory {self.options['basedir']}")
        shutil.rmtree(self.options['basedir'])

        return True

    def exists(self, filename: Path) -> bool:

        filepath = Path(self.options['basedir']) / filename

        return filepath.exists()

    def list_contents_by_date(self, basepath: str,
                              recursive: bool = False) -> Generator[Tuple[Path, datetime]]:  # noqa

        if recursive:
            func = 'rglob'
        if recursive:
            func = 'glob'

        for p in getattr(Path(basepath), func)('*'):
            if p.is_file():
                yield p, datetime.fromtimestamp(p.stat().st_mtime)

    def save(self, data: bytes, filename: Path,
             content_type: str = 'application/octet-stream') -> bool:

        filepath = Path(self.options['basedir']) / filename

        LOGGER.debug(f'Creating directory {filepath.parent}')
        filepath.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.debug(f'Saving data to {filepath}')
        with filepath.open('wb') as fh:
            fh.write(data)

        LOGGER.info(f'Data saved to {filepath}')

        return True

    def delete(self, filename: Path) -> bool:

        filepath = Path(self.options['basedir']) / filename

        LOGGER.debug(f'Deleting file {filepath}')
        filepath.unlink()

        LOGGER.info(f'Deleted file {filepath}')

        return True


class S3(Storage):

    @staticmethod
    def _get_client(self):

        import boto3

        s3_url = self.options['url']
        self.s3_bucket = self.options['bucket']

        s3_client = boto3.client('s3', endpoint_url=s3_url)

        return s3_client

    def teardown(self) -> bool:

        s3_client = self._get_client(self)

        try:
            LOGGER.debug(f'Deleting bucket {self.s3_bucket}')
            s3_client.delete_bucket(Bucket=self.s3_bucket)
        except Exception as err:
            LOGGER.error(err)
            return False

        return True

    def exists(self, filename: Path) -> bool:

        s3_client = self._get_client(self)

        try:
            s3_client.head_object(Bucket=self.s3_bucket, Key=filename)
        except Exception:
            return False

        return True

    def setup(self) -> bool:

        s3_client = self._get_client(self)

        try:
            LOGGER.debug(f'Creating bucket {self.s3_bucket}')
            s3_client.create_bucket(Bucket=self.s3_bucket)
        except Exception as err:
            LOGGER.error(err)
            return False

        return True

    def list_contents_by_date(self, basepath: str,
                              recursive: bool = False) -> Generator[Tuple[Path, datetime]]:  # noqa

        s3_client = self._get_client(self)

        objects = s3_client.list_objects(basepath)
        for obj in objects['Contents']:
            yield obj['Key'], obj['LastModified']

    def save(self, data: bytes, filename: Path,
             content_type: str = 'application/octet-stream') -> bool:

        s3_client = self._get_client(self)

        try:
            s3_client.put_object(Body=data, Bucket=self.s3_bucket,
                                 Key=filename, ContentType=content_type)
        except Exception as err:
            LOGGER.error(err)
            return False

        LOGGER.info(f'Data saved to {filename}')

        return True

    def delete(self, filename: Path) -> bool:

        s3_client = self._get_client(self)

        LOGGER.debug(f'Deleting object {filename}')

        try:
            s3_client.delete_object(Bucket=self.s3_bucket, Key=filename)
        except Exception as err:
            LOGGER.error(err)
            return False

        LOGGER.info(f'Deleted object {filename}')

        return True


STORAGES = {
    'fs': FileSystem,
    'S3': S3
}
