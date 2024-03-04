import logging as log
from PIL import Image
from enum import Enum
from io import BytesIO
from uuid import uuid4
from urllib.parse import (
  urlencode,
  urlparse,
  urlunparse,
  unquote,
  ParseResult
)
import enlighten
from botocore.exceptions import ClientError
import os
import boto3
from typing import Any
import json
import re
from s3fs import S3FileSystem

from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import Bucket, Object, S3ServiceResource

from trz_py_utils import fmt
from trz_py_utils.fmt import sizeof_fmt
from trz_py_utils.file import BadLine


# log = logging.getLogger(__name__)


REGION = os.environ.get("AWS_REGION", "us-east-2")
S3_RESOURCE: S3ServiceResource = boto3.resource("s3", region_name=REGION)


class S3ImageFormat(Enum):
    JPEG = "jpeg"
    PNG = "png"


def upload_img(image: Image, bucket_name: str,
               key: str, s3_resource: S3ServiceResource,
               format=S3ImageFormat.JPEG):
    """save pillow Image locally then upload to S3 with ContentType

    Args:
        image (PIL.Image): image from pillow module
        bucket_name (str): name of s3 bucket to save image to
        key (str): name of the s3 "file" to save image
        s3_resource (S3ServiceResource): _description_
        format (S3ImageFormat, optional): either png or jpeg. Defaults to
            S3ImageFormat.JPEG.

    Returns:
        _type_: _description_

    Example:
        >>> from trz_py_utils.s3 import upload_img
        >>> from PIL import Image
        >>> from boto3 import resource
        >>> s3_r = resource("s3", region_name="us-east-2")
        >>> image = Image.new('RGB', (10, 10))
        >>> upload_img(image, "trz-s3-test", "s3-upload-test.png", s3_r)
        s3.Object(bucket_name='trz-s3-test', key='s3-upload-test.jpg')
        >>> image = Image.open("tests/images/jpeg_image.jpg")
        >>> upload_img(image, "trz-s3-test", "s3-upload-test.jpg", s3_r)
        s3.Object(bucket_name='trz-s3-test', key='s3-upload-test.jpg')
    """
    log.info(f"uploading image '{key}' as {format.value}...")
    bytes_img = BytesIO()
    image.save(bytes_img, format=format.value)
    if format is S3ImageFormat.JPEG:
        ext = ".jpg"
        type = "image/jpeg"
    elif format is S3ImageFormat.PNG:
        ext = ".png"
        type = "image/png"
    # strip off ".png" extension
    log.info(f"stripping extension '{key.split('.')[-1]}' off '{key}'...")
    name_no_ext = ".".join(key.split('.')[:-1])
    bucket: Bucket = s3_resource.Bucket(bucket_name)
    obj: Object = bucket.put_object(
        Key=name_no_ext+ext,
        Body=bytes_img.getvalue(),
        ContentType=type)
    log.info(f"uploaded '{obj.bucket_name}/{obj.key}'.")

    return obj


def upload_obj(filepath: str, s3_obj: Object = None,
               bucket: str = None, key: str = None, **kwargs):
    """Uploads a local file to S3

    Args:
        filepath (str): path to the local file
        s3_obj (Object): boto3.resource.Object with bucket and key

    Example:
        >>> from trz_py_utils.s3 import upload_obj
        >>> key = "s3-upload-test2.txt"
        >>> filepath = f"/tmp/{key}"
        >>> with open(filepath, "w") as file:
        ...     _ = file.write("hello world!")
        >>> upload_obj(filepath, bucket="trz-s3-test", key=key)

    Example:
        >>> from trz_py_utils.s3 import upload_obj
        >>> from boto3 import resource
        >>> s3_r = resource("s3", region_name="us-east-2")
        >>> bucket = "trz-s3-test"
        >>> key = "s3-upload-test.txt"
        >>> filepath = f"/tmp/{key}"
        >>> with open(filepath, "w") as file:
        ...     _ = file.write("hello world!")
        >>> s3_obj = s3_r.Object("trz-s3-test", key)
        >>> upload_obj(filepath, s3_obj)

    Example:
        >>> from trz_py_utils.s3 import upload_obj
        >>> key = "s3-upload-test3.txt"
        >>> filepath = f"/tmp/{key}"
        >>> with open(filepath, "w") as file:
        ...     _ = file.write("hello world!")
        >>> error = upload_obj(filepath,
        ...                    bucket="trz-s3-test",
        ...                    key=key,
        ...                    ExtraArgs=())
        >>> error is not None
        True
    """
    bucket = bucket or s3_obj.bucket_name
    key = key or s3_obj.key

    s3_path = f"{bucket}/{key}"
    log.info(f"uploading local file to S3:\n{filepath}\n{s3_path}")
    log.info(f"with kwargs:\n{json.dumps(kwargs, indent=4)}")

    s3_obj = S3_RESOURCE.Object(bucket_name=bucket, key=key)

    # Use enlighten to create a progress bar
    opts = {
        "total": os.path.getsize(filepath),
        "unit": 'B',
        "unit_scale": True,
        "unit_divisor": 1024,
        "desc": "uploading...",
    }
    with enlighten.get_manager().counter(**opts) as pbar:
        try:
            with open(filepath, 'rb') as file:
                s3_obj.upload_fileobj(file,
                                      Callback=lambda ch: pbar.update(ch),
                                      **kwargs)
        except (ClientError, TypeError) as e:
            log.error(f"error uploading: {e}")
            return e
    log.info(f"uploaded '{bucket}/{key}'.")

    return None


def download_obj(s3_obj: Object, fp=f"/tmp/{uuid4()}"):
    """_summary_

    Args:
        s3_obj (Object): object to download
        fp (_type_, optional): filepath. Defaults to f"/tmp/{uuid4()}".

    Returns:
        _type_: _description_

    Example:
        >>> from trz_py_utils.s3 import download_obj
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> download_obj(s3_obj)
        '/tmp/...'
    """
    log.info("downloading S3 object...")
    log.info(f"'s3://{s3_obj.bucket_name}/{s3_obj.key}' -> {fp}")
    log.debug("calling s3_obj.get() for debug purposes...")
    log.debug(s3_obj.get())
    s3_client: S3Client = s3_obj.meta.client
    with open(fp, 'wb') as f:
        s3_client.download_fileobj(s3_obj.bucket_name, s3_obj.key, f)
    log.info("done.")

    return fp


def download_object(s3_obj: Object, fp: str = f"/tmp/{uuid4()}"):
    """Download an object from S3 and show progress bar.

    Args:
        s3_obj (Object): object to download
        fp (str, optional): filepath to save to. note: lambda functions
        can only save to /tmp. Defaults to f"/tmp/{uuid4()}".

    Returns:
        str: path to downloaded file

    Example:
        >>> from trz_py_utils.s3 import download_object
        >>> import boto3
        >>> s3_resource = boto3.resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> download_object(s3_obj)
        '/tmp/...'

    Example:
        >>> from trz_py_utils.s3 import download_object
        >>> import boto3
        >>> s3_resource = boto3.resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> download_object(s3_obj, "/tmp/myfile")
        '/tmp/myfile'
    """
    s3_client = s3_obj.meta.client
    # Get the size of the S3 object for progress tracking
    log.info("getting file size from ContentLength header...")
    response = s3_client.head_object(Bucket=s3_obj.bucket_name,
                                     Key=s3_obj.key)
    total_size = response['ContentLength']
    log.info(f"file size: {sizeof_fmt(total_size)}")

    # Use enlighten to create a progress bar
    opts = {
        "total": total_size,
        "unit": 'B',
        "unit_scale": True,
        "unit_divisor": 1024,
        "desc": "downloading...",
    }
    log.info(f"downloading '{make_console_url(s3_obj)}'...")
    with enlighten.get_manager().counter(**opts) as pbar:
        # Open a file-like object to write the S3 object contents
        with open(fp, 'wb') as f:
            s3_client.download_fileobj(
                s3_obj.bucket_name,
                s3_obj.key,
                f,
                Callback=lambda chunk: pbar.update(chunk))

    return fp


def make_s3_url(s3_obj: Object):
    """makes a URL which loads directly in any browser regardless of aws auth.
    eg. 'https://trz-rekognition-dev.s3.us-east-2.amazonaws.com/figure-65.png'

    Args:
        s3_obj (Object): the s3 object to make a link for

    Returns:
        str: pre-signed URL string

    Example:
        >>> from trz_py_utils.s3 import make_s3_url
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> make_s3_url(s3_obj)
        'https://trz-s3-test.s3.us-east-2.amazonaws.com/figure-65.png'
    """
    url = urlparse(s3_obj.meta.client.meta.endpoint_url)
    url = url._replace(netloc=f"{s3_obj.bucket_name}.{url.netloc}")
    url = url._replace(path=f"/{s3_obj.key}")
    return urlunparse(url)


def make_s3_presigned_url(s3_obj: Object, expiry=604800):
    """Makes a pre-signed URL for an S3 object. Expires in 7 days.

    Args:
        s3_obj (Object): _description_
        expiry (int, optional): _description_. Defaults to 604800.

    Returns:
        str: web link to load an object in the browser.

    Example:
        >>> from trz_py_utils.s3 import make_s3_presigned_url
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> make_s3_presigned_url(s3_obj)
        'https://trz-s3-test.s3.amazonaws.com/figure-65.png?X-Amz-Algorithm...
    """
    s3_client: S3Client = s3_obj.meta.client
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': s3_obj.bucket_name, 'Key': s3_obj.key},
        ExpiresIn=604800  # 7 days
    )


def make_console_url(s3_obj: Object):
    """Returns a url string pointing to object in AWS Console.
    eg. 'https://s3.console.aws.amazon.com/s3/object/trz-rekognition-dev?region=us-east-2&prefix=figure-65.png'

    Args:
        s3_obj (Object): the object in s3 to return a link for.

    Returns:
        str: the web link pointing to object in AWS Console.

    Example:
        >>> from trz_py_utils.s3 import make_console_url
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> make_console_url(s3_obj)
        'https://s3.console.aws.amazon.com/s3/object/trz-s3-test?...'
    """  # noqa
    return ParseResult(
        scheme="https",
        netloc="s3.console.aws.amazon.com",
        path=f"s3/object/{s3_obj.bucket_name}",
        params="",
        query=urlencode({
            "region": s3_obj.meta.client.meta.region_name,
            "prefix": s3_obj.key
        }),
        fragment=""
    ).geturl()


def test_if_object_exists(s3_obj: Object) -> tuple[Exception, int]:
    """Return ValueError and status code if object doesn't exist.
    Otherwise do nothing.

    Args:
        s3_obj (Object): S3 object to test for existence.

    Raises:
        e: Exception

    Returns:
        tuple[Exception, int]: ValueError and status code

    Example:
        >>> from trz_py_utils.s3 import test_if_object_exists
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> error, code = test_if_object_exists(s3_obj)
        >>> code
        200

    Example:
        >>> from trz_py_utils.s3 import test_if_object_exists
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "fasdlkjasd")
        >>> error, code = test_if_object_exists(s3_obj)
        >>> error
        ValueError("403/404: object doesn't exists or you don't have access...
    """
    try:
        s3_obj.load()
        return None, 200
    except ClientError as e:
        if int(e.response['Error']['Code']) in (403, 404):
            msg = "403/404: object doesn't exists or you don't have access"
            msg += f" to '{s3_obj.bucket_name}/{s3_obj.key}'"
            return ValueError(msg), e.response['Error']['Code']
        else:
            raise e


def write_text_to_obj(text: str, s3_obj: Object = None,
                      bucket: str = None, key: str = None, **kwargs):
    """Write a string to a new S3 object. For kwargs, see S3 object attributes:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/object/index.html#attributes

    Args:
        text (str): String to write as object
        s3_obj (Object, optional): Existing object. This or bucket+key
        bucket (str, optional): bucket to write to. This and `key`, or `s3_obj`.
        key (str, optional): key for the object. This and `bucket`, or `s3_obj`.


    Example:
        >>> from trz_py_utils.s3 import write_text_to_obj
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "write_text_to_obj.txt")
        >>> error, s3_obj = write_text_to_obj("hello world!", s3_obj)
        >>> s3_obj
        s3.Object(bucket_name='trz-s3-test', key='write_text_to_obj.txt')
        >>> error, code = test_if_object_exists(s3_obj)
        >>> code
        200

    Example:
        >>> from trz_py_utils.s3 import write_text_to_obj
        >>> error, s3_obj = write_text_to_obj(text="")
        >>> error
        ValueError('trzpyutils.s3.write_text_to_obj: specify either bucket and key, or an existing s3.Object')
        >>> error, s3_obj = write_text_to_obj(s3_obj="asd", bucket="lkas", text="")
        >>> error
        ValueError('trzpyutils.s3.write_text_to_obj: specify either bucket and key, or an s3.Object, but not both')
    """  # noqa
    log.info("writing object as file then uploading...")
    log.info(f"with kwargs:\n{json.dumps(kwargs, indent=4)}")
    msg = "trzpyutils.s3.write_text_to_obj: "
    if not s3_obj and not (bucket and key):
        msg += "specify either bucket and key, or an existing s3.Object"
        return ValueError(msg), None
    elif (s3_obj and bucket) or (s3_obj and key):
        msg += "specify either bucket and key, or an s3.Object, but not both"
        return ValueError(msg), None
    elif bucket and key:
        s3_obj = S3_RESOURCE.Object(bucket_name=bucket, key=key)

    # write the file to temp dir then upload by path
    try:
        filepath = f"/tmp/{uuid4()}"
        with open(filepath, "w") as file:
            file.write(text)
    except Exception as e:
        return e, s3_obj

    # upload local file
    try:
        upload_obj(filepath, s3_obj, **kwargs)
    except Exception as e:
        return e, s3_obj

    return None, s3_obj


def get_obj_tags(s3_obj: Object):
    """Retrieve TagSet from s3 object and convert list of dict to dict.

    Args:
        s3_obj (Object): _description_

    Returns:
        dict[str, str]: tags!
    """
    tag_set: list[dict[str, str]] = s3_obj.meta.client.get_object_tagging(
        Bucket=s3_obj.bucket_name,
        Key=s3_obj.key, ).get("TagSet", {})

    return {t["Key"]: t["Value"] for t in tag_set}


def make_tags(tags: dict[str, Any]):
    """Encode dictionary of tags for an s3 object's ExtraArgs

    Args:
        tags (dict[str, Any]): object whose keys are tags and values or values

    Returns:
        dict[str, str]: use as ExtraArgs for s3 object upload

    Example:
        >>> from trz_py_utils.s3 import make_tags, get_obj_tags
        >>> tags = make_tags({"header0": "mycol1", "header1": "mycol2"})
        >>> tags
        {'Tagging': 'header0=mycol1&header1=mycol2'}
        >>> error, s3_obj = write_text_to_obj(
        ...     text="hellow world!",
        ...     bucket='trz-s3-test',
        ...     key='make_tags',
        ...     ExtraArgs=tags)
        >>> tags = get_obj_tags(s3_obj)
        >>> tags["header0"]
        'mycol1'
    """
    return {"Tagging": urlencode(tags)}


def get_size(s3_obj: Object):
    """get the size of the S3 object.

    Args:
        s3_obj (Object): _description_

    Returns:
        tuple(Exception, int): error and size in bytes

    Example:
        >>> from trz_py_utils import s3,fmt
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "write_text_to_obj.txt")
        >>> error, s3_obj = s3.write_text_to_obj("hello world!", s3_obj)
        >>> error, size_in_bytes = s3.get_size(s3_obj)
        >>> size_in_bytes
        12
        >>> fmt.sizeof_fmt(size_in_bytes)
        '12.0B'

    Example:
        >>> from trz_py_utils import s3,fmt
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "nonexistent.txt")
        >>> error, size_in_bytes = s3.get_size(s3_obj)
        >>> error
        ClientError('An error occurred (404) when calling the HeadObject operation: Not Found')
    """  # noqa
    client = s3_obj.meta.client
    bucket = s3_obj.bucket_name
    key = s3_obj.key
    try:
        response = client.head_object(Bucket=bucket, Key=key)
        size_in_bytes = response['ContentLength']
        return None, size_in_bytes
    except Exception as error:
        return error, None


def read_object(s3_obj: Object = None, bucket: str = None, key: str = None):
    """Get contents of s3 object after downloading it.

    Args:
        s3_obj (Object, optional): s3 object to read if not bucket+key
        bucket (str, optional): s3 object bucket (must pass key arg)
        key (str, optional): s3 object key (must pass bucket arg)

    Returns:
        Exception, str: error (if any) and string contents

    Example:
        >>> from trz_py_utils import s3
        >>> bucket = "trz-s3-test"
        >>> key = "read-test1.txt"
        >>> text = "hello world!"
        >>> err, obj = s3.write_text_to_obj(bucket=bucket, key=key, text=text)
        >>> err
        >>> error, text = s3.read_object(bucket=bucket, key=key)
        >>> error
        >>> text
        'hello world!'
        >>> error, text = s3.read_object(obj)
        >>> text
        'hello world!'
    """
    error: Exception = None
    contents: str = None
    if bucket and key:
        s3_obj: Object = S3_RESOURCE.Object(bucket_name=bucket, key=key)
    try:
        with open(download_object(s3_obj), "r") as f:
            contents = f.read()
    except OSError as e:
        error = e

    return error, contents


def object_iter_lines(s3_obj: Object = None, bucket: str = None,
                      key: str = None):
    """return lines of s3 object via Generator

    Args:
        s3_obj (Object, optional): _description_. Defaults to None.
        bucket (str, optional): _description_. Defaults to None.
        key (str, optional): _description_. Defaults to None.

    Returns:
        Exception, Generator: error (or None) and lines_iter (or None)

        Example:
            >>> from trz_py_utils import s3
            >>> bucket = "trz-s3-test"
            >>> key = "object_iter_lines.txt"
            >>> text = "hello world!\\n" * 1024
            >>> _, _ = s3.write_text_to_obj(text=text, bucket=bucket, key=key)
            >>> error, iter = s3.object_iter_lines(bucket=bucket, key=key)
            >>> next(iter)
            b'hello world!'
    """
    key = key or s3_obj.key
    bucket = bucket or s3_obj.bucket_name
    s3_obj = s3_obj or S3_RESOURCE.Object(bucket_name=bucket, key=key)
    # path = f"s3://{bucket}/{key}"
    try:
        client_obj = s3_obj.meta.client.get_object(Bucket=bucket, Key=key)
        return None, client_obj['Body'].iter_lines()
    except Exception as e:
        return e, None


def get_object(bucket: str, key: str):
    return S3_RESOURCE.Object(bucket, key)


def iter_lines_progress(s3_obj: Object = None, size_in_bytes: int = 0,
                        chunk_size=1024, keepends=False):
    """Yield lines from s3 object with progress bar (speed)

    This is achieved by reading chunk of bytes (of size chunk_size) at a
    time from the raw stream, and then yielding lines from there.

    Example:
        >>> from trz_py_utils import s3
        >>> bucket = "trz-s3-test"
        >>> key = "s3-cleaner-rewrite.txt"
        >>> lines = "".join([
        ...     "Col1~Col2~Col3\\n",
        ...     "NULL~NULL~\\n",
        ...     "09BB¿~NY~1G\\n",
        ...     "good~good~good\\n",
        ... ])
        >>> for l in lines:
        ...     _ = s3.write_text_to_obj(text=lines, bucket=bucket, key=key)
        >>> next(s3.iter_lines_progress(s3.get_object(bucket, key))).decode()
        'Col1~Col2~Col3'
    """
    pending = b''

    opts = {
        "total": size_in_bytes,
        "unit": 'MB',
        "unit_scale": True,
        "unit_divisor": 1024*1024*1024,
        "leave": True,  # the progress bar in console after completion
        "desc": "downloading...",
    }
    with enlighten.get_manager().counter(**opts) as pbar:
        client = s3_obj.meta.client
        obj = client.get_object(Bucket=s3_obj.bucket_name, Key=s3_obj.key)
        for chunk in obj["Body"].iter_chunks(chunk_size):
            pbar.update(chunk_size)
            lines = (pending + chunk).splitlines(True)
            for line in lines[:-1]:
                yield line.splitlines(keepends)[0]
            pending = lines[-1]
        if pending:
            yield pending.splitlines(keepends)[0]


class S3Cleaner:
    def __init__(self, s3_obj: Object, delim: str,
                 null_handler="drop",  # drop line or 'replace' with nothing
                 is_enforce_column_count=True,
                 reject_line_regex=[r"[^\x00-\x7F]"],
                 encoding="ascii",
                 ):
        self.s3fs = S3FileSystem(anon=False)
        self._lines: list[str] = []
        self._text: bytes = b""
        self.bad_lines: list[str] = []
        self.num_good = -1
        self.num_bad = -1
        self.bucket = s3_obj.bucket_name
        self.key = s3_obj.key
        self.obj = s3_obj
        self._i = 0
        self.delim = delim
        self.encoding = encoding
        self.path = f"s3://{self.bucket}/{self.key}"
        self._regexes = reject_line_regex
        self._set_headers()
        self._null_handler(null_handler, delim)
        self._column_count_enforcer(is_enforce_column_count, delim)
        self.set_regex(fmt.unique(self._regexes))

        log.info("setting default regex to:")
        log.info(f"\t{self.regex}")

    def _null_handler(self, null_handler: str, delim: str):
        # handle NULL
        self.null_handler = null_handler
        if null_handler == "drop":
            log.info("will drop lines if a column value is 'NULL'")
            # just drop the line by matching regex
            self._regexes += [
                f"NULL{delim}",
                f"{delim}NULL"
            ]
        elif null_handler == "replace":
            self._find: bytes = f"NULL{self.delim}|{self.delim}NULL".encode()
            self._replace: bytes = self.delim.encode()

    def _column_count_enforcer(self, is_enforce_column_count: bool,
                               delim: str):
        # handle lines with incorrect number of delimiters
        if is_enforce_column_count:
            log.info("enforcing column count via regex")
            n = len(self.headers)
            d = delim
            self._regexes += [
                # more or less '\t' than in header row
                rf"^(?:[^{d}]*{d}){{{n},}}[^{d}]*$",
                rf"^(?:[^{d}]*{d}){{0,{n-2}}}[^{d}]*$",
            ]

    def set_regex(self, patterns_reject: list[str] = []):
        self.regex = "|".join(patterns_reject).encode()
        log.info(f"setting regex to:\n\t{self.regex}")

    def _set_headers(self):
        line = next(iter_lines_progress(self.obj)).decode()
        self.headers = line.split(self.delim)
        self._i += 1
        log.info("found headers:")
        log.info(f"\t{fmt.dumps(self.headers)}")

    def _add_bad_line(self, **kwargs):
        bl = BadLine(path=self.path,
                     line_no=self._i,
                     delimiter=self.delim,
                     **kwargs)
        if "line" in kwargs:
            bl.print()
        self.bad_lines.append(bl)

    def _replace_null(self):
        if self.null_handler == "replace":
            self._text = re.sub(self._find, self._replace, self._text)

    def _write(self, key_out: str):
        log.info(f"writing s3 chunk (line {self._i})...")
        # write a chunk at a time
        with self.s3fs.open(f"{self.bucket}/{key_out}", "wb") as s3fs_out:
            self._replace_null()
            s3fs_out.write(self._text)
            self._text = b""

    def _parse_line(self, line):
        try:
            # turn binary lines iterator response into a string
            string = line.decode(self.encoding)
            # looking for regex matches to disqualify this line
            bad_matches = [next(re.finditer(self.regex, line))]
            self._add_bad_line(line=string, re_matches=bad_matches)
        except UnicodeDecodeError as e:
            # we don't have line string available
            log.info(f"bad line :( i={self._i}: {e}")
            self._add_bad_line(error=e)
        except StopIteration:
            # accept this line if no decode error and no regex match
            self._text += line

    def rewrite(self, bucket: str, key: str, size_in_bytes: int = None,
                write_chunk_b=1024*1024):
        """Reads a CSV from s3, filters out lines, and uploads

        Args:
            size_in_bytes (int, optional): _description_. Defaults to None.
            write_chunks (int, optional): write every n bytes. Defaults to 1MB

        Example:
            >>> from trz_py_utils import s3
            >>> bucket = "trz-s3-test"
            >>> key = "s3-cleaner-rewrite.txt"
            >>> text = "\\n".join([
            ...     "Col1~Col2~Col3",
            ...     "NULL~NULL~",
            ...     "09BB¿~NY~1G",
            ...     "good~good~good",
            ... ])
            >>> _ = s3.write_text_to_obj(text=text, bucket=bucket, key=key)
            >>> s3c = s3.S3Cleaner(s3.get_object(bucket, key), delim="~")
            >>> s3c.regex
            b'[^\\\\x00-\\\\x7F]|NULL~|~NULL|^(?:[^~]*~){3,}[^~]*$|^(?:[^~]*~){0,1}[^~]*$'
            >>> s3c.headers
            ['Col1', 'Col2', 'Col3']
            >>> s3c.rewrite(bucket, "s3-rewrite.txt")
            >>> error, lines = s3.object_iter_lines(bucket=bucket,
            ...                                     key="s3-rewrite.txt")
            >>> error
            >>> [line for line in lines]
            [b'Col1~Col2~Col3', b'good~good~good']
        """
        if not size_in_bytes:
            error, size_in_bytes = get_size(self.obj)
            if error:
                raise error

        # read object line by line
        for binary_line in iter_lines_progress(self.obj, size_in_bytes,
                                               keepends=True):
            # only write lines if no regex match
            self._i += 1
            self._parse_line(binary_line)

            # write every megabyte (1024*1024)
            if self._i % (write_chunk_b) == 0:
                self._write(key_out=key)
        # write the remainder
        if self._text:
            self._write(key_out=key)

        self.num_bad = len(self.bad_lines)
        self.num_good = self._i


class S3Object:
    def __init__(self,
                 bucket: str = None,
                 key: str = None,
                 console_url: str = None,
                 s3_uri: str = None,
                 obj: Object = None):
        """Translate avarious forms of representing an s3 object.

        s3_uri eg. s3://bucket/path/to/key.json
        s3_uri eg. bucket/key.json
        s3_uri eg. https://trz-s3-test.s3.us-east-2.amazonaws.com/figure-65.png
        s3_uri eg. https://trz-s3-test.s3.amazonaws.com/figure-65.png?X-Amz-Algorithm...
        console_url eg. https://s3.console.aws.amazon.com/s3/object/my-bucket?region=us-east-2&bucketType=general&prefix=key.json

        Args:
            bucket (str, optional): _description_. Defaults to None.
            key (str, optional): _description_. Defaults to None.
            console_url (str, optional): _description_. Defaults to None.
            s3_uri (str, optional): _description_. Defaults to None.

        Example:
            >>> from trz_py_utils.s3 import S3Object
            >>> s3_obj = S3Object("b", "k")
            >>> s3_obj.s3_uri
            's3://b/k'

        Example:
            >>> from trz_py_utils.s3 import S3Object
            >>> url = "https://s3.console.aws.amazon.com/s3/object/my-bucket?region=us-east-2&bucketType=general&prefix=key.json"
            >>> s3_obj = S3Object(console_url=url)
            >>> s3_obj.bucket_name
            'my-bucket'
            >>> s3_obj.key
            'key.json'

        Example:
            >>> from trz_py_utils.s3 import S3Object
            >>> s3_obj = S3Object(s3_uri="s3://bucket/path/to/key.json")
            >>> "s3.console.aws.amazon.com" in s3_obj.console_url
            True
            >>> s3_obj.console_url
            'https://s3.console.aws.amazon.com/s3/object/bucket?region=us-east-2&prefix=path%2Fto%2Fkey.json'
            >>> S3Object(console_url=s3_obj.console_url).key
            'path/to/key.json'

        Example:
            >>> from trz_py_utils.s3 import S3Object
            >>> obj = S3Object(s3_uri="https://trz-s3-test.s3.us-east-2.amazonaws.com/figure-65.png")
            >>> obj.url
            'https://trz-s3-test.s3.us-east-2.amazonaws.com/figure-65.png'

        """  # noqa
        # eg. s3://bucket/path/to/key
        # eg. bucket/path/to/key
        if obj:
            self.bucket_name = obj.bucket_name
            self.key = obj.key
        elif s3_uri and "amazonaws.com" in s3_uri:
            self.bucket_name, self.key = self._parse_direct_url(s3_uri)
        elif s3_uri and "s3://" in s3_uri:
            self.bucket_name, self.key = self._parse_s3_uri(s3_uri)
        elif console_url:
            self.bucket_name, self.key = self._parse_console_url(console_url)
        elif bucket and key:
            self.bucket_name = bucket
            self.key = key

        self.obj = get_object(self.bucket_name, self.key)
        self.console_url = console_url or make_console_url(self.obj)
        self.s3_uri = f"s3://{bucket}/{key}"
        self.url = make_s3_url(self.obj)

    def _parse_s3_uri(self, s3_uri: str):
        s3_uri = unquote(s3_uri)
        s3_uri = f"s3://{s3_uri}" if "s3" not in s3_uri else s3_uri
        url: ParseResult = urlparse(s3_uri)
        if url.params or url.query or url.fragment:
            raise ValueError(f"pass arg console_url not s3_uri: {s3_uri}")

        bucket = url.netloc
        key = url.path.strip("/")

        return bucket, key

    def _parse_console_url(self, console_url: str):
        # https://s3.console.aws.amazon.com/s3/object/trz-fmcsa-dev?region=us-east-2&bucketType=general&prefix=headers/crash_carriers/CrashCarrier_01012018_12312018HDR.txt.json
        console_url = unquote(console_url)
        invalid = [
            urlparse(console_url).netloc != "s3.console.aws.amazon.com",
            "s3/object/" not in console_url,
            "prefix=" not in console_url,
        ]
        if any(invalid):
            raise ValueError(f"not a console url: {console_url}")

        bucket = console_url.split("s3/object/")[1].split("?")[0]
        key = console_url.split("prefix=")[1].split("&")[0]

        return bucket, key

    def _parse_direct_url(self, url: str):
        # eg. https://trz-s3-test.s3.us-east-2.amazonaws.com/figure-65.png
        # eg. https://trz-s3-test.s3.amazonaws.com/figure-65.png?X-Amz-Algor...
        url = unquote(url)
        bucket = urlparse(url).netloc.split(".")[0]
        key = urlparse(url).path.strip("/")
        return bucket, key

    def new_prefix(self, prefix: str):
        key_no_root = self.key.split('/', maxsplit=1)[1]
        return f"{prefix}/{key_no_root}"
