import logging
from PIL import Image
from enum import Enum
from io import BytesIO
from uuid import uuid4
from urllib.parse import urlencode, urlparse, urlunparse, ParseResult
from tqdm import tqdm
from botocore.exceptions import ClientError

from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import Bucket, Object, S3ServiceResource


log = logging.getLogger(__name__)


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
    log.debug(f"stripping extension '{key.split('.')[-1]}' off '{key}'...")
    name_no_ext = ".".join(key.split('.')[:-1])
    bucket: Bucket = s3_resource.Bucket(bucket_name)
    obj: Object = bucket.put_object(
        Key=name_no_ext+ext,
        Body=bytes_img.getvalue(),
        ContentType=type)
    log.info(f"uploaded '{obj.bucket_name}/{obj.key}'.")

    return obj


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


def download_s3_object_with_progress(s3_obj: Object, fp=f"/tmp/{uuid4()}"):
    s3_client = s3_obj.meta.client
    # Get the size of the S3 object for progress tracking
    response = s3_client.head_object(Bucket=s3_obj.bucket_name,
                                     Key=s3_obj.key)
    total_size = response['ContentLength']

    # Use tqdm to create a progress bar
    opts = {
        "total": total_size,
        "unit": 'B',
        "unit_scale": True,
        "unit_divisor": 1024,
        "desc": "downloading...",
    }
    with tqdm(**opts) as pbar:
        # Open a file-like object to write the S3 object contents
        with open(fp, 'wb') as f:
            s3_client.download_fileobj(
                s3_obj.bucket_name,
                s3_obj.key,
                f,
                Callback=lambda chunk: pbar.update(chunk))


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


def make_s3_console_url(s3_obj: Object):
    """Returns a url string pointing to object in AWS Console.
    eg. 'https://s3.console.aws.amazon.com/s3/object/trz-rekognition-dev?region=us-east-2&prefix=figure-65.png'

    Args:
        s3_obj (Object): the object in s3 to return a link for.

    Returns:
        str: the web link pointing to object in AWS Console.

    Example:
        >>> from trz_py_utils.s3 import make_s3_console_url
        >>> from boto3 import resource
        >>> s3_resource = resource("s3", region_name="us-east-2")
        >>> s3_obj = s3_resource.Object("trz-s3-test", "figure-65.png")
        >>> make_s3_console_url(s3_obj)
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
