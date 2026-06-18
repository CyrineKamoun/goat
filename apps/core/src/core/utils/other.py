import base64
import imghdr
import io
from typing import Any

import six


def get_image_extension_from_base64(base64_str: str) -> str | None:
    if base64_str.startswith("data:image/"):
        base64_str = base64_str.split(";base64,", 1)[1]

    image_data = base64.b64decode(base64_str, validate=True)

    extension = imghdr.what(None, h=image_data)
    return extension


def decode_base64_file(data: Any) -> Any:
    """
    Fuction to convert base 64 to readable IO bytes and auto-generate file name with extension
    :param data: base64 file input
    :return: tuple containing IO bytes file and filename
    """
    # Check if this is a base64 string
    if isinstance(data, six.string_types):
        # Check if the base64 string is in the "data:" format
        if "data:" in data and ";base64," in data:
            # Break out the header from the base64 content
            header, data = data.split(";base64,")

        # Try to decode the file. Return validation error if it fails.
        try:
            decoded_file = base64.b64decode(data)
        except TypeError:
            TypeError("invalid_image")

        return io.BytesIO(decoded_file)
