import base64
import hashlib
import hmac
import urllib.parse as urlparse


def add_params_to_url(input_url: str, params: dict) -> str:
    """
    Adds parameters to a url

    Parameters
    ----------
    input_url : str
        url
    params : dict
        parameters

    Returns
    -------
    str
        url with parameters
    """
    url_parse = urlparse.urlparse(input_url)
    query = url_parse.query
    url_dict = dict(urlparse.parse_qsl(query))
    url_dict.update(params)
    url_new_query = urlparse.urlencode(url_dict)
    url_parse = url_parse._replace(query=url_new_query)
    return urlparse.urlunparse(url_parse)


def sign_url(input_url: str, secret: str) -> str:
    """
    Sign a request URL with a URL signing secret.

    Parameters
    ----------
    input_url : str
        The URL to sign
    secret : str
        Your URL signing secret

    Returns
    -------
    str
        The signed request URL
    """
    url = urlparse.urlparse(input_url)

    # We only need to sign the path+query part of the string
    url_to_sign = url.path + '?' + url.query

    # Decode the private key into its binary format
    # We need to decode the URL-encoded private key
    decoded_key = base64.urlsafe_b64decode(secret)

    # Create a signature using the private key and the URL-encoded
    # string using HMAC SHA1. This signature will be binary.
    signature = hmac.new(decoded_key, str.encode(url_to_sign), hashlib.sha1)

    # Encode the binary signature into base64 for use within a URL
    encoded_signature = base64.urlsafe_b64encode(signature.digest())

    original_url = url.scheme + '://' + url.netloc + url.path + '?' + url.query

    # Return signed URL
    return original_url + '&signature=' + encoded_signature.decode()
