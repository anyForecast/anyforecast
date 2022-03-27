import logging
import re
from urllib.parse import urlsplit

from requests import Session as HttpSession

logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60
MAX_POOL_CONNECTIONS = 10
UNSAFE_URL_CHARS = frozenset('\t\r\n')


def is_valid_endpoint_url(endpoint_url):
    """Verify the endpoint_url is valid.
    
    Parameters
    ----------
    endpoint_url : str
        An endpoint_url.  Must have at least a scheme and a hostname.
    
    Returns
    -------
    True if the endpoint url is valid. False otherwise.
    """
    # post-bpo-43882 urlsplit() strips unsafe characters from URL, causing
    # it to pass hostname validation below.  Detect them early to fix that.
    if UNSAFE_URL_CHARS.intersection(endpoint_url):
        return False
    parts = urlsplit(endpoint_url)
    hostname = parts.hostname
    if hostname is None:
        return False
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1]
    allowed = re.compile(
        r"^((?!-)[A-Z\d-]{1,63}(?<!-)\.)*((?!-)[A-Z\d-]{1,63}(?<!-))$",
        re.IGNORECASE)
    return allowed.match(hostname)


class Endpoint:
    """Represents an endpoint for a particular service 
    in a specific region.  Only an endpoint can make requests.
    """

    def __init__(self, host, endpoint_prefix, response_parser_factory=None,
                 http_session=None):
        self._endpoint_prefix = endpoint_prefix
        self.host = host
        if response_parser_factory is None:
            pass
            # response_parser_factory = parsers.ResponseParserFactory()
        self._response_parser_factory = response_parser_factory
        self.http_session = http_session
        if self.http_session is None:
            self.http_session = HttpSession()

    def make_request(self, request_dict):
        logger.debug("Making request with params: %s", request_dict)
        return self._send_request(request_dict)

    def _send_request(self, request_dict):
        http_response = self.http_session.post(url=self.host,
                                               json=request_dict)
        return http_response, http_response.json()

    def __repr__(self):
        return '%s(%s)' % (self._endpoint_prefix, self.host)


class EndpointCreator:
    def __init__(self):
        pass

    def create_endpoint(
            self, service_name, endpoint_url,
            verify=None, response_parser_factory=None,
            timeout=DEFAULT_TIMEOUT, max_pool_connections=MAX_POOL_CONNECTIONS,
            http_session_cls=HttpSession, proxies=None,
            socket_options=None,
            client_cert=None, proxies_config=None
    ):
        if (
                not is_valid_endpoint_url(endpoint_url)
                # and not is_validi_ipv6_endpoint_url(endpoint_url)
        ):
            raise ValueError("Invalid endpoint: %s" % endpoint_url)

        endpoint_prefix = service_name
        http_session = http_session_cls()

        return Endpoint(
            endpoint_url,
            endpoint_prefix=endpoint_prefix,
            response_parser_factory=response_parser_factory,
            http_session=http_session
        )
