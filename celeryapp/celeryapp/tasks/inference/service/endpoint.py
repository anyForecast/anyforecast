import copy
import re
from typing import Dict, OrderedDict, Union
from urllib.parse import urlsplit

from requests import Session as HttpSession

from ...exceptions import UnknownServiceError

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
    """Represents an endpoint for a particular container.
    Only an endpoint can make requests.
    """

    def __init__(
            self, host, endpoint_prefix, response_parser_factory=None,
            http_session=None
    ):
        self._endpoint_prefix = endpoint_prefix
        self.host = host
        if response_parser_factory is None:
            pass
            # response_parser_factory = parsers.ResponseParserFactory()
        self._response_parser_factory = response_parser_factory
        self.http_session = http_session
        if self.http_session is None:
            self.http_session = HttpSession()

    def make_request(self, json=None, data=None, headers=None):
        http_response = self.http_session.post(
            url=self.host, json=json, data=data, headers=headers)
        return http_response

    def __repr__(self):
        return '%s(%s)' % (self._endpoint_prefix, self.host)


class EndpointCreator:

    def create_endpoint(
            self, service_name, endpoint_url, response_parser_factory=None,
            http_session_cls=HttpSession
    ):
        if not is_valid_endpoint_url(endpoint_url):
            raise ValueError("Invalid endpoint: %s" % endpoint_url)

        endpoint_prefix = service_name
        http_session = http_session_cls()

        return Endpoint(
            endpoint_url,
            endpoint_prefix=endpoint_prefix,
            response_parser_factory=response_parser_factory,
            http_session=http_session)


class EndpointResolver:
    """Provides object oriented access to endpoint metadata.

    Parameters
    ----------
    endpoint_data : dict
        A dict of endpoint configuration.
    """

    def __init__(self, endpoint_data: Union[Dict, OrderedDict]):
        self.endpoint_data = endpoint_data

    def resolve(self, service_name: str, model_name: str):
        """Resolves endpoint data.

        Parameters
        ----------
        service_name : str
            Service name.

        model_name : str
            Model name

        Returns
        -------
        resolution : OrderedDict
        """
        service_data = self._get_service_data(service_name)
        endpoint_name = service_data.get('endpoint', '')
        port = service_data.get('port', '')
        hostname = self._get_hostname(service_data)
        result = service_data
        result['model_name'] = model_name
        result['hostname'] = self._expand_template(
            hostname, service_name, model_name, endpoint_name, port)
        return result

    def _get_service_data(self, service_name):
        try:
            return copy.deepcopy(self.endpoint_data['services'][service_name])
        except KeyError:
            raise UnknownServiceError(name=service_name)

    def _get_hostname(self, service_data):
        default_hostname = self.endpoint_data['defaults']['hostname']
        return service_data.get('hostname', default_hostname)

    def _expand_template(self, template, service_name, model_name,
                         endpoint_name, port):
        return template.format(
            service=service_name, model=model_name, endpoint=endpoint_name,
            port=port)
