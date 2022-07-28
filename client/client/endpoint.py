import logging
import re
from urllib.parse import urlsplit

from requests import Session as HttpSession

from .exceptions import NoRegionError, InvalidEndpointForService

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
                 http_session=None, ):
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
        print(f'json: {json}')
        print(f'data: {data}')
        print(f'headers: {headers}')
        http_response = self.http_session.post(
            url=self.host, json=json, data=data, headers=headers)
        return http_response

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


class ClientEndpointBridge:
    """Bridges endpoint data and client creation.
    """

    DEFAULT_ENDPOINT = '{service}.{region}.amazonaws.com'

    def __init__(self, endpoint_resolver):
        self.endpoint_resolver = endpoint_resolver

    def resolve(self, service_name, endpoint_name, endpoint_url=None,
                is_secure=True):
        resolved = self.endpoint_resolver.construct_endpoint(service_name,
                                                             endpoint_name)
        if resolved:
            endpoint_name = resolved.get('endpointName')
            return self._create_endpoint(resolved, service_name,
                                         endpoint_name, endpoint_url,
                                         is_secure)

    def _create_endpoint(self, resolved, service_name, endpoint_name,
                         endpoint_url, is_secure):
        if endpoint_url is None:
            hostname = resolved.get('hostname')
            endpoint_url = self._make_url(
                hostname,
                is_secure,
                resolved.get('protocols', [])
            )
        return self._create_result(service_name, endpoint_name, endpoint_url,
                                   resolved)

    def _create_result(self, service_name, endpoint_name, endpoint_url,
                       metadata):
        return {
            'service_name': service_name,
            'endpoint_name': endpoint_name,
            'endpoint_url': endpoint_url,
            'metadata': metadata
        }

    def _make_url(self, hostname, is_secure, supported_protocols):
        if is_secure and 'https' in supported_protocols:
            scheme = 'https'
        else:
            scheme = 'http'
        return '%s://%s' % (scheme, hostname)


LOG = logging.getLogger(__name__)
DEFAULT_URI_TEMPLATE = '{service}:{port}/{endpoint}.{dnsSuffix}'
DEFAULT_SERVICE_DATA = {'endpoints': {}}


class EndpointResolver:
    """Resolves endpoints based on endpoint metadata.

    Parameters
    ----------
    endpoint_data : dict
        A dict of endpoint configuration.
    """

    def __init__(self, endpoint_data):
        if 'partitions' not in endpoint_data:
            raise ValueError('Missing "partitions" in endpoint data')
        self._endpoint_data = endpoint_data

    def construct_endpoint(self, service_name, endpoint_name=None,
                           partition_name=None):

        """Constructs endpoint data.

        Parameters
        ----------
        service_name : str
        endpoint_name : name
        partition_name : name

        Returns
        -------
        endpoint_data : dict
        """
        if partition_name is not None:
            valid_partition = None
            for partition in self._endpoint_data['partitions']:
                if partition['partition'] == partition_name:
                    valid_partition = partition
            if valid_partition is not None:
                result = self._endpoint_for_partition(
                    valid_partition, service_name, endpoint_name)
                return result
            return None

        # Iterate over each partition until a match is found.
        for partition in self._endpoint_data['partitions']:
            result = self._endpoint_for_partition(
                partition, service_name, endpoint_name)
            if result:
                return result

    def get_available_endpoints(self):
        """Obtains collection of available endpoints.

        Returns
        -------
        endpoints : list
            List of endpoints.
        """
        return self._endpoint_data['endpoints']

    def _get_endpoint_name(self, service_data, service_name, endpoint_name):
        # Use the partition endpoint if no region is supplied.
        if endpoint_name is None:
            if 'partitionEndpoint' in service_data:
                endpoint_name = service_data['partitionEndpoint']
            elif not service_data['endpoints']:
                endpoint_name = ''
            else:
                raise NoRegionError()
        else:
            # Attempt to resolve the exact endpoint name.
            if endpoint_name not in service_data['endpoints']:
                raise InvalidEndpointForService(endpoint=endpoint_name,
                                                service=service_name)

        return endpoint_name

    def _endpoint_for_partition(self, partition, service_name, endpoint_name):
        # Get the service from the partition, or an empty template.
        service_data = partition['services'].get(
            service_name, DEFAULT_SERVICE_DATA)

        endpoint_name = self._get_endpoint_name(service_data, service_name,
                                                endpoint_name)

        resolve_kwargs = {
            'partition': partition,
            'service_name': service_name,
            'service_data': service_data,
            'endpoint_name': endpoint_name
        }
        return self._resolve(**resolve_kwargs)

    def _resolve(self, partition, service_name, service_data, endpoint_name):
        endpoint_data = service_data.get("endpoints", {}).get(endpoint_name,
                                                              {})
        if endpoint_data.get('deprecated'):
            LOG.warning(
                'Client is configured with the deprecated endpoint: %s' % (
                    endpoint_name
                )
            )

        service_defaults = service_data.get('defaults', {})
        partition_defaults = partition.get('defaults', {})
        result = endpoint_data

        # If a port is included in service_data
        port = service_data.get('port', '')

        # If dnsSuffix has not already been consumed from a variant definition
        if 'dnsSuffix' not in result:
            result['dnsSuffix'] = partition['dnsSuffix']

        # Insert names in resulting dict.
        result['partition'] = partition['partition']
        result['endpointName'] = endpoint_name

        # Merge in the service defaults then the partition defaults.
        self._merge_keys(service_defaults, result)
        self._merge_keys(partition_defaults, result)

        result['hostname'] = self._expand_template(
            partition, result['hostname'], service_name, endpoint_name,
            result['dnsSuffix'], port
        )

        return result

    def _merge_keys(self, from_data, result):
        for key in from_data:
            if key not in result:
                result[key] = from_data[key]

    def _expand_template(self, partition, template, service_name,
                         endpoint_name, dnsSuffix, port):
        return template.format(
            service=service_name, endpoint=endpoint_name,
            dnsSuffix=dnsSuffix, port=port)

    def get_service_endpoints_data(self, service_name, partition_name='aws'):
        """Obtains endpoint data for a particular partition-service
        combination.

        Parameters
        ----------
        service_name : str
            Service name inside partition.

        partition_name : str, default='aws'
            Partition name.
        """
        for partition in self._endpoint_data['partitions']:
            if partition['partition'] != partition_name:
                continue
            services = partition['services']
            if service_name not in services:
                continue
            return services[service_name]['endpoints']

    def get_available_partitions(self):
        """Obtains collection of available partitions.

        Returns
        -------
        partitions : list
            List with partition names.
        """
        result = []
        for partition in self._endpoint_data['partitions']:
            result.append(partition['partition'])
        return result
