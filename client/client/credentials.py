import logging
import os

from .exceptions import UnknownCredentialError
from .exceptions import PartialCredentialsError

logger = logging.getLogger(__name__)


def create_credential_resolver(session=None, cache=None):
    """Create a default credential resolver.

    This creates a pre-configured credential resolver
    that includes the default lookup chain for
    credentials.

    Parameters
    ----------
    """
    # profile_name = session.get_config_variable('profile') or 'default'
    # metadata_timeout = session.get_config_variable('metadata_service_timeout')
    # num_attempts = session.get_config_variable('metadata_service_num_attempts')
    # disable_env_vars = session.instance_variables().get('profile') is not None

    if cache is None:
        cache = {}

    env_provider = EnvProvider()
    pre_profile = [env_provider]
    profile_providers = []
    post_profile = []
    providers = pre_profile + profile_providers + post_profile

    # if disable_env_vars:
        # An explicitly provided profile will negate an EnvProvider.
        # We will defer to providers that understand the "profile"
        # concept to retrieve credentials.
        # The one edge case if is all three values are provided via
        # env vars:
        # export AWS_ACCESS_KEY_ID=foo
        # export AWS_SECRET_ACCESS_KEY=bar
        # export AWS_PROFILE=baz
        # Then, just like our client() calls, the explicit credentials
        # will take precedence.
        #
        # This precedence is enforced by leaving the EnvProvider in the chain.
        # This means that the only way a "profile" would win is if the
        # EnvProvider does not return credentials, which is what we want
        # in this scenario.
        # providers.remove(env_provider)
        # logger.debug('Skipping environment variable credential check'
        #             ' because profile name was explicitly set.')

    resolver = CredentialResolver(providers=providers)
    return resolver


class Credentials:
    """Holds the credentials needed to authenticate requests.

    Parameters
    ----------
    """

    def __init__(self, access_key, secret_key, token=None,
                 method=None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token

        if method is None:
            method = 'explicit'
        self.method = method

    def resolve(self):
        return {
            'access_key': self.access_key,
            'secret_key': self.secret_key,
            'token': self.token
        }


class CredentialResolver:
    def __init__(self, providers):
        """
        :param providers: A list of ``CredentialProvider`` instances.
        """
        self.providers = providers

    def insert_before(self, name, credential_provider):
        """
        Inserts a new instance of ``CredentialProvider`` into the chain that
        will be tried before an existing one.

        :param name: The short name of the credentials you'd like to insert the
            new credentials before. (ex. ``env`` or ``config``). Existing names
            & ordering can be discovered via ``self.available_methods``.
        :type name: string
        :param cred_instance: An instance of the new ``Credentials`` object
            you'd like to add to the chain.
        :type cred_instance: A subclass of ``Credentials``
        """
        try:
            offset = [p.METHOD for p in self.providers].index(name)
        except ValueError:
            raise UnknownCredentialError(name=name)
        self.providers.insert(offset, credential_provider)

    def insert_after(self, name, credential_provider):
        """
        Inserts a new type of ``Credentials`` instance into the chain that will
        be tried after an existing one.
        :param name: The short name of the credentials you'd like to insert the
            new credentials after. (ex. ``env`` or ``config``). Existing names
            & ordering can be discovered via ``self.available_methods``.
        :type name: string
        :param cred_instance: An instance of the new ``Credentials`` object
            you'd like to add to the chain.
        :type cred_instance: A subclass of ``Credentials``
        """
        offset = self._get_provider_offset(name)
        self.providers.insert(offset + 1, credential_provider)

    def remove(self, name):
        """
        Removes a given ``Credentials`` instance from the chain.
        :param name: The short name of the credentials instance to remove.
        :type name: string
        """
        available_methods = [p.METHOD for p in self.providers]
        if name not in available_methods:
            # It's not present. Fail silently.
            return

        offset = available_methods.index(name)
        self.providers.pop(offset)

    def get_provider(self, name):
        """Return a credential provider by name.
        :type name: str
        :param name: The name of the provider.
        :raises UnknownCredentialError: Raised if no
            credential provider by the provided name
            is found.
        """
        return self.providers[self._get_provider_offset(name)]

    def _get_provider_offset(self, name):
        try:
            return [p.METHOD for p in self.providers].index(name)
        except ValueError:
            raise UnknownCredentialError(name=name)

    def load_credentials(self):
        """
        Goes through the credentials chain, returning the first ``Credentials``
        that could be loaded.
        """
        # First provider to return a non-None response wins.
        for provider in self.providers:
            logger.debug("Looking for credentials via: %s", provider.METHOD)
            creds = provider.load()
            if creds is not None:
                return creds

        # If we got here, no credentials could be found.
        # This feels like it should be an exception, but historically, ``None``
        # is returned.
        #
        # +1
        # -js
        return None


class CredentialProvider:
    """Base class for credential providers.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """
    # A short name to identify the provider within botocore.
    METHOD = None

    # A name to identify the provider for use in cross-sdk features like
    # assume role's `credential_source` configuration option. These names
    # are to be treated in a case-insensitive way. NOTE: any providers not
    # implemented in botocore MUST prefix their canonical names with
    # 'custom' or we DO NOT guarantee that it will work with any features
    # that this provides.
    CANONICAL_NAME = None

    def __init__(self, session=None):
        self.session = session

    def load(self):
        """Loads the credentials from their source & sets them on the object.

        Subclasses should implement this method (by reading from disk, the
        environment, the network or wherever), returning ``True`` if they were
        found & loaded.

        If not found, this method should return ``False``, indicating that the
        ``CredentialResolver`` should fall back to the next available method.

        The default implementation does nothing, assuming the user has set the
        ``access_key/secret_key/token`` themselves.

        :returns: Whether credentials were found & set
        :rtype: Credentials
        """
        return True

    def _extract_creds_from_mapping(self, mapping, *key_names):
        found = []
        for key_name in key_names:
            try:
                found.append(mapping[key_name])
            except KeyError:
                raise PartialCredentialsError(provider=self.METHOD,
                                              cred_var=key_name)
        return found


class EnvProvider(CredentialProvider):
    """Environment variables provider.

    Parameters
    ----------
    environ : dict, default=None
        The environment variables (defaults to ``os.environ`` if no value is
        provided).

    mapping: dict, default=None
        An optional mapping of variable names to
        environment variable names.  Use this if you want to
        change the mapping of access_key->AWS_ACCESS_KEY_ID, etc.
        The dict can have up to 3 keys: ``access_key``, ``secret_key``,
        ``session_token``.
    """
    METHOD = 'env'
    CANONICAL_NAME = 'Environment'
    ACCESS_KEY = 'AWS_ACCESS_KEY_ID'
    SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
    # The token can come from either of these env var.
    # AWS_SESSION_TOKEN is what other AWS SDKs have standardized on.
    TOKENS = ['AWS_SECURITY_TOKEN', 'AWS_SESSION_TOKEN']
    EXPIRY_TIME = 'AWS_CREDENTIAL_EXPIRATION'

    def __init__(self, environ=None, mapping=None):
        if environ is None:
            environ = os.environ
        self.environ = environ
        self._mapping = self._build_mapping(mapping)

    def _build_mapping(self, mapping):
        # Mapping of variable name to env var name.
        var_mapping = {}
        if mapping is None:
            # Use the class var default.
            var_mapping['access_key'] = self.ACCESS_KEY
            var_mapping['secret_key'] = self.SECRET_KEY
            var_mapping['token'] = self.TOKENS
            var_mapping['expiry_time'] = self.EXPIRY_TIME
        else:
            var_mapping['access_key'] = mapping.get(
                'access_key', self.ACCESS_KEY)
            var_mapping['secret_key'] = mapping.get(
                'secret_key', self.SECRET_KEY)
            var_mapping['token'] = mapping.get(
                'token', self.TOKENS)
            if not isinstance(var_mapping['token'], list):
                var_mapping['token'] = [var_mapping['token']]
            var_mapping['expiry_time'] = mapping.get(
                'expiry_time', self.EXPIRY_TIME)
        return var_mapping

    def load(self):
        """Search for credentials in explicit environment variables.
        """
        access_key = self.environ.get(self._mapping['access_key'], '')
        if access_key:
            logger.info('Found credentials in environment variables.')
            fetcher = self._create_credentials_fetcher()
            credentials = fetcher(require_expiry=False)
            return Credentials(
                credentials['access_key'], credentials['secret_key'],
                credentials['token'], method=self.METHOD
            )
        else:
            return None

    def _create_credentials_fetcher(self):
        mapping = self._mapping
        method = self.METHOD
        environ = self.environ

        def fetch_credentials(require_expiry=True):
            credentials = {}

            access_key = environ.get(mapping['access_key'], '')
            if not access_key:
                raise PartialCredentialsError(
                    provider=method, cred_var=mapping['access_key'])
            credentials['access_key'] = access_key

            secret_key = environ.get(mapping['secret_key'], '')
            if not secret_key:
                raise PartialCredentialsError(
                    provider=method, cred_var=mapping['secret_key'])
            credentials['secret_key'] = secret_key

            credentials['token'] = None
            for token_env_var in mapping['token']:
                token = environ.get(token_env_var, '')
                if token:
                    credentials['token'] = token
                    break

            credentials['expiry_time'] = None
            expiry_time = environ.get(mapping['expiry_time'], '')
            if expiry_time:
                credentials['expiry_time'] = expiry_time
            if require_expiry and not expiry_time:
                raise PartialCredentialsError(
                    provider=method, cred_var=mapping['expiry_time'])

            return credentials

        return fetch_credentials
