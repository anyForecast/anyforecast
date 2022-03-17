"""This module contains the interface for controlling how configuration
is loaded.
"""

DEFAULT_SESSION_VARIABLES = {
    # logical:  (config_file, env_var, default_value, conversion_func)
    'credentials_file': (None, 'CREDENTIALS_FILE', '~/.aws/credentials', None),
    'config_file': (None, 'CONFIG_FILE', '~/.aws/config', None),
}
