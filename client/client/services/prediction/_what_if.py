def _logical_xor(x, y):
    return x ^ y


def make_what_if(feature, pct=None, value=None):
    """Factory function for :class:`WhatIf` objects.

    Parameters
    ----------
    feature : str
        Feature name for the "what-if" scenario.

    pct: int, default=None
        Percentage increase/decrease the feature will take for the "what-if"
        scenario. Either ``pct`` or ``value`` must be passed, but not both
        simultaneously.

    value : int, default=None
        Constant value the feature will take for the "what-if" scenario.
        Either ``pct`` or ``value`` must be passed, but not both
        simultaneously.

    Returns
    -------
    what_if : WhatIf
    """
    return WhatIf(feature, pct, value)


class WhatIf:
    def __init__(self, feature, pct=None, value=None):
        self.feature = feature
        self.pct = pct
        self.value = value
        self._validate()

    def _validate(self):
        if not _logical_xor(self.pct is None, self.value is None):
            raise

        if not (isinstance(self.pct, int) or self.pct is None):
            raise

        if not (isinstance(self.value, int) or self.value is None):
            print(self.value)
            raise

    def _json(self):
        return {'feature': self.feature, 'ptc': self.pct,
                'value': self.value}
