import numpy as np


class AttrDict(dict):
    """Auxiliary class for accessing dictionary keys like attributes.
    """

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def get_history(estimator, name):
    """Obtains history from estimator

    A history is any collection values recorded during training under a
    given name. Histories can be error metrics, loss functions, flags, etc

    For any fitted mooncake estimator, histories are stored inside the
    `history` attribute inside the skorch model, e.g., estimator.net_.history.

    Parameters
    ----------
    estimator : mooncake estimator
        Fitted blue meth estimator

    name : str
        Name of the history

    Returns
    -------
    history : list
    """
    if hasattr(estimator, 'estimators'):
        # Collect history in all estimators
        histories = []
        for est in estimator.estimators:
            histories.append(est.net_.history[:, name])

        # Get max length
        max_length = max(len(x) for x in histories)

        # Pad each history with nans at the end
        padded_histories = []
        for hist in histories:
            pad_hist = np.pad(
                hist,
                (0, max_length - len(hist)),
                constant_values=np.nan
            )
            padded_histories.append(pad_hist)

        # Return mean of all histories
        avg_histories = np.nanmean(padded_histories, axis=0)
        return avg_histories

    history = estimator.net_.history[:, name]
    return history
