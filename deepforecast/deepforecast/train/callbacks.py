import math
import skorch
from torch.optim import lr_scheduler


class CallbacksCreator:

    def create_callback(self, name, **kwargs):
        cls = self.get_callback_cls(name)
        return cls(**kwargs)

    def get_callback_cls(self, name):
        return getattr(skorch.callbacks, name)


class LRSCallbackCreator:
    def __init__(self, estimator, dataset, event_name='train_lr'):
        self.estimator = estimator
        self.dataset = dataset
        self.event_name = event_name

        self._callbacks_creator = CallbacksCreator()

    def create_callback(self, policy, **kwargs):
        kwargs_creator = self._get_kwargs_creator(policy)
        kwargs = kwargs_creator.create_kwargs(**kwargs)
        kwargs['event_name'] = self.event_name
        return self._callbacks_creator.create_callback('LRScheduler', **kwargs)

    def _get_kwargs_creator(self, policy):
        kwargs_creators = {
            'OneCycleLR': OneCycleLRArgsCreator(self.estimator, self.dataset)
        }

        return kwargs_creators.get(policy, LRSArgsCreator(policy))


class LRSArgsCreator:

    def __init__(self, policy):
        self.policy = policy

    def create_kwargs(self, **kwargs):
        calculated_kwargs = self._calculate_kwargs()
        calculated_kwargs.update(kwargs)
        return {'policy': self.policy, **calculated_kwargs}

    def _calculate_kwargs(self):
        return {}


class OneCycleLRArgsCreator(LRSArgsCreator):
    """Args creator for the One Cycle learning rate scheduler.
    """

    def __init__(self, estimator, dataset):
        super().__init__(policy=lr_scheduler.OneCycleLR)
        self.estimator = estimator
        self.dataset = dataset

    def _calculate_kwargs(self):
        kwargs = {
            'steps_per_epoch': self.get_steps_per_epoch(),
            'epochs': self.get_epochs(),
            'max_lr': self.get_max_lr(),
            'step_every': 'batch'
        }
        return kwargs

    def get_steps_per_epoch(self):
        len_dataset = len(self.dataset)
        batch_size = self.estimator.batch_size
        return int(math.ceil(len_dataset / batch_size))

    def get_epochs(self):
        return self.estimator.max_epochs

    def get_max_lr(self):
        return self.estimator.lr
