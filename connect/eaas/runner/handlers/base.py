import importlib
import inspect
import sys
from abc import ABC, abstractmethod

from connect.eaas.runner.helpers import iter_entry_points


class ApplicationHandlerBase(ABC):
    def __init__(self, config):
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def should_start(self):
        return self.get_application() is not None

    @property
    def name(self):
        return self.get_descriptor().get('name', 'Unnamed API')

    @property
    def description(self):
        return self.get_descriptor().get('description', '')

    @property
    def version(self):
        return self.get_descriptor().get('version', '0.0.0')

    @property
    def readme(self):
        return self.get_descriptor()['readme_url']

    @property
    def changelog(self):
        return self.get_descriptor()['changelog_url']

    @property
    def audience(self):
        return self.get_descriptor().get('audience')

    @property
    def icon(self):
        return self.get_descriptor().get('icon')

    @property
    def features(self):
        return self.get_features()

    @property
    def variables(self):
        return self.get_variables()

    def load_application(self, name):
        ep = next(
            iter_entry_points('connect.eaas.ext', name),
            None,
        )
        return ep.load() if ep else None

    def reload(self):
        application = self.get_application()
        if not application:
            return
        if inspect.isclass(application):
            importlib.reload(sys.modules[application.__module__])
        elif inspect.ismodule(application):
            importlib.reload(application)
        else:
            raise RuntimeError('Invalid application object.')

    def get_descriptor(self):
        if application := self.get_application():
            return application.get_descriptor()

    def get_variables(self):
        if application := self.get_application():
            return application.get_variables()

    @abstractmethod
    def get_application(self):
        raise NotImplementedError()

    @abstractmethod
    def get_features(self):
        raise NotImplementedError()
