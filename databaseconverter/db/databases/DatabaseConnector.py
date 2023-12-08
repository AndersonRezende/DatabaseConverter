from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    @abstractmethod
    def open_connection(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass
