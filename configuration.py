
import configparser
import os


class Config:
    """Interact with configuration variables."""

    parser = configparser.SafeConfigParser()
    configFilePath = (os.path.join(os.getcwd(), 'config.ini'))
    parser.read(configFilePath)

    @classmethod
    def get(cls, section, key):
        """Get prod values from config.ini."""
        return cls.parser.get(section, key)


if __name__ == "__main__":
    print(Config.get('NGSI', 'host'))