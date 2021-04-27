import collections.abc as collections


class DbDict(collections.MutableMapping):
    """A dictionary which applies an arbitrary key-altering function before accessing the keys"""

    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        """:param key: a postgresql connection string or a dict with keys which can be substituted into a connection string"""
        if isinstance(key, dict):
            return dict_to_dsn(key)
        else:
            return key


def dict_to_dsn(d):
    defaults = {"dialect": "postgresql", "driver": "psycopg2", "sslmode": "require"}
    defaults.update(d)
    d = defaults
    if set(("database", "user", "host")) <= set(d):
        d["login"] = "{user}:{password}".format(**d) if "password" in d else d["user"]
        return (
            f"{d['dialect']}+{d['driver']}://{d['login']}@{d['host']}/{d['database']}"
        )
    else:
        raise KeyError(
            "The mapping must contain keys for at least 'database', 'user', and 'host'"
        )
