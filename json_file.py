import json


class JsonFile:
    def __init__(self, filename, encoding='utf-8'):
        self._filename = filename
        self._encoding = encoding

        with open(filename, encoding=self._encoding) as file:
            source = file.read()
            self._content = json.loads(source)

    def __getitem__(self, key):
        return self._content[key]

    def __setitem__(self, key, value):
        self._content[key] = value

    def contains(self, key):
        return key in self._content

    def save(self):
        with open(self._filename, 'w', encoding=self._encoding) as file:
            source = json.dumps(self._content, indent=4)
            file.write(source)
