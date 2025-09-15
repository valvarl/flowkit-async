class AlwaysFalseDetector:
    def is_sensitive(self, key_path, value):
        return False


class FlagLeakDetector:
    def is_sensitive(self, key_path, value):
        return value == "LEAK"


def callable_detector(key_path, value):
    return isinstance(value, bytes | bytearray) and len(value) > 1000
