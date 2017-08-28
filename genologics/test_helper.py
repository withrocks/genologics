from urllib3.connectionpool import HTTPConnectionPool


# Patch urlopen so we have a pre callback:
def wrap(func, pre):
    def call(*args, **kwargs):
        pre(func, *args, **kwargs)
        result = func(*args, **kwargs)
        return result

    return call


class RequestWatcher(object):
    """Watches requests to urlopen. Used for reporting on diffs."""

    def __init__(self, context):
        self.requests = dict()
        self._calls = dict()  # Allowed calls per test case
        self._allowed = dict()  # Allowed calls per test case
        self._last_request_pointer = 0
        self.context = context
        HTTPConnectionPool.urlopen = wrap(HTTPConnectionPool.urlopen, self.callback)

    def append(self, url):
        self.requests.append(url)

    def delta_requests(self):
        ret = self.requests[self._last_request_pointer:]
        self._last_request_pointer = len(self.requests)
        return ret

    def delta(self):
        return len(self.delta_requests())

    def expect(self, calls):
        delta = self.delta()
        assert delta == calls, "Expecting {} call(s), got {}".format(calls, delta)

    def identify_test_case(self):
        # This is ugly but effective, monkey patching urlopen in a with could be better...
        # ... not really designed for reuse (yet)
        def iterate_ids():
            import traceback
            stack = traceback.extract_stack()
            for fname, lineno, fn, line in stack:
                if self.context in fname:
                    yield fn
        return "/".join(iterate_ids())

    def register_url(self, url):
        test_case = self.identify_test_case()
        self.requests.setdefault(test_case, dict())
        self.requests[test_case].setdefault(url, 0)
        self.requests[test_case][url] += 1
        self.increment_calls()

    @property
    def calls(self):
        test_case = self.identify_test_case()
        self._calls.setdefault(test_case, 0)
        return self._calls[test_case]

    def increment_calls(self):
        test_case = self.identify_test_case()
        self._calls.setdefault(test_case, 0)
        self._calls[test_case] += 1

    @property
    def allowed(self):
        test_case = self.identify_test_case()
        try:
            return self._allowed[test_case]
        except:
            import sys
            return sys.maxint

    def callback(self, *_, **kwargs):
        test_case = self.identify_test_case()
        self.register_url(kwargs["url"])
        if self.calls > self.allowed:
            raise ValueError(
                "Allowed calls ({}) reached. Call `allow()` to increase the count again. Call history: \n\t{}"
                    .format(self.allowed, "\n\t".join(self.requests[test_case])))

    def allow(self, count):
        test_case = self.identify_test_case()
        self._allowed[test_case] = count
        self._calls[test_case] = 0

    def __repr__(self):
        import pprint
        return pprint.pformat(self.requests)


