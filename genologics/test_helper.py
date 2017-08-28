from urllib3.connectionpool import HTTPConnectionPool
import sys


# Patch urlopen so we have a pre callback:
def wrap(func, pre):
    def call(*args, **kwargs):
        pre(func, *args, **kwargs)
        result = func(*args, **kwargs)
        return result

    return call


class RequestWatcher(object):
    """Watches requests to urlopen. Used for reporting on diffs."""

    def __init__(self):
        self.requests = list()
        self._last_request_pointer = 0
        self.allowed = sys.maxint
        self.calls = 0
        print "HERE, I am, about to patch the stuff, guessing it always points to the same?"
        raise
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

    def callback(self, *_, **kwargs):
        self.requests.append(kwargs["url"] + "HERE")
        if self.calls >= self.allowed:
            raise ValueError(
                "Allowed calls ({}) reached. Call `allow()` to increase the count again. Call history: \n\t{}"
                    .format(self.allowed, "\n\t".join(self.requests)))
        self.calls += 1

    def allow(self, count):
        self.allowed = count
        self.calls = 0

    def __repr__(self):
        return repr(self.requests)


# Patch urlopen so we have a pre callback:
def wrap(func, pre):
    def call(*args, **kwargs):
        pre(func, *args, **kwargs)
        result = func(*args, **kwargs)
        return result

    return call


class RequestWatcher(object):
    """Watches requests to urlopen. Used for reporting on diffs."""

    def __init__(self):
        self.requests = list()
        self._last_request_pointer = 0
        self.allowed = sys.maxint
        self.calls = 0
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

    def callback(self, *args, **kwargs):
        import traceback
        print args, kwargs

        def running_test():
            # Rather ugly, fetch which test is running from the stack trace, as the monkey patched 

        def format_stack():
            # Limit the stack to this module, TODO very hacky for now
            def filter_stack():
                stack = traceback.extract_stack()
                filtered = False
                for stack_entry in stack:
                    filename, lineno, name, line = stack_entry
                    if "python2.7" in filename or "pycharm" in filename or "test_helper" in filename:
                        filtered = True
                        continue
                    if filtered:
                        yield "  ..."
                        filtered = False
                    yield "  File \"{}\", line {}".format(filename, lineno)
                    yield "    {}".format(line)
                if filtered:
                    yield "  ..."

            return "\n".join(filter_stack())

        msg = "{}\nObject ID: {}, Stack: \n{}".format(kwargs["url"], id(self), format_stack())

        self.requests.append(msg)
        if self.calls >= self.allowed:
            raise ValueError(
                "Allowed calls ({}) reached. Call `allow()` to increase the count again. Call history: \n{}".format(self.allowed, "\n".join(self.requests)))
        self.calls += 1

    def allow(self, count):
        self.allowed = count
        self.calls = 0

    def __repr__(self):
        return repr(self.requests)
