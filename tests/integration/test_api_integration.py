from unittest import TestCase
from genologics.lims import Lims
from genologics import config
from genologics.entities import *
import logging
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
        if self.calls >= self.allowed:
            raise ValueError("Allowed calls ({}) reached. Call `allow()` to increase the count again.".format(self.allowed))
        self.requests.append(kwargs["url"])
        self.calls += 1

    def allow(self, count):
        self.allowed = count
        self.calls = 0

    def __repr__(self):
        return repr(self.requests)


class TestApiIntegrationVersion2_24(TestCase):
    """Tests GETing data. Contains no tests that should POST, PUT or DELETE

    NOTE: This tests version v2r24 of the API. See: https://www.genologics.com/developer/4-2/

    If you are running against another version, the test case will not run.

    Previous versions are not tested. Later versions should be put in another class, inheriting from this one.
    """

    def setUp(self):
        self.lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)
        logging.basicConfig(level=logging.DEBUG)  # NOMERGE
        # TODO: Assert that this is exactly v2,r24

        self.request_watcher = RequestWatcher()

    def test_get_artifact_groups(self):
        pass

    def test_get_workflows(self):
        """Fetches the workflow via /configuration/workflows via one HTTP call.

        Each workflow should already have status and name.
        """

        # TODO: Change that watcher so it limits the calls beforehand. Then we have a way of stopping the test
        # right away if it's doing unexpected calls
        # TODO: Add the expected statuses to the descriptor
        expected_statuses = set(["PENDING", "ARCHIVED", "ACTIVE"])
        workflows = self.lims.get_workflows()
        statuses = set()
        for workflow in workflows:
            self.assertEqual(workflow.fetch_state, FETCH_STATE_MINIMAL)
            self.assertTrue(workflow.status in expected_statuses, "Status not defined: {}".format(workflow.status))
            self.assertTrue(len(workflow.name) > 0, workflow.name)
            statuses.add(workflow.status)

        if statuses not in expected_statuses:
            logging.warn("Didn't find any workflow with statuses in {}".format(expected_statuses - statuses))

    def test_get_workflow(self):
        # NOMERGE: temp test, specific ID. Replace when get_workflows is working again
        self.request_watcher.allow(1)
        workflow = Workflow(self.lims, id=51)
        assert len(workflow.status) > 0

    def test_expand_workflow(self):
        """Fetch a workflow from the list entry point, then expand it and ensure that we can
        access information on it with expansion only when necessary"""
        self.request_watcher.allow(1)  # Allows exactly one call through, more calls will fail
        workflow = self.lims.get_workflows()[0]

        # These attribute don't require another call to the server:
        assert len(workflow.name) > 0, "Expecting a non-zero length name"
        assert len(workflow.status) > 0, "Expecting a non-zero length status"

        # However, listing the protocols requires a round trip to the server, to /configuration/workflows/<number>
        assert len(workflow.protocols) > 0
        self.request_watcher.expect(1)

        # But fetching another property of the full does not require another call to that endpoint
        assert workflow.stages is not None
        self.request_watcher.expect(0)

        # Accessing the following attributes of the Protocol should also be free:
        protocol = workflow.protocols[0]
        assert len(protocol.name) > 0
        self.request_watcher.expect(0)
