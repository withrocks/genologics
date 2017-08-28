from unittest import TestCase
from genologics.lims import Lims
from genologics import config
from genologics.entities import *
import logging
from genologics.test_helper import RequestWatcher

"""
TODO: 
* When loading from xml, check which descriptors have been tested. If there are any missing (not in the xml or not
in the object) report that.

TODO: The RequestWatcher currently fails when running all tests, some kind of sharing going on
"""


class TestApiIntegrationVersionMajor2Minor24(TestCase):
    """Tests GETing data. Contains no tests that should POST, PUT or DELETE

    NOTE: This tests version v2r24 of the API. See: https://www.genologics.com/developer/4-2/

    If you are running against another version, the test case will not run.

    Previous versions are not tested. Later versions should be put in another class, inheriting from this one.
    """

    def setUp(self):
        # TODO: The state of these objects is shared!
        self.lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD, use_cache=False)
        # logging.basicConfig(level=logging.DEBUG)  # NOMERGE
        # TODO: Assert that this is exactly v2,r24
        self.request_watcher = RequestWatcher()
        print(id(self.request_watcher))
        print(id(self.lims))

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
            print workflow, workflow.id, workflow.name, workflow.status
            self.assertEqual(workflow.fetch_state, FETCH_STATE_OVERVIEW)
            self.assertTrue(workflow.status in expected_statuses, "Status not defined: {}".format(workflow.status))
            self.assertTrue(len(workflow.name) > 0, workflow.name)
            statuses.add(workflow.status)

        not_found_expected_statuses = expected_statuses - statuses
        if len(not_found_expected_statuses) > 0:
            logging.warn("Didn't find any workflow with these statuses {}".format(not_found_expected_statuses))

    def test_get_workflow(self):
        lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)
        request_watcher = RequestWatcher()

        # NOMERGE: temp test, specific ID. Replace when get_workflows is working again
        request_watcher.allow(1)
        workflow = Workflow(lims, id=51)
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
        self.request_watcher.allow(1)
        assert len(workflow.protocols) > 0

        # But fetching another property of the full does not require another call to that endpoint
        assert workflow.stages is not None

        # Accessing the following attributes of the Protocol should also be free:
        protocol = workflow.protocols[0]
        assert len(protocol.name) > 0
