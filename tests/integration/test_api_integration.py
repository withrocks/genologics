from unittest import TestCase
import unittest
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

request_watcher = RequestWatcher(__name__)


class ClarityApiIntegrationTestCase(TestCase):
    def setUp(self):
        # TODO: The state of these objects is shared!
        self.lims = Lims(config.BASEURI, config.USERNAME, config.PASSWORD)
        # logging.basicConfig(level=logging.DEBUG)  # NOMERGE
        # TODO: Assert that this is exactly v2,r24


class TestArtifactsMajor2Minor24(ClarityApiIntegrationTestCase):
    """Tests GETing data. Contains no tests that should POST, PUT or DELETE

    NOTE: This tests version v2r24 of the API. See: https://www.genologics.com/developer/4-2/

    If you are running against another version, the test case will not run.

    Previous versions are not tested. Later versions should be put in another class, inheriting from this one.
    """

    @unittest.skip("Implement")
    def test_get_artifact_groups(self):
        self.lims.get_artifactgroups()

    def test_get_artifacts(self):
        """Tests if we can list all artifacts in the system without a filter.

        No details are provided in the overview page

        TODO: calling self.lims.get_artifacts twice is not cached. Users might assume that it is though.
        TODO: The `allow` mechanism doesn't work here because we're paging. Consider expanding so that the watcher
        understands paging.
        """
        # TODO: This should be configurable, but here we assume that we test on a system with more than 2000 artifacts

        # We allow one call per page, but not more
        artifacts = self.lims.get_artifacts()
        assert len(artifacts) > 2000, \
            "Artifact count less than expected. If there are more artifacts in the system, the test has failed."

    def test_expand_analyte(self):
        """Can find an artifact that's an analyte and auto-expand it (lazy load) to fetch all of it's properties"""
        import random
        request_watcher.allow(100)  # Allow for paging
        analytes = self.lims.get_artifacts(type="Analyte")

        # Fetch a random analyte (so these tests are not always testing the same set) TODO
        request_watcher.allow(1)  # Now limit it again
        analyte = random.choice(analytes)
        assert len(analyte.name) > 0

    # NO_MERGE: The following tests are using specific names for completeness. Don't merge into upstream
    # before it has at least been made more generic (e.g. configuration file)
    # TODO: Move this to clarity-ext or clarity-snpseq
    # TODO: This huge test should be broken down and made more generic
    def test_expand_specific_analyte(self):
        request_watcher.allow(2)  # One for the search, one for the details
        analyte_name = "Test-0002-Jojo21"
        analyte = self.lims.get_artifacts(name=analyte_name)[0]
        assert analyte.name == analyte_name
        assert analyte.type == "Analyte"
        assert analyte.output_type == "Analyte"
        assert analyte.parent_process.id == "24-3675"
        assert analyte.qc_flag == "PASSED"
        container, well = analyte.location
        assert container.id == "27-794"
        assert well == "F:1"
        assert analyte.working_flag is True
        assert len(analyte.samples) == 1
        assert analyte.samples[0].id == "LAG101A21"
        assert len(analyte.reagent_labels) == 1
        assert analyte.reagent_labels[0] == "D701-D506"
        assert len(analyte.udf) == 23
        assert set([(key, value) for key, value in analyte.udf.items()]) == \
               {('TS Length (bp)', 287), ('Dil. calc. source vol', 0), ('Conc. Current (ng/ul)', 50),
                ('qPCR conc. (nM)', 111.85138390028155), ('Dil. calc. target vol', 10), ('Fragment Upper (bp)', 389),
                ('Pooling', '4 libraries/pool'), ('PhiX %', '1'), ('conc FC', '14 pM'), ('TS Fragment Lower (bp)', 206),
                ('Fragment Lower (bp)', 206), ('ng input', 500), ('Special info seq', 'No'),
                ('TS Fragment Upper (bp)', 389), ('Current sample volume (ul)', 20),
                ('Sequencing instrument', 'HiSeq2500 High Output'), ('Initial qPCR conc. (pM)', 1.77551699001),
                ('Number of lanes', '1 lane/pool'), ('Target conc. (ng/ul)', 50), ('Target vol. (ul)', 10),
                ('Dil. calc. target conc.', 50), ('Conc. Current (nM)', 111.85138390028155),
                ('Length Current (bp)', 287)}
        assert len(analyte.workflow_stages) == 8
        # Status and name can now be fetched without loading the entire stage object:
        assert {(stage.status, len(stage.name) > 0) for stage in analyte.workflow_stages} == {('COMPLETE', True)}

        # Check if we can expand the sample:
        sample = analyte.samples[0]
        self.check_sample_details(sample)

    def check_sample_details(self, sample):
        request_watcher.allow(1)
        assert len(sample.name) > 0
        assert sample.date_received == "2017-06-22"  # TODO: check if parsable to date
        assert len(sample.project.id) > 0  # TODO: Expand!
        self.check_project_details(sample.project)
        self.check_researcher_details(sample.submitter)

    def check_project_details(self, project):
        request_watcher.allow(1)
        assert len(project.name) > 0
        assert len(project.open_date) > 0  #TODO: parse
        assert len(project.researcher.id) > 0

        # TODO: files are currently required
        assert len(project.files) > 0

    def check_researcher_details(self, researcher):
        request_watcher.allow(1)
        assert len(researcher.first_name) > 0
        assert len(researcher.last_name) > 0

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
            self.assertEqual(workflow.fetch_state, FETCH_STATE_OVERVIEW)
            self.assertTrue(workflow.status in expected_statuses, "Status not defined: {}".format(workflow.status))
            self.assertTrue(len(workflow.name) > 0, workflow.name)
            statuses.add(workflow.status)

        not_found_expected_statuses = expected_statuses - statuses
        if len(not_found_expected_statuses) > 0:
            logging.warn("Didn't find any workflow with these statuses {}".format(not_found_expected_statuses))

    def test_get_workflow(self):
        # NOMERGE: temp test, specific ID. Replace when get_workflows is working again
        request_watcher.allow(1)
        workflow = Workflow(self.lims, id=51)
        assert len(workflow.status) > 0

    def test_expand_workflow(self):
        """Fetch a workflow from the list entry point, then expand it and ensure that we can
        access information on it with expansion only when necessary"""

        request_watcher.allow(
            1)  # Allows exactly one call through, more calls will fail. request_watcher is global but will use the stack trace to identify this
        workflow = self.lims.get_workflows()[0]

        # These attribute don't require another call to the server:
        assert len(workflow.name) > 0, "Expecting a non-zero length name"
        assert len(workflow.status) > 0, "Expecting a non-zero length status"

        # However, listing the protocols requires a round trip to the server, to /configuration/workflows/<number>
        request_watcher.allow(1)
        assert len(workflow.protocols) > 0

        # But fetching another property of the full does not require another call to that endpoint
        assert workflow.stages is not None

        # Accessing the following attributes of the Protocol should also be free:
        protocol = workflow.protocols[0]
        assert len(protocol.name) > 0
