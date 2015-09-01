import unittest
from genologics.lims import Lims
import cgi


class ParametersRepeatedRegressionTest(unittest.TestCase):
    def test_get_should_not_repeat_parameters(self):
        """When requesting paged results, the parameters where repeated in the query string"""
        urls = []

        def on_response_ready(url_visited):
            urls.append(url_visited)

        def send_mock(prep, **ignored_send_kwargs):
            # First, return a response with a next page
            resp = MockResponse()
            template = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
            <con:containers xmlns:con="http://genologics.com/ri/container">
              <container uri="http://somehwere/api/v2/containers/container_name" limsid="container_id">
                <name>container_name</name>
              </container>
              {next_page_element}
            </con:containers>"""
            if len(urls) < 5:
                new_url = cgi.escape("{url}".format(url=prep.url))
                next_page_element = '<next-page uri="{url}"/>'.format(url=new_url)
            else:
                next_page_element = ''
            resp.content = template.format(next_page_element=next_page_element)
            resp.status_code = 200
            on_response_ready(prep.url)
            return resp

        lims = Lims("http://somewhere", "ANY", "ANY")
        lims.request_session.send = send_mock
        lims.get_containers(name="container")
        print urls
        # All the urls should be the same, since we're skipping the start-index:
        all_equal = all([url == "http://somewhere/api/v2/containers?name=container" for url in urls])
        self.assertTrue(all_equal)


class MockResponse:
    pass

