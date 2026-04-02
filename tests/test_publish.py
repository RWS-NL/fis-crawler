import pathlib
import unittest
from unittest.mock import patch
from click.testing import CliRunner
from fis.publish.cli import publish_cli, _md_to_html


class TestPublishZenodo(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_md_to_html_simple(self):
        md = "# Title\n\nSome text."
        html = _md_to_html(md)
        self.assertIn("<h1>Title</h1>", html)
        self.assertIn("<p>Some text.</p>", html)

    def test_md_to_html_list(self):
        md = "List:\n\n- item 1\n- item 2"
        html = _md_to_html(md)
        self.assertIn("<ul>", html)
        self.assertIn("<li>item 1</li>", html)
        self.assertIn("<li>item 2</li>", html)
        self.assertIn("</ul>", html)
        self.assertIn("<p>List:</p>", html)

    @patch("requests.post")
    @patch("requests.get")
    @patch("requests.put")
    @patch("requests.delete")
    def test_publish_zenodo_missing_token(
        self, mock_del, mock_put, mock_get, mock_post
    ):
        # We catch the exception because we want to check the error message
        with self.runner.isolated_filesystem():
            pathlib.Path("output").mkdir()
            result = self.runner.invoke(
                publish_cli,
                ["zenodo", "--output-dir", "output"],
                env={"ZENODO_KEY": ""},
            )
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Zenodo access token not provided", result.output)

    @patch("requests.post")
    def test_publish_zenodo_mutually_exclusive(self, mock_post):
        with self.runner.isolated_filesystem():
            pathlib.Path("output").mkdir()
            result = self.runner.invoke(
                publish_cli,
                [
                    "zenodo",
                    "--output-dir",
                    "output",
                    "--base-id",
                    "123",
                    "--draft-id",
                    "456",
                ],
                env={"ZENODO_KEY": "test-token"},
            )
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("mutually exclusive", result.output)

    @patch("requests.post")
    @patch("requests.get")
    @patch("requests.put")
    @patch("requests.delete")
    @patch("fis.publish.cli.logger")
    def test_publish_zenodo_default_versioning_flow(
        self, mock_logger, mock_del, mock_put, mock_get, mock_post
    ):
        # Mock new version creation from default ID 19389587
        mock_post.return_value.status_code = 201
        mock_post.return_value.json.return_value = {
            "links": {"latest_draft": "http://zenodo/depositions/12345"}
        }

        # Mock GET for the newly created draft
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "id": 12345,
            "links": {"bucket": "http://zenodo/bucket/12345"},
            "files": [],
        }

        # Mock metadata update
        mock_put.return_value.status_code = 200

        with self.runner.isolated_filesystem():
            # Create dummy output structure
            output = pathlib.Path("output")
            output.mkdir()

            # Main candidates
            (output / "merged-graph").mkdir()
            (output / "merged-graph/graph.pickle").touch()
            (output / "merged-graph/edges.geojson").touch()
            (output / "merged-graph/edges.geoparquet").touch()
            (output / "merged-graph/nodes.geojson").touch()
            (output / "merged-graph/nodes.geoparquet").touch()
            (output / "merged_validation_report.md").touch()
            (output / "fis_validation_report.md").touch()

            # Zips
            (output / "fis-export").mkdir()
            (output / "fis-enriched").mkdir()
            (output / "euris-export").mkdir()
            (output / "euris-enriched").mkdir()
            (output / "lock-schematization").mkdir()
            (output / "bridge-schematization").mkdir()
            (output / "integrated-schematization-detailed").mkdir()
            (output / "integrated-schematization-simplified").mkdir()
            (output / "integrated-schematization-with-berths").mkdir()

            # Dummy description
            pathlib.Path("docs").mkdir()
            pathlib.Path("docs/ZENODO_DESCRIPTION.md").write_text("# Description")

            result = self.runner.invoke(
                publish_cli,
                ["zenodo", "--output-dir", "output"],
                env={"ZENODO_KEY": "test-token", "ZENODO_BASE_ID": "19389587"},
            )

            self.assertEqual(result.exit_code, 0)
            # Check that logger was called with expected messages
            logger_calls = [call.args[0] for call in mock_logger.info.call_args_list]
            self.assertTrue(
                any(
                    "Using default Zenodo base record ID: 19389587" in msg
                    for msg in logger_calls
                )
            )
            self.assertTrue(
                any(
                    "Created new draft version ID: 12345" in msg for msg in logger_calls
                )
            )

            self.assertTrue(
                any("Successfully updated metadata" in msg for msg in logger_calls)
            )


if __name__ == "__main__":
    unittest.main()
