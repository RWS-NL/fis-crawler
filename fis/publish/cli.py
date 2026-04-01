import logging
import os
import pathlib
import zipfile
import re
import click
import requests
from typing import List, Optional

logger = logging.getLogger(__name__)

# Standard Zenodo API timeout in seconds
ZENODO_TIMEOUT = 60


@click.group(name="publish")
def publish_cli():
    """Publish dataset artifacts to external repositories."""
    pass


def _md_to_html(md_text: str) -> str:
    """Basic markdown to HTML conversion for Zenodo description."""
    html = md_text

    # Escape existing HTML if any (very basic)
    html = html.replace("<", "&lt;").replace(">", "&gt;")

    # Headers
    html = re.sub(r"^# (.*)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.*)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.*)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)

    # Bold
    html = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", html)

    # Lists: handle blocks of lines starting with '- '
    def replace_list(match):
        items = match.group(0).strip().split("\n")
        list_html = "<ul>" + "".join(f"<li>{item[2:]}</li>" for item in items) + "</ul>"
        return list_html

    html = re.sub(r"(^- .*(?:\n- .*)*)", replace_list, html, flags=re.MULTILINE)

    # Links
    html = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', html)

    # Paragraphs (avoid wrapping existing block tags)
    # This is a very simplistic heuristic
    blocks = html.split("\n\n")
    formatted_blocks = []
    for block in blocks:
        if not re.match(r"<(h1|h2|h3|ul|li)", block.strip()):
            formatted_blocks.append(f"<p>{block.strip()}</p>")
        else:
            formatted_blocks.append(block.strip())

    return "\n".join(formatted_blocks)


@publish_cli.command(name="zenodo")
@click.option("--token", envvar="ZENODO_KEY", help="Zenodo API access token.")
@click.option(
    "--base-id", default=None, help="Base deposition ID to create a new version from."
)
@click.option(
    "--draft-id", default=None, help="Existing draft deposition ID to update."
)
@click.option(
    "--title",
    default="FIS-EURIS Inland Waterway Network Dataset",
    help="Deposition title.",
)
@click.option(
    "--license", default="cc-by-4.0", help="Dataset license (Zenodo identifier)."
)
@click.option(
    "--output-dir",
    default="output",
    type=click.Path(exists=True),
    help="Directory containing processing outputs.",
)
@click.option(
    "--publish", is_flag=True, help="Automatically publish (submit) the deposition."
)
def publish_zenodo(token, base_id, draft_id, title, license, output_dir, publish):
    """Publish processed artifacts to Zenodo (supports versioning and draft updates)."""
    if not token:
        msg = "Zenodo access token not provided. Set ZENODO_KEY environment variable."
        logger.error(msg)
        raise click.ClickException(msg)

    # Load description from Markdown file
    desc_path = pathlib.Path("docs/ZENODO_DESCRIPTION.md")
    if desc_path.exists():
        logger.info(f"Loading description from {desc_path}")
        with open(desc_path, "r") as f:
            description = _md_to_html(f.read())
    else:
        description = "FIS-EURIS Inland Waterway Network Dataset. Data provided by EuRIS and Rijkswaterstaat."

    # Define authors
    creators = [
        {
            "name": "Baart, Fedor",
            "affiliation": "Rijkswaterstaat",
            "orcid": "0000-0001-8231-094X",
        },
        {
            "name": "Turpijn, Bas",
            "affiliation": "Rijkswaterstaat",
            "orcid": "0009-0002-6779-1065",
        },
    ]

    output_path = pathlib.Path(output_dir)
    upload_files = []
    zip_to_cleanup = []

    try:
        # 1. Prepare Artifacts
        logger.info("Preparing artifacts for upload...")

        def find_file(
            rel_path: str, artifact_name: Optional[str] = None
        ) -> Optional[pathlib.Path]:
            """Robustly find a file in output/ or output/artifact-name/."""
            # 1. Try direct path
            p = output_path / rel_path
            if p.exists():
                return p
            # 2. Try inside artifact-named subfolder (as created by GitHub Actions download-artifact)
            if artifact_name:
                p = output_path / artifact_name / rel_path
                if p.exists():
                    return p
            return None

        def find_dir(
            rel_path: str, artifact_name: Optional[str] = None
        ) -> Optional[pathlib.Path]:
            """Robustly find a directory in output/ or output/artifact-name/."""
            # Same logic as find_file
            p = output_path / rel_path
            if p.exists() and p.is_dir():
                return p
            if artifact_name:
                p = output_path / artifact_name / rel_path
                if p.exists() and p.is_dir():
                    return p
                # If the entire directory IS the artifact
                p = output_path / artifact_name
                if (
                    p.exists() and p.is_dir() and (p / ".git").exists() is False
                ):  # Just a safety check
                    # We might be looking for output/fis-export but only output/fis-export/ exists
                    # where the content is already what we want.
                    # This is tricky because rel_path might be 'fis-export'.
                    pass
            return None

        # Main directory files (Direct upload)
        # We try both flattened and nested (GA style) paths
        candidates = [
            ("merged-graph/graph.pickle", "merged-graph"),
            ("merged-graph/edges.geojson", "merged-graph"),
            ("merged-graph/edges.geoparquet", "merged-graph"),
            ("merged-graph/nodes.geojson", "merged-graph"),
            ("merged-graph/nodes.geoparquet", "merged-graph"),
            ("merged_validation_report.md", "merged-validation-report"),
            ("fis_validation_report.md", "fis-validation-report"),
        ]

        for rel, art in candidates:
            f = find_file(rel, art)
            if f:
                upload_files.append(f)
            else:
                logger.warning(f"Important file {rel} (artifact: {art}) missing.")

        # Zipped Raw/Full Exports
        def stage_zip(zip_name, paths_with_artifacts):
            resolved_paths = []
            for rel, art in paths_with_artifacts:
                d = find_dir(rel, art)
                if d:
                    resolved_paths.append(d)
                else:
                    logger.warning(
                        f"Directory {rel} (artifact: {art}) missing for {zip_name}."
                    )

            if resolved_paths:
                _create_zip(zip_name, resolved_paths)
                upload_files.append(pathlib.Path(zip_name))
                zip_to_cleanup.append(zip_name)

        logger.info("Creating supplemental zip archives...")
        stage_zip(
            "fis-export.zip",
            [("fis-export", "fis-export"), ("fis-enriched", "fis-enriched")],
        )
        stage_zip(
            "euris-export.zip",
            [("euris-export", "euris-export"), ("euris-enriched", "euris-enriched")],
        )
        stage_zip(
            "schematizations.zip",
            [
                ("lock-schematization", "schematize-lock"),
                ("bridge-schematization", "schematize-bridge"),
                (
                    "dropins-schematization-detailed",
                    "integrated-schematization-detailed",
                ),
                (
                    "dropins-schematization-simplified",
                    "integrated-schematization-simplified",
                ),
                (
                    "integrated-schematization-with-berths",
                    "integrated-schematization-with-berths",
                ),
                ("dropins-euris-detailed", "dropins-euris-detailed"),  # local name
            ],
        )

        # 2. Interact with Zenodo API
        base_url = "https://zenodo.org/api/deposit/depositions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        if draft_id:
            logger.info(f"Updating existing draft ID: {draft_id}...")
            r = requests.get(
                f"{base_url}/{draft_id}", headers=headers, timeout=ZENODO_TIMEOUT
            )
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            bucket_url = deposition["links"]["bucket"]

            logger.info(f"Cleaning up existing files in draft {deposition_id}...")
            for existing_file in deposition.get("files", []):
                file_id = existing_file["id"]
                requests.delete(
                    f"{base_url}/{deposition_id}/files/{file_id}",
                    headers=headers,
                    timeout=ZENODO_TIMEOUT,
                ).raise_for_status()

        elif base_id:
            logger.info(f"Creating new version from base ID: {base_id}...")
            r = requests.post(
                f"{base_url}/{base_id}/actions/newversion",
                headers=headers,
                timeout=ZENODO_TIMEOUT,
            )
            r.raise_for_status()
            new_version_url = r.json()["links"]["latest_draft"]
            r = requests.get(new_version_url, headers=headers, timeout=ZENODO_TIMEOUT)
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            bucket_url = deposition["links"]["bucket"]
            logger.info(f"Created new draft version ID: {deposition_id}")
            logger.info("Cleaning up files from previous version in draft...")
            for existing_file in deposition.get("files", []):
                file_id = existing_file["id"]
                requests.delete(
                    f"{base_url}/{deposition_id}/files/{file_id}",
                    headers=headers,
                    timeout=ZENODO_TIMEOUT,
                ).raise_for_status()
        else:
            logger.info("Creating new Zenodo deposition...")
            r = requests.post(
                base_url, json={}, headers=headers, timeout=ZENODO_TIMEOUT
            )
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            bucket_url = deposition["links"]["bucket"]
            logger.info(f"Created deposition ID: {deposition_id}")

        # 3. Upload Files
        for file_path in upload_files:
            filename = file_path.name
            logger.info(f"Uploading {filename}...")
            with open(file_path, "rb") as fp:
                r = requests.put(
                    f"{bucket_url}/{filename}",
                    data=fp,
                    headers=headers,
                    timeout=ZENODO_TIMEOUT,
                )
                r.raise_for_status()
            logger.info(f"Successfully uploaded {filename}")

        # 4. Update Metadata
        logger.info("Updating deposition metadata...")
        meta_data = {
            "metadata": {
                "title": title,
                "upload_type": "dataset",
                "description": description,
                "creators": creators,
                "license": license,
            }
        }
        r = requests.put(
            f"{base_url}/{deposition_id}",
            json=meta_data,
            headers=headers,
            timeout=ZENODO_TIMEOUT,
        )
        r.raise_for_status()

        logger.info("Successfully updated metadata.")

        if publish:
            logger.info("Publishing (submitting) deposition...")
            r = requests.post(
                f"{base_url}/{deposition_id}/actions/publish",
                headers=headers,
                timeout=ZENODO_TIMEOUT,
            )
            r.raise_for_status()
            logger.info("Deposition published successfully!")

        logger.info(
            f"Deposition complete! URL: https://zenodo.org/deposit/{deposition_id}"
        )

    except Exception as e:
        logger.exception(f"Failed to publish to Zenodo: {e}")
        raise click.ClickException(f"Failed to publish to Zenodo: {e}")
    finally:
        logger.info("Cleaning up temporary zip files...")
        for f in zip_to_cleanup:
            if os.path.exists(f):
                os.remove(f)


def _create_zip(zip_name: str, paths: List[pathlib.Path]):
    """Helper to create a zip file from a list of directories or files."""
    logger.info(f"Creating {zip_name} from {len(paths)} paths...")
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in paths:
            if not path.exists():
                logger.warning(f"Path {path} does not exist, skipping.")
                continue
            if path.is_file():
                zf.write(path, arcname=path.name)
            else:
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = pathlib.Path(root) / file
                        # Preserve relative structure within the zip
                        rel_path = file_path.relative_to(path.parent)
                        zf.write(file_path, arcname=rel_path)
