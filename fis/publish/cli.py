import logging
import os
import pathlib
import zipfile
import click
import requests
from typing import List

logger = logging.getLogger(__name__)


@click.group(name="publish")
def publish_cli():
    """Publish dataset artifacts to external repositories."""
    pass


def _md_to_html(md_text: str) -> str:
    """Very basic markdown to HTML conversion for Zenodo description."""
    import re

    html = md_text
    # Headers
    html = re.sub(r"^# (.*)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.*)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.*)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    # Bold
    html = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", html)
    # Lists
    html = re.sub(r"^- (.*)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    # Links
    html = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', html)
    # Paragraphs (simple)
    html = html.replace("\n\n", "</p><p>")
    return f"<p>{html}</p>"


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
        logger.error(
            "Zenodo access token not provided. Set ZENODO_KEY environment variable."
        )
        return

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

        # Main directory files (Direct upload)
        merged_path = output_path / "merged-graph"
        important_files = [
            merged_path / "graph.pickle",
            merged_path / "edges.geojson",
            merged_path / "edges.geoparquet",
            merged_path / "nodes.geojson",
            merged_path / "nodes.geoparquet",
            output_path / "merged_validation_report.md",
            output_path / "fis_validation_report.md",
        ]
        for f in important_files:
            if f.exists():
                upload_files.append(f)
            else:
                logger.warning(f"Important file {f} missing.")

        # Zipped Raw/Full Exports
        def stage_zip(name, paths):
            _create_zip(name, paths)
            upload_files.append(pathlib.Path(name))
            zip_to_cleanup.append(name)

        logger.info("Creating supplemental zip archives...")
        stage_zip(
            "fis-export.zip", [output_path / "fis-export", output_path / "fis-enriched"]
        )
        stage_zip(
            "euris-export.zip",
            [output_path / "euris-export", output_path / "euris-enriched"],
        )
        stage_zip(
            "schematizations.zip",
            [
                output_path / "lock-schematization",
                output_path / "bridge-schematization",
                output_path / "dropins-fis-detailed",
                output_path / "dropins-fis-simplified",
                output_path / "dropins-euris-detailed",
            ],
        )

        # 2. Interact with Zenodo API
        base_url = "https://zenodo.org/api/deposit/depositions"
        headers = {"Content-Type": "application/json"}
        params = {"access_token": token}

        if draft_id:
            logger.info(f"Updating existing draft ID: {draft_id}...")
            r = requests.get(f"{base_url}/{draft_id}", params=params)
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            bucket_url = deposition["links"]["bucket"]

            logger.info(f"Cleaning up existing files in draft {deposition_id}...")
            for existing_file in deposition.get("files", []):
                file_id = existing_file["id"]
                requests.delete(
                    f"{base_url}/{deposition_id}/files/{file_id}", params=params
                ).raise_for_status()

        elif base_id:
            logger.info(f"Creating new version from base ID: {base_id}...")
            r = requests.post(f"{base_url}/{base_id}/actions/newversion", params=params)
            r.raise_for_status()
            new_version_url = r.json()["links"]["latest_draft"]
            r = requests.get(new_version_url, params=params)
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            bucket_url = deposition["links"]["bucket"]
            logger.info(f"Created new draft version ID: {deposition_id}")
            logger.info("Cleaning up files from previous version in draft...")
            for existing_file in deposition.get("files", []):
                file_id = existing_file["id"]
                requests.delete(
                    f"{base_url}/{deposition_id}/files/{file_id}", params=params
                ).raise_for_status()
        else:
            logger.info("Creating new Zenodo deposition...")
            r = requests.post(base_url, params=params, json={}, headers=headers)
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
                r = requests.put(f"{bucket_url}/{filename}", data=fp, params=params)
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
            params=params,
            json=meta_data,
            headers=headers,
        )
        r.raise_for_status()

        logger.info("Successfully updated metadata.")

        if publish:
            logger.info("Publishing (submitting) deposition...")
            r = requests.post(
                f"{base_url}/{deposition_id}/actions/publish", params=params
            )
            r.raise_for_status()
            logger.info("Deposition published successfully!")

        logger.info(
            f"Deposition complete! URL: https://zenodo.org/deposit/{deposition_id}"
        )

    except Exception as e:
        logger.exception(f"Failed to publish to Zenodo: {e}")
    finally:
        logger.info("Cleaning up temporary zip files...")
        for f in zip_to_cleanup:
            if os.path.exists(f):
                os.remove(f)


def _create_zip(zip_name: str, paths: List[pathlib.Path]):
    """Helper to create a zip file from a list of directories or files."""
    logger.info(f"Creating {zip_name}...")
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
