import logging
import os
import pathlib
import zipfile
import tempfile
import click
import requests
import markdown
from typing import List, Optional

logger = logging.getLogger(__name__)

# Standard Zenodo API timeout in seconds
ZENODO_TIMEOUT = 60


@click.group(name="publish")
def publish_cli():
    """Publish dataset artifacts to external repositories."""
    pass


def _validate_mutually_exclusive_ids(ctx, param, value):
    """Ensure that --base-id and --draft-id are not both provided."""
    if value is None:
        return value

    if param.name == "base_id":
        other_name = "draft_id"
    elif param.name == "draft_id":
        other_name = "base_id"
    else:
        return value

    other_value = ctx.params.get(other_name)
    if other_value is not None:
        raise click.UsageError(
            "Options --base-id and --draft-id are mutually exclusive; please provide only one."
        )

    return value


def _md_to_html(md_text: str) -> str:
    """Proper markdown to HTML conversion."""
    return markdown.markdown(md_text)


@publish_cli.command(name="zenodo")
@click.option("--token", envvar="ZENODO_KEY", help="Zenodo API access token.")
@click.option(
    "--base-id",
    default=None,
    callback=_validate_mutually_exclusive_ids,
    help="Base deposition ID to create a new version from.",
)
@click.option(
    "--draft-id",
    default=None,
    callback=_validate_mutually_exclusive_ids,
    help="Existing draft deposition ID to update.",
)
@click.option(
    "--title",
    default="FIS-EURIS Inland Waterway Network Dataset",
    help="Deposition title.",
)
@click.option(
    "--license",
    "license_id",
    default="cc-by-4.0",
    help="Dataset license (Zenodo identifier).",
)
@click.option(
    "--output-dir",
    default="output",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, path_type=pathlib.Path
    ),
    help="Directory containing processing outputs.",
)
@click.option(
    "--publish", is_flag=True, help="Automatically publish (submit) the deposition."
)
@click.option(
    "--allow-partial", is_flag=True, help="Proceed even if some artifacts are missing."
)
def publish_zenodo(
    token, base_id, draft_id, title, license_id, output_dir, publish, allow_partial
):
    """Publish processed artifacts to Zenodo (supports versioning and draft updates)."""
    if not token:
        msg = "Zenodo access token not provided. Set ZENODO_KEY environment variable."
        logger.error(msg)
        raise click.ClickException(msg)

    # Load description from Markdown file
    desc_path = pathlib.Path("docs/ZENODO_DESCRIPTION.md")
    if desc_path.exists():
        logger.info(f"Loading description from {desc_path}")
        with open(desc_path, "r", encoding="utf-8") as f:
            description = _md_to_html(f.read())
    else:
        description = "<p>FIS-EURIS Inland Waterway Network Dataset. Data provided by EuRIS and Rijkswaterstaat.</p>"

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

    upload_files = []

    # Create a temporary directory for zips to avoid clobbering user files
    tmp_zip_dir = pathlib.Path(tempfile.mkdtemp(prefix="fis_zenodo_"))
    logger.info(f"Using temporary directory for archives: {tmp_zip_dir}")

    try:
        # 1. Prepare Artifacts
        logger.info("Preparing artifacts for upload...")

        def find_file(
            rel_path: str, artifact_name: Optional[str] = None
        ) -> Optional[pathlib.Path]:
            """Robustly find a file in output/ or output/artifact-name/."""
            # 1. Try direct path
            p = output_dir / rel_path
            if p.exists() and p.is_file():
                return p
            # 2. Try inside artifact-named subfolder
            if artifact_name:
                p = output_dir / artifact_name / rel_path
                if p.exists() and p.is_file():
                    return p
                # If the artifact name is already the directory, and the rel_path is just the filename
                p = output_dir / artifact_name / pathlib.Path(rel_path).name
                if p.exists() and p.is_file():
                    return p
            return None

        def find_dir(
            rel_path: str, artifact_name: Optional[str] = None
        ) -> Optional[pathlib.Path]:
            """Robustly find a directory in output/ or output/artifact-name/."""
            # 1. Try direct path
            p = output_dir / rel_path
            if p.exists() and p.is_dir():
                return p
            # 2. Try inside artifact-named subfolder
            if artifact_name:
                p = output_dir / artifact_name / rel_path
                if p.exists() and p.is_dir():
                    return p
                # 3. If the entire artifact directory IS the target directory
                p = output_dir / artifact_name
                if p.exists() and p.is_dir():
                    return p
            return None

        # Main directory files (Direct upload)
        candidates = [
            ("merged-graph/graph.pickle", "merged-graph"),
            ("merged-graph/edges.geojson", "merged-graph"),
            ("merged-graph/edges.geoparquet", "merged-graph"),
            ("merged-graph/nodes.geojson", "merged-graph"),
            ("merged-graph/nodes.geoparquet", "merged-graph"),
            ("merged_validation_report.md", "merged-validation-report"),
            ("fis_validation_report.md", "fis-validation-report"),
        ]

        missing_required = False
        for rel, art in candidates:
            f = find_file(rel, art)
            if f:
                upload_files.append(f)
            else:
                msg = f"Required file {rel} (artifact: {art}) missing."
                if not allow_partial:
                    logger.error(msg)
                    missing_required = True
                else:
                    logger.warning(msg)

        # Zipped Raw/Full Exports
        def stage_zip(zip_filename, paths_with_artifacts, required=True):
            nonlocal missing_required
            resolved_paths = []
            for rel, art in paths_with_artifacts:
                d = find_dir(rel, art)
                if d:
                    resolved_paths.append(d)
                else:
                    msg = (
                        f"Directory {rel} (artifact: {art}) missing for {zip_filename}."
                    )
                    if required and not allow_partial:
                        logger.error(msg)
                        missing_required = True
                    else:
                        logger.warning(msg)

            if resolved_paths:
                zip_path = tmp_zip_dir / zip_filename
                _create_zip(zip_path, resolved_paths)
                upload_files.append(zip_path)

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
                ("lock-schematization", "lock-schematization"),
                ("bridge-schematization", "bridge-schematization"),
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
            ],
        )

        if missing_required:
            raise click.ClickException(
                "One or more required dataset artifacts are missing. "
                "Aborting Zenodo publication. Use --allow-partial to override."
            )

        # 2. Interact with Zenodo API
        base_url = "https://zenodo.org/api/deposit/depositions"
        json_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        auth_headers = {"Authorization": f"Bearer {token}"}

        if draft_id:
            logger.info(f"Updating existing draft ID: {draft_id}...")
            r = requests.get(
                f"{base_url}/{draft_id}", headers=auth_headers, timeout=ZENODO_TIMEOUT
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
                    headers=auth_headers,
                    timeout=ZENODO_TIMEOUT,
                ).raise_for_status()

        elif base_id:
            logger.info(f"Creating new version from base ID: {base_id}...")
            r = requests.post(
                f"{base_url}/{base_id}/actions/newversion",
                headers=auth_headers,
                timeout=ZENODO_TIMEOUT,
            )
            r.raise_for_status()
            new_version_url = r.json()["links"]["latest_draft"]
            r = requests.get(
                new_version_url, headers=auth_headers, timeout=ZENODO_TIMEOUT
            )
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
                    headers=auth_headers,
                    timeout=ZENODO_TIMEOUT,
                ).raise_for_status()
        else:
            logger.info("Creating new Zenodo deposition...")
            r = requests.post(
                base_url, json={}, headers=json_headers, timeout=ZENODO_TIMEOUT
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
                # Use auth_headers (no content-type) for binary bucket uploads
                r = requests.put(
                    f"{bucket_url}/{filename}",
                    data=fp,
                    headers=auth_headers,
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
                "license": license_id,
            }
        }
        r = requests.put(
            f"{base_url}/{deposition_id}",
            json=meta_data,
            headers=json_headers,
            timeout=ZENODO_TIMEOUT,
        )
        r.raise_for_status()

        logger.info("Successfully updated metadata.")

        if publish:
            logger.info("Publishing (submitting) deposition...")
            r = requests.post(
                f"{base_url}/{deposition_id}/actions/publish",
                headers=auth_headers,
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
        logger.info("Cleaning up temporary archives...")
        import shutil

        if tmp_zip_dir.exists():
            shutil.rmtree(tmp_zip_dir)


def _create_zip(zip_path: pathlib.Path, paths: List[pathlib.Path]):
    """Helper to create a zip file from a list of directories or files."""
    logger.info(f"Creating {zip_path.name} from {len(paths)} paths...")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
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
