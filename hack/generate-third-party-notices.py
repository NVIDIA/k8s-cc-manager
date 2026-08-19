#!/usr/bin/env python3
# Copyright (c) NVIDIA CORPORATION & AFFILIATES.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regenerate with `make -f deployments/container/Makefile third-party-notices`.

Verify a built image with the `verify-third-party-notices` target beside it.
"""

from __future__ import annotations

import argparse
import base64
import csv
import dataclasses
import email.message
import email.parser
import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
from typing import NoReturn

MINIMUM_PYTHON = (3, 9)

GENERATOR_SCRIPT = pathlib.Path(__file__).resolve()
REPO_ROOT = GENERATOR_SCRIPT.parent.parent

REQUIREMENTS_FILE = REPO_ROOT / "requirements.txt"
DOCKERFILE = REPO_ROOT / "deployments" / "container" / "Dockerfile.distroless"
NOTICES_FILE = REPO_ROOT / "THIRD_PARTY_NOTICES.md"

RELEASED_IMAGE = "nvcr.io/nvidia/k8s-cc-manager"

BUILDER_STAGE = "builder"
BUILDER_TAG = "k8s-cc-manager-notices-builder"
BUILDER_DEPS_PATH = "/build/deps"

IMAGE_DEPS_PATH = "/usr/local"

BUILD_PLATFORM = "linux/amd64"

LICENSE_FILENAME_RE = re.compile(
    r"^(LICEN[CS]E|COPYING|COPYRIGHT|NOTICE|AUTHORS|PATENTS)([-._].*)?$",
    re.IGNORECASE,
)

# "BSD License" is deliberately absent: it does not say which BSD variant.
SPDX_ALIASES = {
    "Apache 2.0": "Apache-2.0",
    "Apache License 2.0": "Apache-2.0",
    "Apache License Version 2.0": "Apache-2.0",
    "Apache License, Version 2.0": "Apache-2.0",
    "Apache Software License": "Apache-2.0",
    "MIT License": "MIT",
}

KNOWN_SPDX_IDS = {
    "Apache-2.0",
    "BSD-3-Clause",
    "ISC",
    "MIT",
    "MPL-2.0",
    "PSF-2.0",
}

SPDX_OPERATOR_RE = re.compile(r"\s+(?:AND|OR|WITH)\s+")

PACKAGE_LICENSE_RULINGS = {
    "python-dateutil": ("Dual License", "Apache-2.0 AND BSD-3-Clause"),
}

UNRESOLVED_LICENSE = "See license text below"
NO_LICENSE_TEXT = (
    "License text unavailable in the distributed package. See upstream source "
    "for the full license."
)

COMMON_LICENSES = {"Apache-2.0", "MIT", "BSD-3-Clause", "ISC"}

RECORD_MINIMUM_FIELDS = 2

NOTICES_HEADER_TEMPLATE = """# Third-Party Notices

NVIDIA CC Manager for Kubernetes

This file lists the third-party **Python distributions** installed into the
released `{image}` container image, along with the verbatim
text of each distribution's license. It is a snapshot of what the build
installed when this file was generated: `{requirements}` carries no lock file,
so a later build may install newer versions, and may pull in distributions
that are not listed here. Regenerate it whenever the image is released.

Third-party code that reaches the image by another route is named below rather
than listed above. The image uses `nvcr.io/nvidia/distroless/python` as a base
image, which provides the Python interpreter and its standard library. All of
the OSS packages and source included in that image can be found at
<https://developer.nvidia.com/w/distroless-oss/index.html>. A statically
compiled `/bin/rm` is added to the image; its source is NVIDIA's own, but it is
linked against musl libc. NVIDIA's own code, including the bundled copy of
[NVIDIA/gpu-admin-tools](https://github.com/NVIDIA/gpu-admin-tools), is not a
third-party dependency and is not listed here.
"""


@dataclasses.dataclass(frozen=True)
class Distribution:
    name: str
    version: str
    declared_license: str
    source_url: str
    license_texts: dict[str, str]
    license: str


def fail(message: str) -> NoReturn:
    raise SystemExit(f"ERROR: {message}")


def fail_listing(names: list[str], message: str) -> NoReturn:
    for name in names:
        print(f"  {name}", file=sys.stderr)
    fail(message)


def normalize_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()  # PEP 503


def repo_relative(path: pathlib.Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def is_inside_directory(path: pathlib.Path, directory: pathlib.Path) -> bool:
    """Tolerate a path that does not exist, which a RECORD entry often is."""
    try:
        return path.resolve().is_relative_to(directory)
    except (OSError, RuntimeError):
        return False


def code_fence_for(text: str) -> str:
    """Outgrow any backtick run, so Markdown licence text cannot close early."""
    longest_backtick_run = max(
        (len(backtick_run) for backtick_run in re.findall(r"`+", text)),
        default=0,
    )
    return "`" * max(3, longest_backtick_run + 1)


def write_atomically(path: pathlib.Path, text: str) -> None:
    """Keep the temp file beside the target: replace is atomic per filesystem."""
    temp_fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".")
    temp_path = pathlib.Path(temp_name)
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        temp_path.chmod(0o644)
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def docker(
    docker_args: list[str], capture: bool = False, stream: bool = False
) -> str:
    try:
        result = subprocess.run(
            ["docker", *docker_args],
            capture_output=not stream,
            stdout=sys.stderr if stream else None,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        fail(
            "docker is not installed. Regenerating reads the Python "
            "distributions out of a container image, so it needs Docker and "
            "network access."
        )
    if result.returncode != 0:
        if not stream:
            sys.stderr.write(result.stdout)
            sys.stderr.write(result.stderr)
        fail(f"docker {docker_args[0]} failed.")
    return result.stdout if capture else ""


def copy_deps_from_image(
    image: str, deps_path: str, work_dir: pathlib.Path
) -> pathlib.Path:
    container_id = docker(
        ["create", "--platform", BUILD_PLATFORM, image], capture=True
    ).strip()
    if not container_id:
        fail("docker create returned no container id")
    try:
        docker(["cp", f"{container_id}:{deps_path}/lib", str(work_dir)])
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container_id],
            capture_output=True, check=False,
        )
    return work_dir / "lib"


def build_and_extract_deps(work_dir: pathlib.Path) -> pathlib.Path:
    if not DOCKERFILE.is_file():
        fail(f"Dockerfile not found: {DOCKERFILE}")

    print(
        f"Building {repo_relative(DOCKERFILE)} target '{BUILDER_STAGE}' for "
        f"{BUILD_PLATFORM}...",
        file=sys.stderr,
    )
    docker([
        "build",
        "--pull",
        "--no-cache",
        "--platform", BUILD_PLATFORM,
        "--target", BUILDER_STAGE,
        "--file", str(DOCKERFILE),
        "--tag", BUILDER_TAG,
        str(REPO_ROOT),
    ], stream=True)

    return copy_deps_from_image(BUILDER_TAG, BUILDER_DEPS_PATH, work_dir)


def extract_deps_from_image(image: str, work_dir: pathlib.Path) -> pathlib.Path:
    print(f"Reading {image} for {BUILD_PLATFORM}...", file=sys.stderr)
    return copy_deps_from_image(image, IMAGE_DEPS_PATH, work_dir)


def find_site_packages(lib_dir: pathlib.Path) -> pathlib.Path:
    site_packages_dirs = sorted(lib_dir.glob("python*/site-packages"))
    if len(site_packages_dirs) != 1:
        relative_paths = [
            str(path.relative_to(lib_dir)) for path in site_packages_dirs
        ]
        fail(
            "expected exactly one python*/site-packages in the extracted lib "
            f"directory, found {len(site_packages_dirs)}: {relative_paths}"
        )
    return site_packages_dirs[0]


def record_agreement(
    dist_info: pathlib.Path, site_packages: pathlib.Path
) -> tuple[int, int]:
    """Count an absent file as checked and not matching.

    Ignoring it would let one surviving file give stale metadata a perfect
    score.
    """
    record_file = dist_info / "RECORD"
    if not record_file.is_file():
        return 0, 0
    site_packages_directory = site_packages.resolve()
    matching_files = checked_files = 0
    with record_file.open(
        newline="", encoding="utf-8", errors="replace"
    ) as handle:
        for row in csv.reader(handle):
            if len(row) < RECORD_MINIMUM_FIELDS or not row[1].startswith("sha256="):
                continue
            recorded_path, recorded_hash = row[0], row[1].split("=", 1)[1]
            if ".dist-info/" in recorded_path:
                continue
            installed_file = site_packages / recorded_path
            if not is_inside_directory(installed_file, site_packages_directory):
                continue
            checked_files += 1
            if not installed_file.is_file():
                continue
            digest = base64.urlsafe_b64encode(
                hashlib.sha256(installed_file.read_bytes()).digest()
            ).rstrip(b"=").decode()
            matching_files += digest == recorded_hash
    return matching_files, checked_files


def choose_installed_dist_info(
    dist_name: str, dist_infos: list[pathlib.Path], site_packages: pathlib.Path
) -> pathlib.Path:
    scored_dist_infos = []
    for dist_info in dist_infos:
        matching_files, checked_files = record_agreement(dist_info, site_packages)
        agreement = matching_files / checked_files if checked_files else -1.0
        scored_dist_infos.append((agreement, checked_files, dist_info))
    scored_dist_infos.sort(key=lambda entry: entry[0], reverse=True)

    best_agreement, best_checked, best_dist_info = scored_dist_infos[0]
    runner_up_agreement = scored_dist_infos[1][0]
    if best_checked == 0 or best_agreement <= runner_up_agreement:
        listed_names = ", ".join(
            sorted(dist_info.name for dist_info in dist_infos)
        )
        fail(
            f"{dist_name} has more than one .dist-info ({listed_names}) and "
            "their RECORD hashes do not say which is installed. Remove the "
            f"superseded metadata in {repo_relative(DOCKERFILE)} so the image "
            "describes itself."
        )

    ignored_names = ", ".join(
        info.name for _, _, info in scored_dist_infos[1:]
    )
    print(
        f"  {dist_name}: {best_dist_info.name} is installed "
        f"({best_agreement:.0%} of RECORD matches disk); ignoring "
        f"{ignored_names}",
        file=sys.stderr,
    )
    return best_dist_info


def read_verbatim(path: pathlib.Path) -> str:
    """Decode strictly: replacing a byte would edit the text being reproduced."""
    try:
        return path.read_bytes().decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"{path} is not valid UTF-8 and cannot be reproduced: {error}")


def resolve_license_file(
    candidate_path: pathlib.Path, dist_info_directory: pathlib.Path
) -> pathlib.Path | None:
    """Refuse a License-File that resolves outside its dist-info.

    The value is package-controlled, so an absolute path, a '..' segment or a
    symlink could otherwise copy a host file into the notices. One that escapes
    stops the run rather than returning None.
    """
    try:
        resolved_path = candidate_path.resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if not is_inside_directory(resolved_path, dist_info_directory):
        fail(
            f"license file {candidate_path} resolves to {resolved_path}, "
            f"outside {dist_info_directory}. Refusing to reproduce a file from "
            "outside the package."
        )
    return resolved_path if resolved_path.is_file() else None


def find_source_url(metadata: email.message.Message) -> str:
    home_page = (metadata.get("Home-page") or "").strip()
    if home_page:
        return home_page

    project_urls = {}
    for entry in metadata.get_all("Project-URL") or []:
        label, _, target_url = entry.partition(",")
        project_urls[label.strip().lower()] = target_url.strip()
    for label in ("source", "source code", "repository", "homepage",
                  "documentation"):
        if label in project_urls:
            return project_urls[label]
    return ""


def collect_license_texts(
    dist_info: pathlib.Path, declared_filenames: list[str]
) -> dict[str, str]:
    dist_info_directory = dist_info.resolve()
    license_texts: dict[str, str] = {}
    captured_paths: set[pathlib.Path] = set()
    for declared_filename in declared_filenames:
        filename = declared_filename.strip()
        for candidate_path in (
            dist_info / "licenses" / filename,
            dist_info / "license_files" / filename,
            dist_info / filename,
        ):
            resolved_path = resolve_license_file(
                candidate_path, dist_info_directory
            )
            if resolved_path is None:
                continue
            license_texts[filename] = read_verbatim(resolved_path)
            captured_paths.add(resolved_path)
            break
        else:
            fail(
                f"{dist_info.name} declares License-File {filename!r} but no "
                "such file is present. The distribution is incomplete; obtain "
                "the text from the upstream project."
            )

    for path in sorted(dist_info.rglob("*")):
        if not LICENSE_FILENAME_RE.match(path.name):
            continue
        resolved_path = resolve_license_file(path, dist_info_directory)
        if resolved_path is None or resolved_path in captured_paths:
            continue
        license_texts.setdefault(
            str(path.relative_to(dist_info)), read_verbatim(resolved_path)
        )
    return license_texts


def read_dist_info(dist_info: pathlib.Path) -> Distribution:
    metadata_file = dist_info / "METADATA"
    if not metadata_file.is_file():
        fail(f"no METADATA in {dist_info.name}")
    metadata = email.parser.Parser().parsestr(read_verbatim(metadata_file))

    declared_license = (
        metadata.get("License-Expression") or metadata.get("License") or ""
    ).strip()
    source_url = find_source_url(metadata)
    license_texts = collect_license_texts(
        dist_info, metadata.get_all("License-File") or []
    )

    version = (metadata.get("Version") or "").strip()
    if not version:
        fail(f"{dist_info.name} declares no Version")

    dist_name = normalize_distribution_name(
        metadata.get("Name") or dist_info.name.split("-")[0]
    )
    return Distribution(
        name=dist_name,
        version=version,
        declared_license=declared_license,
        source_url=source_url or "n/a",
        license_texts=license_texts,
        license=resolve_license(dist_name, declared_license),
    )


def is_known_spdx(expression: str) -> bool:
    identifiers = [
        identifier.strip(" ()")
        for identifier in SPDX_OPERATOR_RE.split(expression)
    ]
    return bool(identifiers) and all(
        identifier in KNOWN_SPDX_IDS for identifier in identifiers
    )


def resolve_license(dist_name: str, declared_license: str) -> str:
    """Leave an unrecognised declaration unresolved.

    Echoing it back would record a licence nobody has read, and the run would
    pass.
    """
    ruling = PACKAGE_LICENSE_RULINGS.get(dist_name)
    if ruling is not None:
        expected_declaration, spdx_id = ruling
        if declared_license == expected_declaration:
            return spdx_id
        fail(
            f"{dist_name} declares license {declared_license!r}, but this tool "
            f"carries a ruling written for {expected_declaration!r}. Re-read "
            "the package's license files and update PACKAGE_LICENSE_RULINGS."
        )

    if declared_license in SPDX_ALIASES:
        return SPDX_ALIASES[declared_license]

    first_line = declared_license.split("\n", 1)[0].strip()
    return first_line if is_known_spdx(first_line) else UNRESOLVED_LICENSE


def collect_distributions(site_packages: pathlib.Path) -> list[Distribution]:
    dist_infos_by_name: dict[str, list[pathlib.Path]] = {}
    for dist_info in sorted(site_packages.glob("*.dist-info")):
        directory_stem = dist_info.name[: -len(".dist-info")]
        dist_name = normalize_distribution_name(directory_stem.rsplit("-", 1)[0])
        dist_infos_by_name.setdefault(dist_name, []).append(dist_info)

    if not dist_infos_by_name:
        fail(f"no .dist-info directories under {BUILDER_DEPS_PATH}")

    distributions = []
    for dist_name in sorted(dist_infos_by_name):
        dist_infos = dist_infos_by_name[dist_name]
        dist_info = (
            dist_infos[0] if len(dist_infos) == 1
            else choose_installed_dist_info(dist_name, dist_infos, site_packages)
        )
        distributions.append(read_dist_info(dist_info))
    return distributions


def render(distributions: list[Distribution]) -> str:
    lines: list[str] = []
    emit = lines.append

    emit(NOTICES_HEADER_TEMPLATE.format(
        image=RELEASED_IMAGE,
        requirements=repo_relative(REQUIREMENTS_FILE),
    ))

    emit("## License Summary")
    emit("")
    emit("| License | Distributions |")
    emit("|---------|---------------|")
    for license_name in sorted(
        {distribution.license for distribution in distributions}
    ):
        members = ", ".join(
            f"`{distribution.name}`"
            for distribution in distributions
            if distribution.license == license_name
        )
        emit(f"| {license_name} | {members} |")
    emit("")

    emit("## Python Dependency Index")
    emit("")
    emit("| Distribution | Version | License | Source |")
    emit("|--------------|---------|---------|--------|")
    for distribution in distributions:
        emit(
            f"| `{distribution.name}` | {distribution.version} "
            f"| {distribution.license} | {distribution.source_url} |"
        )
    emit("")

    emit("## Python Dependency License Texts")
    for distribution in distributions:
        emit("")
        emit(f"### {distribution.name}")
        emit("")
        emit(f"* Version: {distribution.version}")
        emit(f"* License: {distribution.license}")
        if (
            distribution.declared_license
            and distribution.declared_license != distribution.license
        ):
            emit(
                "* Declared in package metadata: "
                f"`{distribution.declared_license}`"
            )
        emit(f"* Source: {distribution.source_url}")
        if not distribution.license_texts:
            emit("")
            emit(NO_LICENSE_TEXT)
        for filename in sorted(distribution.license_texts):
            license_body = distribution.license_texts[filename].rstrip("\n")
            fence = code_fence_for(license_body)
            emit("")
            emit(f"#### {filename}")
            emit("")
            emit(f"{fence}text")
            emit(license_body)
            emit(fence)

    return "\n".join(lines) + "\n"


def reject_incomplete(distributions: list[Distribution]) -> None:
    unresolved_names = [
        distribution.name
        for distribution in distributions
        if distribution.license == UNRESOLVED_LICENSE
    ]
    if unresolved_names:
        fail_listing(
            unresolved_names,
            "the distributions above declare no usable license identifier. Read "
            "their license files and add a ruling to PACKAGE_LICENSE_RULINGS.",
        )


def report_unusual_licenses(distributions: list[Distribution]) -> None:
    unusual_distributions = [
        distribution
        for distribution in distributions
        if distribution.license not in COMMON_LICENSES
    ]
    if not unusual_distributions:
        return
    print(
        "\nNOTE: these distributions are under something other than "
        "Apache-2.0, MIT,\nBSD-3-Clause or ISC. Confirm they have been "
        "reviewed:",
        file=sys.stderr,
    )
    for distribution in unusual_distributions:
        print(
            f"  {distribution.name} {distribution.version}: "
            f"{distribution.license}",
            file=sys.stderr,
        )


def main() -> int:
    if sys.version_info < MINIMUM_PYTHON:
        needed = ".".join(str(part) for part in MINIMUM_PYTHON)
        running = ".".join(str(part) for part in sys.version_info[:3])
        fail(
            f"this tool needs Python {needed} or newer, but is running on "
            f"{running}."
        )

    parser = argparse.ArgumentParser(
        description="Generate or validate THIRD_PARTY_NOTICES.md."
    )
    parser.add_argument(
        "--image",
        help="Read this already-built image instead of building one. Point it "
        "at the exact artifact that is about to be published.",
    )
    args = parser.parse_args()

    work_dir = pathlib.Path(tempfile.mkdtemp(prefix="ccm-notices."))
    try:
        lib_dir = (
            extract_deps_from_image(args.image, work_dir)
            if args.image
            else build_and_extract_deps(work_dir)
        )
        distributions = collect_distributions(find_site_packages(lib_dir))
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    reject_incomplete(distributions)

    write_atomically(NOTICES_FILE, render(distributions))

    print(
        f"Wrote {repo_relative(NOTICES_FILE)} ({len(distributions)} distributions)",
        file=sys.stderr,
    )

    report_unusual_licenses(distributions)
    return 0


if __name__ == "__main__":
    sys.exit(main())
