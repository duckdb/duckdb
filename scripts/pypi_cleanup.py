import pyotp
import datetime
import logging
import os
import re
import sys
import time
from html.parser import HTMLParser
from textwrap import dedent
from urllib.parse import urlparse

import requests
from requests.exceptions import RequestException

# deletes old dev wheels from pypi. evil hack.
# how many days to retain dev releases - all dev releases older than 10 days are deleted
retain_days = 10
patterns = [re.compile(r".*\.dev\d+$")]
actually_delete = True
host = 'https://pypi.org/'

pypi_username = os.getenv('PYPI_CLEANUP_USERNAME', "")
pypi_password = os.getenv("PYPI_CLEANUP_PASSWORD", "")
pypi_otp = os.getenv("PYPI_CLEANUP_OTP", "")
if pypi_username == "":
    print(f'need username in PYPI_CLEANUP_USERNAME env variable')
    exit(1)
if pypi_password == "":
    print(f'need {pypi_username}\' PyPI password in PYPI_CLEANUP_PASSWORD env variable')
    exit(1)
if pypi_otp == "":
    print(f'need {pypi_username}\' PyPI OTP secret in PYPI_CLEANUP_OTP env variable')
    exit(1)


###### NOTE: This code is taken from the pypi-cleanup package (https://github.com/arcivanov/pypi-cleanup/tree/master)
class CsfrParser(HTMLParser):
    def __init__(self, target, contains_input=None):
        super().__init__()
        self._target = target
        self._contains_input = contains_input
        self.csrf = None  # Result value from all forms on page
        self._csrf = None  # Temp value from current form
        self._in_form = False  # Currently parsing a form with an action we're interested in
        self._input_contained = False  # Input field requested is contained in the current form

    def handle_starttag(self, tag, attrs):
        if tag == "form":
            attrs = dict(attrs)
            action = attrs.get("action")  # Might be None.
            if action and (action == self._target or action.startswith(self._target)):
                self._in_form = True
            return

        if self._in_form and tag == "input":
            attrs = dict(attrs)
            if attrs.get("name") == "csrf_token":
                self._csrf = attrs["value"]

            if self._contains_input and attrs.get("name") == self._contains_input:
                self._input_contained = True

            return

    def handle_endtag(self, tag):
        if tag == "form":
            self._in_form = False
            # If we're in a right form that contains the requested input and csrf is not set
            if (not self._contains_input or self._input_contained) and not self.csrf:
                self.csrf = self._csrf
            return


class PypiCleanup:
    def __init__(self, url, username, package, password, otp, patterns, retain_days, delete):
        self.url = urlparse(url).geturl()
        if self.url[-1] == "/":
            self.url = self.url[:-1]
        self.username = username
        self.password = password
        self.otp = otp
        self.do_it = delete
        self.package = package
        self.patterns = patterns
        self.verbose = True
        self.date = datetime.datetime.now() - datetime.timedelta(days=retain_days)

    def run(self):
        csrf = None

        if self.verbose:
            logging.root.setLevel(logging.DEBUG)

        if self.do_it:
            logging.warning("!!! WILL ACTUALLY DELETE THINGS !!!")
            logging.warning("Will sleep for 3 seconds - Ctrl-C to abort!")
            time.sleep(3.0)
        else:
            logging.info("Running in DRY RUN mode")

        logging.info(f"Will use the following patterns {self.patterns} on package {self.package}")

        with requests.Session() as s:
            with s.get(f"{self.url}/pypi/{self.package}/json") as r:
                try:
                    r.raise_for_status()
                except RequestException as e:
                    logging.error(f"Unable to find package {repr(self.package)}", exc_info=e)
                    return 1

                releases_by_date = {}
                for release, files in r.json()["releases"].items():
                    releases_by_date[release] = max(
                        [datetime.datetime.strptime(f["upload_time"], '%Y-%m-%dT%H:%M:%S') for f in files]
                    )

            if not releases_by_date:
                logging.info(f"No releases for package {self.package} have been found")
                return

            pkg_vers = list(
                filter(
                    lambda k: any(filter(lambda rex: rex.match(k), self.patterns)) and releases_by_date[k] < self.date,
                    releases_by_date.keys(),
                )
            )

            if not pkg_vers:
                logging.info(f"No releases were found matching specified patterns and dates in package {self.package}")
                return

            if set(pkg_vers) == set(releases_by_date.keys()):
                print(
                    dedent(
                        f"""
                WARNING:
                \tYou have selected the following patterns: {self.patterns}
                \tThese patterns would delete all available released versions of `{self.package}`.
                \tThis will render your project/package permanently inaccessible.
                \tSince the costs of an error are too high I'm refusing to do this.
                \tGoodbye.
                """
                    ),
                    file=sys.stderr,
                )

                if not self.do_it:
                    return 3
            for pkg in pkg_vers:
                if 'dev' not in pkg:
                    raise Exception(f"Would be deleting version {pkg} but the version is not a dev version")

            if self.username is None:
                raise Exception("No username provided")

            if self.password is None:
                raise Exception("No password provided")

            with s.get(f"{self.url}/account/login/") as r:
                r.raise_for_status()
                form_action = "/account/login/"
                parser = CsfrParser(form_action)
                parser.feed(r.text)
                if not parser.csrf:
                    raise ValueError(f"No CSFR found in {form_action}")
                csrf = parser.csrf

            two_factor = False
            with s.post(
                f"{self.url}/account/login/",
                data={"csrf_token": csrf, "username": self.username, "password": self.password},
                headers={"referer": f"{self.url}/account/login/"},
            ) as r:
                r.raise_for_status()
                if r.url == f"{self.url}/account/login/":
                    logging.error(f"Login for user {self.username} failed")
                    return 1

                if r.url.startswith(f"{self.url}/account/two-factor/"):
                    form_action = r.url[len(self.url) :]
                    parser = CsfrParser(form_action)
                    parser.feed(r.text)
                    if not parser.csrf:
                        raise ValueError(f"No CSFR found in {form_action}")
                    csrf = parser.csrf
                    two_factor = True
                    two_factor_url = r.url

            if two_factor:
                success = False
                for i in range(3):
                    auth_code = pyotp.TOTP(self.otp).now()
                    with s.post(
                        two_factor_url,
                        data={"csrf_token": csrf, "method": "totp", "totp_value": auth_code},
                        headers={"referer": two_factor_url},
                    ) as r:
                        r.raise_for_status()
                        if r.url == two_factor_url:
                            logging.error(f"Authentication code {auth_code} is invalid, retrying in 5 seconds...")
                            time.sleep(5)
                        else:
                            success = True
                            break
                if not success:
                    raise Exception("Could not authenticate with OTP")

            for pkg_ver in pkg_vers:
                if 'dev' not in pkg_ver:
                    raise Exception(f"Would be deleting version {pkg_ver} but the version is not a dev version")
                if self.do_it:
                    logging.info(f"Deleting {self.package} version {pkg_ver}")
                    form_action = f"/manage/project/{self.package}/release/{pkg_ver}/"
                    form_url = f"{self.url}{form_action}"
                    with s.get(form_url) as r:
                        r.raise_for_status()
                        parser = CsfrParser(form_action, "confirm_delete_version")
                        parser.feed(r.text)
                        if not parser.csrf:
                            raise ValueError(f"No CSFR found in {form_action}")
                        csrf = parser.csrf
                        referer = r.url

                    with s.post(
                        form_url,
                        data={
                            "csrf_token": csrf,
                            "confirm_delete_version": pkg_ver,
                        },
                        headers={"referer": referer},
                    ) as r:
                        r.raise_for_status()

                    logging.info(f"Deleted {self.package} version {pkg_ver}")
                else:
                    logging.info(f"Would be deleting {self.package} version {pkg_ver}, but not doing it!")


PypiCleanup(host, pypi_username, 'duckdb', pypi_password, pypi_otp, patterns, retain_days, actually_delete).run()
