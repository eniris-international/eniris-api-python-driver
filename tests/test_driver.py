"""Tests of the authentication of the ApiDriver, against a stand-in for the
authentication server and the API behind it.

Run them from the root of the repository: python -m unittest discover -s tests
"""

import datetime
import json
import threading
import unittest
from typing import Optional, Union
from unittest import mock

import requests

from eniris import driver as driverModule
from eniris.driver import (
    ApiDriver,
    AuthenticationFailure,
    REJECTED_REFRESHTOKEN_LOGIN_INTERVAL_S,
)

AUTH_URL = "https://authentication.test"
API_URL = "https://api.test"
REJECTED = (
    401,
    json.dumps(
        {"statusCode": 401, "error": "Unauthorized", "message": "INVALID_TOKEN"}
    ),
)


# What a test lets the fake server answer: a status code and a text, or an exception
Answer = Optional[Union["tuple[int, str]", Exception]]


def response(statusCode: int, text: str) -> requests.Response:
    resp = requests.Response()
    resp.status_code = statusCode
    resp._content = text.encode()  # pylint: disable=protected-access
    return resp


class FakeServer:
    """Takes the place of the requests session of a driver. It answers like the
    authentication server, which knows the refresh tokens it handed out, and like an
    API which only accepts the access tokens of that server"""

    def __init__(self):
        self.password = "secret"
        # An answer (status code, text) or an exception to give instead of the
        # normal one
        self.loginAnswer: Answer = None
        self.accesstokenAnswer: Answer = None
        self.apiAnswer: Answer = None
        self.validRefreshtokens = set()
        self.validAccesstokens = set()
        self.logins = 0
        self.renewals = 0
        self.accesstokenRequests = 0
        self.apiRequests = []
        self._lock = threading.Lock()

    def revokeRefreshtokens(self):
        self.validRefreshtokens.clear()

    def get(self, url, **kwargs):
        return self._request("GET", url, kwargs)

    def post(self, url, **kwargs):
        return self._request("POST", url, kwargs)

    def _request(self, method, url, kwargs):
        with self._lock:
            token = kwargs.get("headers", {}).get("Authorization", "")[7:]
            if url == f"{AUTH_URL}/auth/login":
                self.logins += 1
                return self._answer(self.loginAnswer, self._login, kwargs["json"])
            if url == f"{AUTH_URL}/auth/refreshtoken":
                self.renewals += 1
                return self._handOut(
                    token, f"renewed-{self.renewals}", self.validRefreshtokens
                )
            if url == f"{AUTH_URL}/auth/accesstoken":
                self.accesstokenRequests += 1
                return self._answer(
                    self.accesstokenAnswer,
                    self._handOut,
                    token,
                    f"access-{self.accesstokenRequests}",
                    self.validAccesstokens,
                )
            self.apiRequests.append((method, url))
            if token not in self.validAccesstokens:
                return response(401, "INVALID_TOKEN")
            return self._answer(self.apiAnswer, response, 200, "data")

    @staticmethod
    def _answer(answer: Answer, normalAnswer, *args):
        """The answer a test asked for, or else the normal one. A response with an
        error status counts as False, so it cannot be chosen with `or`"""
        if answer is None:
            return normalAnswer(*args)
        if isinstance(answer, Exception):
            raise answer
        return response(*answer)

    def _login(self, credentials):
        if credentials != {"username": "user", "password": self.password}:
            return response(401, "INVALID_CREDENTIALS")
        token = f"refresh-{self.logins}"
        self.validRefreshtokens.add(token)
        return response(200, token)

    def _handOut(self, refreshtoken, newToken, validTokens):
        if refreshtoken not in self.validRefreshtokens:
            return response(*REJECTED)
        validTokens.add(newToken)
        return response(200, newToken)


class AuthenticationTest(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.driver = self.makeDriver()
        self.nowS = 1000.0

    def makeDriver(
        self, username: Optional[str] = "user", password: Optional[str] = "secret"
    ) -> ApiDriver:
        return ApiDriver(
            username,  # type: ignore
            password,  # type: ignore
            authUrl=AUTH_URL,
            apiUrl=API_URL,
            maximumRetries=0,
            session=self.server,  # type: ignore
        )

    def controlTheClock(self):
        """Let self.nowS be the time by which the driver spaces its logins"""
        patcher = mock.patch.object(
            driverModule.time, "monotonic", side_effect=lambda: self.nowS
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def ageAccesstoken(self):
        """Make the driver ask for a new access token at its next call"""
        stored = self.driver.accessDtAndToken
        assert stored is not None
        self.driver.accessDtAndToken = (
            stored[0] - datetime.timedelta(minutes=4),
            stored[1],
        )

    def ageRefreshtoken(self, days: int):
        stored = self.driver.refreshDtAndToken
        assert stored is not None
        self.driver.refreshDtAndToken = (
            stored[0] - datetime.timedelta(days=days),
            stored[1],
        )

    def storedRefreshtoken(self) -> str:
        stored = self.driver.refreshDtAndToken
        assert stored is not None
        return stored[1]

    def loggedInDriverWithRevokedRefreshtoken(self):
        """A driver which worked until the server revoked its refresh token"""
        self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.server.revokeRefreshtokens()
        self.ageAccesstoken()

    def assertRejected(self, text="Unable to collect an access token: .*INVALID_TOKEN"):
        with self.assertRaisesRegex(AuthenticationFailure, text):
            self.driver.accesstoken()

    def testTokensAreReused(self):
        self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.assertEqual(self.server.logins, 1)
        self.assertEqual(self.server.accesstokenRequests, 1)
        self.assertEqual(len(self.server.apiRequests), 2)

    def testRevokedRefreshtokenIsReplacedByLoggingInAgain(self):
        self.loggedInDriverWithRevokedRefreshtoken()
        with self.assertLogs(level="WARNING") as logs:
            self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.assertIn("rejected the refresh token", logs.output[0])
        self.assertEqual(self.server.logins, 2)
        self.assertEqual(self.storedRefreshtoken(), "refresh-2")
        self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.assertEqual(self.server.logins, 2)

    def testRequestIsSentOnceWhenTheRefreshtokenWasRevoked(self):
        self.loggedInDriverWithRevokedRefreshtoken()
        with self.assertLogs(level="WARNING"):
            self.assertEqual(self.driver.post("/v1/device", json={}).text, "data")
        self.assertEqual(
            self.server.apiRequests,
            [("GET", f"{API_URL}/v1/device"), ("POST", f"{API_URL}/v1/device")],
        )

    def testFailedLoginKeepsTheRejectedTokenAndIsNotRepeatedAtOnce(self):
        answers = [
            ((503, "Service Unavailable"), AuthenticationFailure),
            (requests.Timeout(), AuthenticationFailure),
            (requests.ConnectionError(), requests.ConnectionError),
        ]
        for loginAnswer, expectedError in answers:
            with self.subTest(loginAnswer=loginAnswer):
                self.setUp()
                self.controlTheClock()
                self.loggedInDriverWithRevokedRefreshtoken()
                self.server.loginAnswer = loginAnswer
                with self.assertLogs(level="WARNING"), self.assertRaises(expectedError):
                    self.driver.accesstoken()
                self.assertEqual(self.server.logins, 2)
                self.assertEqual(self.storedRefreshtoken(), "refresh-1")

                # The next calls fail on the rejected token without a login
                self.nowS += REJECTED_REFRESHTOKEN_LOGIN_INTERVAL_S - 1
                for _ in range(3):
                    self.assertRejected()
                self.assertEqual(self.server.logins, 2)

                # Once the interval has passed, a login is tried again
                self.server.loginAnswer = None
                self.nowS += 2
                with self.assertLogs(level="WARNING"):
                    self.assertEqual(self.driver.get("/v1/device").text, "data")
                self.assertEqual(self.server.logins, 3)

    def testRefusedCredentialsAreReportedAndNotTriedAtEveryCall(self):
        self.loggedInDriverWithRevokedRefreshtoken()
        self.server.password = "changed"
        with self.assertLogs(level="WARNING"):
            self.assertRejected("Unable to login: INVALID_CREDENTIALS")
        self.assertRejected()
        self.assertEqual(self.server.logins, 2)

    def testNewTokenWhichIsRejectedToo(self):
        self.controlTheClock()
        self.loggedInDriverWithRevokedRefreshtoken()
        self.server.accesstokenAnswer = REJECTED
        with self.assertLogs(level="WARNING"):
            self.assertRejected()
        self.assertEqual(self.server.logins, 2)
        self.assertEqual(self.server.accesstokenRequests, 3)

        self.assertRejected()
        self.assertEqual(self.server.logins, 2)
        self.assertEqual(self.server.accesstokenRequests, 4)

        self.nowS += REJECTED_REFRESHTOKEN_LOGIN_INTERVAL_S + 1
        with self.assertLogs(level="WARNING"):
            self.assertRejected()
        self.assertEqual(self.server.logins, 3)

    def testRejectionRightAfterTheFirstLoginCausesNoSecondLogin(self):
        self.server.accesstokenAnswer = REJECTED
        self.assertRejected()
        self.assertEqual(self.server.logins, 1)
        self.assertEqual(self.server.accesstokenRequests, 1)

    def testRejectionRightAfterTheLoginOfAnOldTokenCausesNoSecondLogin(self):
        self.driver.get("/v1/device")
        self.ageAccesstoken()
        self.ageRefreshtoken(days=14)
        self.server.accesstokenAnswer = REJECTED
        self.assertRejected()
        self.assertEqual(self.server.logins, 2)

    def testRejectionRightAfterARenewalCausesNoLogin(self):
        self.driver.get("/v1/device")
        self.ageAccesstoken()
        self.ageRefreshtoken(days=8)
        self.server.accesstokenAnswer = REJECTED
        self.assertRejected()
        self.assertEqual(self.server.renewals, 1)
        self.assertEqual(self.storedRefreshtoken(), "renewed-1")
        self.assertEqual(self.server.logins, 1)

    def testRevokedTokenWhichCouldNotBeRenewedIsReplaced(self):
        self.loggedInDriverWithRevokedRefreshtoken()
        self.ageRefreshtoken(days=8)
        with self.assertLogs(level="WARNING") as logs:
            self.assertEqual(self.driver.get("/v1/device").text, "data")
        self.assertIn("Unable to renew the refresh token", logs.output[0])
        self.assertEqual(self.server.renewals, 1)
        self.assertEqual(self.server.logins, 2)

    def testOtherAnswersToTheAccesstokenRequestCauseNoLogin(self):
        self.driver.get("/v1/device")
        answers = [(400, "x"), (403, "ACCESS_DENIED"), (500, "x"), requests.Timeout()]
        for answer in answers:
            with self.subTest(answer=answer):
                self.ageAccesstoken()
                self.server.accesstokenAnswer = answer
                self.assertRejected("Unable to collect an access token")
                self.assertEqual(self.server.logins, 1)
                self.assertEqual(self.storedRefreshtoken(), "refresh-1")

    def testDriverWithoutCredentialsDoesNotLogIn(self):
        self.driver = self.makeDriver(username=None, password=None)
        self.driver.refreshDtAndToken = (datetime.datetime.now(), "from-a-file")
        self.assertRejected()
        self.assertEqual(self.server.logins, 0)

    def testRefusalByTheApiIsReturnedWithoutLogin(self):
        self.driver.get("/v1/device")
        for statusCode in (401, 403):
            with self.subTest(statusCode=statusCode):
                self.server.apiAnswer = (statusCode, "ACCESS_DENIED")
                self.assertEqual(self.driver.get("/v1/device").status_code, statusCode)
                self.assertEqual(self.server.logins, 1)
                self.assertEqual(self.server.accesstokenRequests, 1)

    def testThreadsSharingADriverLogInOnce(self):
        self.loggedInDriverWithRevokedRefreshtoken()
        tokens = []

        def collect():
            tokens.append(self.driver.accesstoken())

        threads = [threading.Thread(target=collect) for _ in range(8)]
        with self.assertLogs(level="WARNING"):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
        self.assertEqual(tokens, ["Bearer access-3"] * 8)
        self.assertEqual(self.server.logins, 2)


if __name__ == "__main__":
    unittest.main()
