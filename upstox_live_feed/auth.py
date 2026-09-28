"""Secure Upstox OAuth login adapted from the existing upstox project."""

from __future__ import annotations

import json
import logging
import os
import secrets
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlsplit
from zoneinfo import ZoneInfo

import httpx
import pyotp
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config import LOGIN_FAILURE_DIR, RUNTIME_DIR, TOKEN_CACHE_FILE, LoginSettings

LOGGER = logging.getLogger(__name__)
LOGIN_DIALOG_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"
INDIA_TIMEZONE = ZoneInfo("Asia/Kolkata")
WINDOW_SIZE = (1280, 1024)


class UpstoxAuthenticationError(RuntimeError):
    """Raised when the browser or token-exchange flow cannot authenticate."""


class UpstoxTokenProvider:
    """Obtain and securely cache an Upstox access token."""

    def __init__(self, settings: LoginSettings) -> None:
        self.settings = settings
        self._prepare_private_runtime()

    @staticmethod
    def _prepare_private_runtime() -> None:
        RUNTIME_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        RUNTIME_DIR.chmod(0o700)
        LOGIN_FAILURE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        LOGIN_FAILURE_DIR.chmod(0o700)

    @staticmethod
    def _read_cached_token() -> Optional[str]:
        if not TOKEN_CACHE_FILE.is_file():
            return None
        if os.stat(TOKEN_CACHE_FILE).st_mode & 0o077:
            LOGGER.warning("Ignoring token cache with unsafe file permissions")
            return None

        try:
            payload = json.loads(TOKEN_CACHE_FILE.read_text(encoding="utf-8"))
            expires_at = datetime.fromisoformat(payload["expires_at"])
            token = payload["access_token"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            LOGGER.warning("Ignoring invalid token cache")
            return None

        now = datetime.now(INDIA_TIMEZONE)
        if expires_at.tzinfo is None or expires_at <= now + timedelta(minutes=5):
            return None
        if not isinstance(token, str) or not token:
            return None

        LOGGER.info("Using the protected cached Upstox token")
        return token

    @staticmethod
    def _write_cached_token(token: str) -> None:
        now = datetime.now(INDIA_TIMEZONE)
        expires_at = datetime.combine(
            now.date() + timedelta(days=1),
            time(hour=3, minute=25),
            tzinfo=INDIA_TIMEZONE,
        )
        payload = json.dumps(
            {"access_token": token, "expires_at": expires_at.isoformat()},
            separators=(",", ":"),
        )

        temporary_file = TOKEN_CACHE_FILE.with_suffix(".tmp")
        file_descriptor = os.open(
            temporary_file,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_file, TOKEN_CACHE_FILE)
            TOKEN_CACHE_FILE.chmod(0o600)
        finally:
            if temporary_file.exists():
                temporary_file.unlink()

    @staticmethod
    def invalidate_cache() -> None:
        if TOKEN_CACHE_FILE.exists():
            TOKEN_CACHE_FILE.unlink()
            LOGGER.info("Local Upstox token cache invalidated")

    @staticmethod
    def _type(driver: webdriver.Chrome, element_id: str, value: str) -> None:
        element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, element_id))
        )
        element.send_keys(value)

    @staticmethod
    def _click(driver: webdriver.Chrome, element_id: str) -> None:
        element = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, element_id))
        )
        try:
            element.click()
        except ElementClickInterceptedException:
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", element
            )
            driver.execute_script("arguments[0].click();", element)

    @staticmethod
    def _save_failure_screenshot(driver: webdriver.Chrome) -> None:
        try:
            path = LOGIN_FAILURE_DIR / f"login_failure_{datetime.now():%Y%m%d_%H%M%S}.png"
            driver.get_screenshot_as_file(str(path))
            path.chmod(0o600)
            LOGGER.error("Login failed; protected screenshot saved under .runtime")
        except Exception:
            LOGGER.warning("Login failed and a diagnostic screenshot could not be saved")

    def _authorization_code(self) -> str:
        state = secrets.token_urlsafe(32)
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.settings.api_key,
                "redirect_uri": self.settings.redirect_uri,
                "state": state,
            }
        )
        login_url = f"{LOGIN_DIALOG_URL}?{query}"

        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"--window-size={WINDOW_SIZE[0]},{WINDOW_SIZE[1]}")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)
        try:
            driver.get(login_url)
            self._type(driver, "mobileNum", self.settings.mobile_number)
            self._click(driver, "getOtp")
            self._type(driver, "otpNum", pyotp.TOTP(self.settings.totp_secret).now())
            self._click(driver, "continueBtn")
            self._type(driver, "pinCode", self.settings.pin)
            self._click(driver, "pinContinueBtn")

            WebDriverWait(driver, 30).until(EC.url_contains("code="))
            parameters = parse_qs(urlsplit(driver.current_url).query)
            authorization_code = parameters.get("code", [""])[0]
            returned_state = parameters.get("state", [""])[0]
            if not authorization_code:
                raise UpstoxAuthenticationError("Upstox redirect omitted authorization code")
            if returned_state != state:
                raise UpstoxAuthenticationError("Upstox OAuth state validation failed")
            return authorization_code
        except Exception as error:
            self._save_failure_screenshot(driver)
            if isinstance(error, UpstoxAuthenticationError):
                raise
            raise UpstoxAuthenticationError("Upstox browser login failed") from error
        finally:
            driver.quit()

    def _exchange_code(self, authorization_code: str) -> str:
        try:
            response = httpx.post(
                TOKEN_URL,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "code": authorization_code,
                    "client_id": self.settings.api_key,
                    "client_secret": self.settings.api_secret,
                    "redirect_uri": self.settings.redirect_uri,
                    "grant_type": "authorization_code",
                },
                timeout=self.settings.request_timeout_seconds,
            )
            response.raise_for_status()
            access_token = response.json()["access_token"]
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            raise UpstoxAuthenticationError("Upstox token exchange failed") from error

        if not isinstance(access_token, str) or not access_token:
            raise UpstoxAuthenticationError("Upstox returned an invalid access token")
        return access_token

    def get_access_token(self) -> str:
        cached_token = self._read_cached_token()
        if cached_token:
            return cached_token

        LOGGER.info("Starting protected Upstox browser login")
        authorization_code = self._authorization_code()
        access_token = self._exchange_code(authorization_code)
        self._write_cached_token(access_token)
        LOGGER.info("Upstox access token obtained and cached locally")
        return access_token
