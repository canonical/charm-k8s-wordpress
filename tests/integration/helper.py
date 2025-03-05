# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helper classes and functions for integration tests."""

import asyncio
import html
import inspect
import json
import logging
import mimetypes
import re
import secrets
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypedDict,
    Union,
)

import requests
import yaml
from juju.application import Application
from juju.model import Model
from juju.unit import Unit
from kubernetes import kubernetes
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


def retry(times: int, exceptions: Tuple[Type[Exception]], interval=5):
    """Retry decorator to catch exceptions and retry.

    Args:
        times: Number of times to retry.
        exceptions: Types of exceptions to catch to retry.
        interval: Interval between retries.
    """

    def decorator(func: Callable):
        """The decorating wrapper function.

        Args:
            func: Function to retry.
        """

        async def newfn(*args: Any, **kwargs: Any):
            """Newly wrapped function with retry.

            Returns:
                The newly decorated function with retry capability.
            """
            attempt = 0
            while attempt < times:
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    return func(*args, **kwargs)
                except exceptions as exc:
                    logger.warning(
                        "Function failed with exception %s, retrying %s/%s times.",
                        exc,
                        attempt,
                        times,
                    )
                    attempt += 1
                time.sleep(interval)
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            return func(*args, **kwargs)

        return newfn

    return decorator


class WordPressPost(TypedDict):
    """Typing for a WordPress post object.

    Attrs:
        id: A numeric identifier of a given post.
        link: A url path to given post.
    """

    id: int
    link: str


class WordpressClient:
    """A very simple WordPress client for test purpose only."""

    @classmethod
    def run_wordpress_functionality_test(cls, host: str, admin_username: str, admin_password: str):
        """Run standard WordPress functionality test suite.

        Args:
            host: ip address or hostname of the WordPress instance.
            admin_username: WordPress admin user username.
            admin_password: WordPress admin user password.
        """
        wp_client = cls(host=host, username=admin_username, password=admin_password, is_admin=True)
        post_title = secrets.token_hex(16)
        post_content = secrets.token_hex(16)
        post = wp_client.create_post(
            title=post_title,
            content=post_content,
        )
        homepage = wp_client.get_homepage()
        assert post_title in homepage, "admin user should be able to create a new post"
        comment = secrets.token_hex(16)
        post_link = post["link"]
        comment_link = wp_client.create_comment(
            post_id=post["id"],
            post_link=post_link,
            content=comment,
        )
        assert comment_link.startswith(post_link) and comment in wp_client.get_post(
            post_link
        ), "admin user should be able to create a comment"

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        is_admin: bool,
        use_launchpad_login: bool = False,
    ):
        """Initialize the WordPress JSON API client.

        Args:
            host: ip address or hostname of the WordPress instance.
            username: WordPress user username.
            password: WordPress user password.
            is_admin: if this user is a WordPress admin.
            use_launchpad_login: Use Launchpad OpenID to login instead of WordPress userpass.

        Raises:
            RuntimeError: if invalid credentials were used to login to WordPress.
            ValueError: if non-admin account was used to access /wp-json/ endpoint or the /wp-json/
                endpoint was not set through permalink.
        """
        self.host = host
        self.username = username
        self.password = password
        self._session = requests.session()
        self.timeout = 10
        if use_launchpad_login:
            self.login_using_launchpad(username, password)
        else:
            if not self._login():
                raise RuntimeError(f"login failed with username {username}")
        # By default, WordPress does not expose the /wp-json/ endpoint test if /wp-json
        # is exposed, and expose that with a permalink setting if not
        try:
            self._get(f"http://{self.host}/wp-json/").json()
        except requests.exceptions.JSONDecodeError as exc:
            if not is_admin:
                raise ValueError(
                    "set options-permalink manually or login with an admin account"
                ) from exc
            self._set_options_permalink()

    def _get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        except_status_code: Optional[int] = None,
    ) -> requests.Response:
        """HTTP GET using the instance session.

        The instance session contains user's login session cookies, so this method can assess
        restricted resources on WordPress.

        Args:
            url: Same as the ``url`` argument in :meth:`requests.Session.get`.
            headers: Same as ``url``  in :meth:`requests.Session.get`.
            except_status_code: Except the response http status code,
                raise :exc:`requests.HTTPError` if not match.

        Raises:
            HTTPError: if unexpected status code was returned.

        Returns:
            An instance of :class:`requests.Response`.
        """
        request = requests.Request("GET", url, headers=headers)
        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, timeout=self.timeout)
        if except_status_code is not None and response.status_code != except_status_code:
            raise requests.HTTPError(request=request, response=response)
        return response

    def _post(
        self,
        url: str,
        json_: Optional[dict] = None,
        data: Optional[Union[bytes, Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None,
        except_status_code: Optional[int] = None,
    ) -> requests.Response:
        """HTTP GET using the instance session.

        The instance session contains user's login session cookies, so this method can assess
        restricted resources on WordPress.

        Args:
            url: Same as the ``url`` argument in :meth:`requests.Session.post`.
            json_: Same as the ``json`` argument in :meth:`requests.Session.post`.
            data: Same as the ``data`` argument in :meth:`requests.Session.post`.
            headers: Same as the ``url``  in :meth:`requests.Session.post`.
            except_status_code: Except the response http status code,
                raise :exc:`requests.HTTPError` if not match.

        Raises:
            HTTPError: if unexpected status code was returned.

        Returns:
            An instance of :class:`requests.Response`.
        """
        request = requests.Request("POST", url, json=json_, data=data, headers=headers)
        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, timeout=self.timeout)
        if except_status_code is not None and response.status_code != except_status_code:
            raise requests.HTTPError(request=request, response=response)
        return response

    def _login(self) -> bool:
        """Login WordPress with current username and password, set session cookies.

        Returns:
            True if login successfully.
        """
        self._get(f"http://{self.host}/wp-login.php")
        response = self._post(
            f"http://{self.host}/wp-login.php",
            data={
                "log": self.username,
                "pwd": self.password,
                "wp-submit": "Log In",
                "redirect_to": f"http://{self.host}/wp-admin/",
                "testcookie": 1,
            },
            except_status_code=200,
        )
        return response.url == f"http://{self.host}/wp-admin/"

    def _set_options_permalink(self) -> None:
        """Set WordPress permalink option to /%postname%/."""
        options_permalink_page = self._get(
            f"http://{self.host}/wp-admin/options-permalink.php"
        ).text
        wp_nonce = re.findall('"nonce":"([a-zA-Z0-9]+)"', options_permalink_page)[0]
        self._post(
            f"http://{self.host}/wp-admin/options-permalink.php",
            data={
                "_wpnonce": wp_nonce,
                "_wp_http_referer": "/wp-admin/options-permalink.php",
                "selection": "/%postname%/",
                "permalink_structure": "/%postname%/",
                "submit": "Save Changes",
            },
            except_status_code=200,
        )

    def _gen_wp_rest_nonce(self) -> str:
        """Generate a nonce for WordPress REST API.

        Returns:
            (str) A WordPress nonce for WordPress JSON REST API.
        """
        new_post_page = self._get(f"http://{self.host}/wp-admin/post-new.php").text
        nonce: str = json.loads(re.findall("var wpApiSettings = ([^;]+);", new_post_page)[0])[
            "nonce"
        ]
        return nonce

    def create_post(
        self, title: str, content: str, featured_media: Optional[int] = None
    ) -> WordPressPost:
        """Create a WordPress post using wp-json API, return post object.

        Args:
            title (str): Title of the post.
            content (str): Content of the post.
            featured_media (Optional[int]): Media ID for the featured media of the post.

        Returns:
            Post object returned from WordPress REST API.
        """
        body: Dict[str, Union[str, int]] = {
            "status": "publish",
            "title": title,
            "content": content,
        }
        if featured_media is not None:
            body["featured_media"] = featured_media
        response = self._post(
            f"http://{self.host}/wp-json/wp/v2/posts/",
            json_=body,
            headers={"X-WP-Nonce": self._gen_wp_rest_nonce()},
            except_status_code=201,
        )
        return response.json()

    def create_comment(self, post_id: int, post_link: str, content: str) -> str:
        """Add a comment to a WordPress post using HTML form, return url link of the new comment.

        Args:
            post_id: ID of the post that the new comment will be attached to.
            post_link: URL of the post that the new comment will be attached to.
            content: Content of the new comment.

        Raises:
            ValueError: if same comment already exists.

        Returns:
            (str) URL pointed to the comment created.
        """
        post_page = self._get(post_link)
        nonce = re.findall(
            'name="_wp_unfiltered_html_comment_disabled" value="([a-zA-Z0-9]+)"', post_page.text
        )[0]

        response = self._post(
            f"http://{self.host}/wp-comments-post.php",
            data={
                "comment": content,
                "submit": "Post Comment",
                "comment_post_ID": post_id,
                "comment_parent": "0",
                "_wp_unfiltered_html_comment": nonce,
            },
            except_status_code=200,
        )
        if "Duplicate comment detected" in response.text:
            raise ValueError(f"Duplicate comment detected: {repr(content)}")
        return response.url

    def get_homepage(self) -> str:
        """Get the WordPress homepage source (HTML).

        Returns:
            (str) The WordPress homepage content, HTML.
        """
        return self._get(f"http://{self.host}").text

    def get_post(self, post_link: str) -> str:
        """Get the WordPress blog post page source (HTML).

        Args:
            post_link: URL to the WordPress post.

        Returns:
            (str) The WordPress homepage content, HTML.
        """
        return self._get(post_link).text

    def list_themes(self) -> List[str]:
        """List all installed WordPress theme slugs.

        Return:
            (List[str]) WordPress themes Installed.
        """
        response = self._get(
            f"http://{self.host}/wp-json/wp/v2/themes?per_page=100",
            headers={"X-WP-Nonce": self._gen_wp_rest_nonce()},
            except_status_code=200,
        )
        return [t["stylesheet"] for t in response.json()]

    def list_plugins(self) -> List[str]:
        """List all installed WordPress plugin slugs.

        Return:
            (List[str]) WordPress plugins Installed.
        """
        response = self._get(
            f"http://{self.host}/wp-json/wp/v2/plugins?per_page=100",
            headers={"X-WP-Nonce": self._gen_wp_rest_nonce()},
            except_status_code=200,
        )
        return [p["plugin"].split("/")[0] for p in response.json()]

    def list_comments(self, status: str = "approve", post_id: Optional[int] = None) -> List[dict]:
        """List all comments in the WordPress site.

        Args:
            status: WordPress comment status, can be 'hold', 'approve', 'spam', or 'trash'.
            post_id: List all comments attached to the post, None to query the entire site.

        Returns:
            (List[dict]) A list of comment objects returned by WordPress REST API.
        """
        url = f"http://{self.host}/wp-json/wp/v2/comments?status={status}"
        if post_id:
            url += f"&post={post_id}"
        response = self._get(url, headers={"X-WP-Nonce": self._gen_wp_rest_nonce()})
        return response.json()

    def upload_media(self, filename: str, content: bytes, mimetype: Optional[str] = None) -> dict:
        """Upload a media file (image/video).

        Args:
            filename: Filename of the media file.
            content: Content of the media file, bytes.
            mimetype: Mimetype of the media file, will infer from the filename if not provided.

        Raises:
            ValueError: if filename has invalid mimetype that cannot be automatically deduced.

        Returns:
             A dict with two keys: id and urls. Id is the WordPress media id and urls is a list of
             URL of the original image and resized images for the uploaded file on WordPress.
        """
        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0]
        if mimetype is None:
            raise ValueError("Unable to deduce mimetype from filename")
        response = self._post(
            f"http://{self.host}/wp-json/wp/v2/media",
            headers={
                "X-WP-Nonce": self._gen_wp_rest_nonce(),
                "Content-Type": mimetype,
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
            data=content,
            except_status_code=201,
        )
        media = response.json()
        image_urls = []
        for size in media["media_details"]["sizes"].values():
            image_urls.append(size["source_url"])
        if media["source_url"] not in image_urls:
            image_urls.append(media["source_url"])
        return {"id": media["id"], "urls": image_urls}

    def login_using_launchpad(self, username: str, password: str) -> None:
        """Log in the WordPress site using Launchpad OpenID, require launchpad related plugins.

        Args:
            username: Username of the launchpad account.
            password: Password of the launchpad account.
        """
        login_url = f"http://{self.host}/wp-login.php"
        self._get(login_url)
        openid_redirect = self._post(
            login_url,
            data={
                "launchpad": "Login",
                "redirect_to": f"http://{self.host}/wp-admin/",
                "testcookie": "1",
            },
        )
        openid_args = dict(
            re.findall(
                '<input type="hidden" name="([^"]+)" value="([^"]+)" />',
                html.unescape(openid_redirect.text),
            )
        )
        # openid_args["openid.ns"] = "http://specs.openid.net/auth/2.0"
        # openid_args["openid.ns.sreg"] = "http://openid.net/extensions/sreg/1.1"
        # openid_args["openid.sreg.required"] = "email,fullname"
        # openid_args["openid.realm"] = "https://"

        login_page = self._post(
            "https://login.launchpad.net/+openid",
            data=openid_args,
        )
        csrf_token = re.findall(
            "<input type='hidden' name='csrfmiddlewaretoken' value='([^']+)' />", login_page.text
        )[0]
        login_link = re.findall(
            '<a id="login-link" data-qa-id="login_link" href="([^"]+)" class="p-link--soft">',
            login_page.text,
        )[0]
        login_url = f"https://login.launchpad.net{login_link}"
        confirm_page = self._post(
            login_url,
            data={
                "csrfmiddlewaretoken": csrf_token,
                "email": username,
                "user-intentions": "login",
                "password": password,
                "continue": "",
                "openid.usernamesecret": "",
            },
            headers={"Referer": login_page.url},
        )
        csrf_token = re.findall(
            "<input type='hidden' name='csrfmiddlewaretoken' value='([^']+)' />", confirm_page.text
        )[0]
        team = re.findall("Team membership: ([^<]+)<", confirm_page.text)[0]
        self._post(
            confirm_page.url,
            data={
                "csrfmiddlewaretoken": csrf_token,
                "nickname": "on",
                "email": "on",
                "fullname": "on",
                team: "on",
                "ok": "",
                "yes": "",
                "openid.usernamesecret": "",
            },
            headers={"Referer": confirm_page.url},
            except_status_code=200,
        )

    def list_associated_ubuntu_one_accounts(self) -> List[str]:
        """List Ubuntu One accounts OpenID IDs associated with the current WordPress account.

        Returns:
            A list of Ubuntu One account OpenID IDs
            (something like https://login.ubuntu.com/+id/xxxxxxxx).
        """
        openid_setting = self._get(
            f"http://{self.host}/wp-admin/users.php?page=your_openids",
            except_status_code=200,
        )
        return re.findall("<td>(https://login\\.ubuntu\\.com[^<]+)</td>", openid_setting.text)

    def list_roles(self) -> List[str]:
        """List all WordPress roles of the current user.

        Raises:
            ValueError: No valid user to call list_roles was found.

        Returns:
            WordPress roles as a list of str.
        """
        user_page = self._get(f"http://{self.host}/wp-admin/users.php").text
        emails = re.findall("""data-colname="Email"><a href='mailto:([^']+)'>""", user_page)
        usernames = re.findall('users\\.php">([^<]+)</a>', user_page)
        roles = re.findall('data-colname="Role">([^<]+)</td>', user_page)
        for email, username, role in zip(emails, usernames, roles):
            if self.username in (email, username):
                return [r.strip() for r in role.lower().split(",")]
        raise ValueError(f"User {self.username} not found")


class WordpressApp:
    """An object represents the wordpress charm application."""

    def __init__(self, app: Application, ops_test: OpsTest, kube_config: str):
        """Initialize the WordpressApp object."""
        self.app = app
        self.ops_test = ops_test
        kubernetes.config.load_kube_config(config_file=kube_config)
        self.kube_core_client = kubernetes.client.CoreV1Api()

    @property
    def model(self) -> Model:
        """Get the current juju model."""
        model = self.ops_test.model
        assert model
        return model

    @property
    def name(self) -> str:
        """Get the wordpress charm application name."""
        return self.app.name

    @retry(times=5, exceptions=(KeyError,))
    async def get_unit_ips(self) -> List[str]:
        """Retrieve unit ip addresses, similar to fixture_get_unit_status_list.

        Returns:
            list of WordPress units ip addresses.
        """
        _, status, _ = await self.ops_test.juju("status", "--format", "json")
        status = json.loads(status)
        units = status["applications"][self.name]["units"]
        ip_list = []
        for key in sorted(units.keys(), key=lambda n: int(n.split("/")[-1])):
            ip_list.append(units[key]["address"])
        return ip_list

    async def get_default_admin_password(self) -> str:
        """Get default admin password using get-initial-password action.

        Returns:
            WordPress admin account password
        """
        action = await self.app.units[0].run_action("get-initial-password")
        await action.wait()
        return action.results["password"]

    async def set_config(self, config):
        """Update the configuration of the wordpress charm."""
        await self.app.set_config(config)

    async def get_swift_bucket(self) -> Optional[str]:
        """Get the swift bucket name used by the wordpress application."""
        config = await self.app.get_config()
        openstack_config = config["wp_plugin_openstack-objectstorage_config"]["value"]
        return yaml.safe_load(openstack_config).get("bucket")

    async def client_for_units(self) -> List[WordpressClient]:
        """Get a list of WordpressClient for each unit of the wordpress application."""
        clients = []
        default_admin_password = await self.get_default_admin_password()
        for unit_ip in await self.get_unit_ips():
            clients.append(
                WordpressClient(
                    host=unit_ip, username="admin", password=default_admin_password, is_admin=True
                )
            )
        return clients

    async def wait_for_wordpress_idle(self, status: Optional[str] = None):
        """Wait for the wordpress application is idle."""
        await self.model.wait_for_idle(status=status, apps=[self.name])

    def get_units(self) -> List[Unit]:
        """Get units of the wordpress application."""
        return self.app.units

    async def get_wordpress_config(self) -> str:
        """Get wp-config.php contents from the leader unit.

        Returns:
            The contents of wp-config.php
        """
        unit = self.app.units[0]
        stdout = kubernetes.stream.stream(
            self.kube_core_client.connect_get_namespaced_pod_exec,
            unit.name.replace("/", "-"),
            unit.model.name,
            container="wordpress",
            command=["cat", "/var/www/html/wp-config.php"],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        return stdout


async def wait_for(
    func: Callable[[], Union[Awaitable, Any]],
    timeout: int = 300,
    check_interval: int = 10,
) -> Any:
    """Wait for function execution to become truthy.

    Args:
        func: A callback function to wait to return a truthy value.
        timeout: Time in seconds to wait for function result to become truthy.
        check_interval: Time in seconds to wait between ready checks.

    Raises:
        TimeoutError: if the callback function did not return a truthy value within timeout.
    """
    deadline = time.time() + timeout
    is_awaitable = inspect.iscoroutinefunction(func)
    while time.time() < deadline:
        if is_awaitable and (result := await func()):
            return result
        if result := func():
            return result
        time.sleep(check_interval)
    raise TimeoutError()


async def get_mysql_primary_unit(units: Iterable[Unit]) -> Optional[Unit]:
    """Get the mysql primary unit.

    Args:
        units: An iterable list of units to search for primary unit from.
    """
    for unit in units:
        if unit.workload_status_message == "Primary":
            return unit
    return None
