#!/usr/bin/env python3
import collections
import itertools
import json
import logging
import os
import secrets
import string
import textwrap
import time
import traceback

import ops.charm
import ops.pebble
import yaml

from yaml import safe_load
import mysql.connector

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus

from charms.nginx_ingress_integrator.v0.ingress import IngressRequires
import exceptions
from opslib.mysql import MySQLClient

# MySQL logger prints database credentials on debug level, silence it
logging.getLogger(mysql.connector.__name__).setLevel(logging.WARNING)
logger = logging.getLogger()


class WordpressCharm(CharmBase):
    class _ReplicaRelationNotReady(Exception):
        pass

    _ExecResult = collections.namedtuple("ExecResult", "success result message")

    _WP_CONFIG_PATH = "/var/www/html/wp-config.php"
    _CONTAINER_NAME = "wordpress"
    _SERVICE_NAME = "wordpress"
    _WORDPRESS_USER = "www-data"
    _WORDPRESS_GROUP = "www-data"
    _WORDPRESS_DB_CHARSET = "utf8mb4"

    # Default themes and plugins are installed in oci image build time and defined in Dockerfile
    _WORDPRESS_DEFAULT_THEMES = [
        'fruitful',
        'launchpad',
        'light-wordpress-theme',
        'mscom',
        'thematic',
        'twentyeleven',
        'twentynineteen',
        'twentytwenty',
        'twentytwentyone',
        'ubuntu-cloud-website',
        'ubuntu-community-wordpress-theme/ubuntu-community',
        'ubuntu-community/ubuntu-community',
        'ubuntu-fi',
        'ubuntu-light',
        'ubuntustudio-wp/ubuntustudio-wp',
        'xubuntu-website/xubuntu-eighteen',
        'xubuntu-website/xubuntu-fifteen',
        'xubuntu-website/xubuntu-fourteen',
        'xubuntu-website/xubuntu-thirteen',
    ]

    _WORDPRESS_DEFAULT_PLUGINS = [
        '404page',
        'akismet',
        'all-in-one-event-calendar',
        'powerpress',
        'coschedule-by-todaymade',
        'elementor',
        'essential-addons-for-elementor-lite',
        'favicon-by-realfavicongenerator',
        'feedwordpress',
        'fruitful-shortcodes',
        'genesis-columns-advanced',
        'hello',
        'line-break-shortcode',
        'wp-mastodon-share',
        'no-category-base-wpml',
        'openid',
        'wordpress-launchpad-integration',
        'wordpress-teams-integration',
        'openstack-objectstorage-k8s',
        'post-grid',
        'redirection',
        'relative-image-urls',
        'rel-publisher',
        'safe-svg',
        'show-current-template',
        'simple-301-redirects',
        'simple-custom-css',
        'so-widgets-bundle',
        'social-media-buttons-toolbar',
        'svg-support',
        'syntaxhighlighter',
        'wordpress-importer',
        'wp-markdown',
        'wp-polls',
        'wp-font-awesome',
        'wp-lightbox-2',
        'wp-statistics',
        'xubuntu-team-members',
        'wordpress-seo',
    ]

    _DB_CHECK_INTERVAL = 1

    _container_name = "wordpress"
    _default_service_port = 80

    state = StoredState()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.db = MySQLClient(self, "db")

        c = self.model.config
        self.state.set_default(
            blog_hostname=c["blog_hostname"] or self.app.name,
            installed_successfully=False,
            install_state=set(),
            has_db_relation=False,
            has_ingress_relation=False,
            db_host=c["db_host"] or None,
            db_name=c["db_name"] or None,
            db_user=c["db_user"] or None,
            db_password=None,
            relation_db_host=None,
            relation_db_name=None,
            relation_db_user=None,
            relation_db_password=None,
        )

        self.ingress = IngressRequires(self, self.ingress_config)

        self.framework.observe(
            self.on.get_initial_password_action, self._on_get_initial_password_action
        )

        self.framework.observe(self.on.leader_elected, self._on_leader_elected_replica_data_handler)
        self.framework.observe(self.db.on.database_changed, self._on_relation_database_changed)

        self.framework.observe(self.on.config_changed, self._reconciliation)
        self.framework.observe(self.on.wordpress_pebble_ready, self._reconciliation)
        self.framework.observe(self.on["wordpress-replica"].relation_changed, self._reconciliation)
        self.framework.observe(self.db.on.database_changed, self._reconciliation)

    @property
    def ingress_config(self):
        blog_hostname = self.state.blog_hostname
        ingress_config = {
            "service-hostname": blog_hostname,
            "service-name": self.app.name,
            "service-port": "80",
        }
        tls_secret_name = self.model.config["tls_secret_name"]
        if tls_secret_name:
            ingress_config["tls-secret-name"] = tls_secret_name
        return ingress_config

    def _on_get_initial_password_action(self, event):
        """Handle the get-initial-password action."""
        if self._replica_consensus_reached():
            default_admin_password = self._replica_relation_data().get("default_admin_password")
            event.set_results({"password": default_admin_password})
        else:
            logger.error("Action get-initial-password failed. Replica consensus not exists")
            event.fail("Default admin password has not been generated yet.")

    @staticmethod
    def _wordpress_secret_key_fields():
        return [
            'auth_key',
            'secure_auth_key',
            'logged_in_key',
            'nonce_key',
            'auth_salt',
            'secure_auth_salt',
            'logged_in_salt',
            'nonce_salt',
        ]

    def _generate_wp_secret_keys(self):
        def _wp_generate_password(length=64):
            characters = string.ascii_letters + "!@#$%^&*()" + "-_ []{}<>~`+=,.;:/?|"
            return "".join(secrets.choice(characters) for _ in range(length))

        wp_secrets = {
            field: _wp_generate_password() for field in self._wordpress_secret_key_fields()
        }
        wp_secrets["default_admin_password"] = secrets.token_urlsafe(32)
        return wp_secrets

    def _replica_relation_data(self):
        relation = self.model.get_relation("wordpress-replica")
        if relation is None:
            raise self._ReplicaRelationNotReady(
                "Access replica peer relation data before relation established"
            )
        else:
            return relation.data[self.app]

    def _replica_consensus_reached(self):
        """Test if the synchronized data required for WordPress replication are initialized.

        Returns:
            True if the initialization of synchronized data has finished, else False.
        """
        fields = self._wordpress_secret_key_fields()
        try:
            replica_data = self._replica_relation_data()
        except self._ReplicaRelationNotReady:
            return False
        return all(replica_data.get(f) for f in fields)

    def _on_leader_elected_replica_data_handler(self, event):
        """Initialize the synchronized data required for WordPress replication

        Only the leader can update the data shared with all replicas. Leader should check if
        the data exist when leadership is established, generate required data and set it in
        the peer relation if not.

        Args:
            event: required by ops framework, not used.

        Returns:
            None.
        """
        if not self._replica_consensus_reached() and self.unit.is_leader():
            replica_relation_data = self._replica_relation_data()
            new_replica_data = self._generate_wp_secret_keys()
            for secret_key, secret_value in new_replica_data.items():
                replica_relation_data[secret_key] = secret_value

    def _on_relation_database_changed(self, event):
        """Callback function to handle db relation changes (data changes/relation breaks)

        This method will set all db relation related states ``relation_db_*`` when db relation
        changes and will reset all that to ``None`` after db relation is broken.

        Args:
            event: An instance of opslib.mysql.MySQLDatabaseChangedEvent represents the new
                database connection information.

        Returns:
            None.
        """
        self.state.relation_db_host = event.host
        self.state.relation_db_name = event.database
        self.state.relation_db_user = event.user
        self.state.relation_db_password = event.password

    def _gen_wp_config(self):
        """Generate the wp-config.php file WordPress needs based on charm config and relations

        This method will not check the validity of the configuration or current state,
        unless they are security related, in that case, an exception will be raised.

        Returns:
            The content of wp-config.php file in string.
        """
        wp_config = [
            textwrap.dedent(
                """\
            <?php
            # This file is managed by Juju. Do not make local changes.
            if (strpos($_SERVER['HTTP_X_FORWARDED_PROTO'], 'https') !== false) {
                $_SERVER['HTTPS']='on';
            }
            $table_prefix = 'wp_';
            $_w_p_http_protocol = 'http://';
            if (!empty($_SERVER['HTTPS']) && 'off' != $_SERVER['HTTPS']) {
                $_w_p_http_protocol = 'https://';
            }
            define( 'WP_PLUGIN_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] . '/wp-content/plugins' );
            define( 'WP_CONTENT_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] . '/wp-content' );
            define( 'WP_SITEURL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );
            define( 'WP_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );
            define( 'WP_HOME', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );"""
            )
        ]

        # database info in config takes precedence over database info provided by relations
        database_info = self._current_effective_db_info()
        for db_key, db_value in database_info.items():
            wp_config.append(f"define( '{db_key.upper()}', '{db_value}' );")

        wp_config.append(f"define( 'DB_CHARSET',  '{self._WORDPRESS_DB_CHARSET}' );")

        replica_relation_data = self._replica_relation_data()
        for secret_key in self._wordpress_secret_key_fields():
            secret_value = replica_relation_data.get(secret_key)
            if not secret_value:
                raise ValueError(f"{secret_key} value is empty")
            wp_config.append(f"define( '{secret_key.upper()}', '{secret_value}' );")

        # make WordPress immutable, user can not install or update any plugins or themes from
        # admin panel and all updates are disabled
        wp_config.append("define( 'DISALLOW_FILE_MODS', true );")
        wp_config.append("define( 'AUTOMATIC_UPDATER_DISABLED', true );")

        wp_config.append("define( 'WP_CACHE', true );")
        wp_config.append(
            textwrap.dedent(
                """\
                if ( ! defined( 'ABSPATH' ) ) {
                    define( 'ABSPATH', __DIR__ . '/' );
                }

                /** Sets up WordPress vars and included files. */
                require_once ABSPATH . 'wp-settings.php';
                """
            )
        )
        return "\n".join(wp_config)

    def _container(self):
        """Get the WordPress workload container.

        Returns:
            The pebble instance of the WordPress container.
        """
        return self.unit.get_container(self._CONTAINER_NAME)

    def _wordpress_service_exists(self):
        """Check if the WordPress pebble layer exists.

        Returns:
            True if WordPress layer already exists, else False.
        """
        return self._SERVICE_NAME in self._container().get_plan().services

    def _stop_server(self):
        """Stop WordPress (apache) server, this operation is idempotence.

        Returns:
            None.
        """
        logger.debug("Ensure WordPress (apache) server is down")
        if (
            self._wordpress_service_exists()
            and self._container().get_service(self._SERVICE_NAME).is_running()
        ):
            self._container().stop(self._SERVICE_NAME)

    def _run_cli(
        self, cmd, user=None, group=None, working_dir=None, combine_stderr=False, timeout=60
    ):
        """Execute a command in WordPress container.

        Args:
            cmd (List[str]): The command to be executed.
            user (str): Username to run this command as, use root when not provided.
            group (str): Name of the group to run this command as, use root when not provided.
            working_dir (str):  Working dir to run this command in, use home dir if not provided.
            combine_stderr (bool): Redirect stderr to stdout, when enabled, stderr in the result
                will always be empty.
            timeout (int): Set a timeout for the running program in seconds. Default is 60 seconds.
                ``TimeoutError`` will be raised if timeout exceeded.

        Returns:
            A named tuple with three fields: return code, stdout and stderr. Stdout and stderr are
            both string.
        """

        Result = collections.namedtuple("CommandExecResult", "return_code stdout stderr")
        process = self._container().exec(
            cmd,
            user=user,
            group=group,
            working_dir=working_dir,
            combine_stderr=combine_stderr,
            timeout=timeout,
        )
        try:
            stdout, stderr = process.wait_output()
            result = Result(0, stdout, stderr)
        except ops.pebble.ExecError as e:
            result = Result(e.exit_code, e.stdout, e.stderr)
        return_code = result.return_code
        if combine_stderr:
            logger.debug(
                "Run command: %s return code %s\noutput: %s", cmd, return_code, result.stdout
            )
        else:
            logger.debug(
                "Run command: %s, return code %s\nstdout: %s\nstderr:%s",
                cmd,
                return_code,
                result.stdout,
                result.stderr,
            )
        return result

    def _run_wp_cli(self, cmd, timeout=60, combine_stderr=False):
        """Execute a wp-cli command, this is a wrapper around :meth:`charm.WordpressCharm._run_cli`

        See :meth:`charm.WordpressCharm._run_cli` for documentation of the arguments and return
        value.
        """
        result = self._run_cli(
            cmd,
            user=self._WORDPRESS_USER,
            group=self._WORDPRESS_GROUP,
            working_dir="/var/www/html",
            combine_stderr=combine_stderr,
            timeout=timeout,
        )
        return result

    def _wrapped_run_wp_cli(self, cmd, timeout=60, error_message=None):
        """Run wp cli command and return the result as ``self._ExecResult``

        Stdout and stderr are discarded, the result field of ExecResult is always none. The
        execution is considered success if return code is 0. The message field will be generated
        automatically based on command if ``error_message`` is not provided.

        Args:
            cmd (List[str]): The command to be executed.
            timeout (int): Set a timeout for the running program, in seconds. Default is 60 seconds.
                ``TimeoutError`` will be raised if timeout exceeded.
            error_message (str) message in the return result if the command failed, if None, a default
                error message will be provided in the result.

        Returns:
            A named tuple with three fields: success, result and message. ``success`` will be True
            if the command succeed. ``result`` will always be None and ``message`` represents the
            error message, in case of success, it will be empty.
        """
        result = self._run_wp_cli(cmd=cmd, timeout=timeout, combine_stderr=True)
        if result.return_code != 0:
            return self._ExecResult(
                success=False,
                result=None,
                message=f"command {cmd} failed" if not error_message else error_message,
            )
        else:
            return self._ExecResult(success=True, result=None, message="")

    def _wp_is_installed(self):
        """Check if WordPress is installed (check if WordPress related tables exist in database)

        Returns:
            True if WordPress is installed in the current connected database.
        """
        logger.debug("Check if WordPress is installed")
        return self._run_wp_cli(["wp", "core", "is-installed"]).return_code == 0

    def _current_effective_db_info(self):
        """Get the current effective db connection information

        Database info in config takes precedence over database info provided by relations.
        Return value is a dict containing four keys (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)

        Returns:
            A dict containing four keys "db_host", "db_name", "db_user" and "db_password". All
            values are string.
        """
        database_info = {
            key.upper(): self.model.config[key]
            for key in ["db_host", "db_name", "db_user", "db_password"]
        }
        if any(not value for value in database_info.values()):
            database_info = {
                key.upper(): getattr(self.state, f"relation_{key}")
                for key in ["db_host", "db_name", "db_user", "db_password"]
            }
        return database_info

    def _test_database_connectivity(self):
        """Test the connectivity of the current database config/relation

        Returns:
            A tuple of connectivity as bool and error message as str, error message will be
            an empty string if charm can connect to the database.
        """
        db_info = self._current_effective_db_info()
        try:
            # TODO: add database charset check later
            cnx = mysql.connector.connect(
                host=db_info["DB_HOST"],
                database=db_info["DB_NAME"],
                user=db_info["DB_USER"],
                password=db_info["DB_PASSWORD"],
                charset="latin1",
            )
            cnx.close()
            return True, ""
        except mysql.connector.Error as err:
            if err.errno < 0:
                logger.debug("MySQL connection test failed, traceback: %s", traceback.format_exc())
            return False, f"MySQL error {err.errno}"

    def _wp_install_cmd(self):
        """Generate wp-cli command used to install WordPress on database

        Returns:
            Wp-cli WordPress install command, a list of strings.
        """
        initial_settings = yaml.safe_load(self.model.config["initial_settings"])
        admin_user = initial_settings.get("user_name", "admin_username")
        admin_email = initial_settings.get("admin_email", "name@example.com")
        default_admin_password = self._replica_relation_data()["default_admin_password"]
        admin_password = initial_settings.get("admin_password", default_admin_password)
        return [
            "wp",
            "core",
            "install",
            "--url=localhost",
            f"--title=The {self.model.config['blog_hostname'] or self.app.name} Blog",
            f"--admin_user={admin_user}",
            f"--admin_email={admin_email}",
            f"--admin_password={admin_password}",
        ]

    def _wp_install(self):
        """Install WordPress (create WordPress required tables in DB)"""
        logger.debug("Install WordPress, create WordPress related table in the database")
        self.unit.status = ops.model.MaintenanceStatus("Initializing WordPress DB")
        process = self._run_wp_cli(self._wp_install_cmd(), combine_stderr=True, timeout=60)
        if process.return_code != 0:
            logger.error("WordPress installation failed: %s", process.stdout)
            raise exceptions.WordPressInstallError("check logs for more information")

    def _init_pebble_layer(self):
        """Ensure WordPress layer exists in pebble"""
        logger.debug("Ensure WordPress layer exists in pebble")
        layer = {
            "summary": "WordPress layer",
            "description": "WordPress server",
            "services": {
                self._SERVICE_NAME: {
                    "override": "replace",
                    "summary": "WordPress server (apache)",
                    "command": "apache2ctl -D FOREGROUND",
                }
            },
        }
        self._container().add_layer("wordpress", layer, combine=True)

    def _start_server(self):
        """Start WordPress (apache) server. On leader unit, also make sure WordPress is installed

        Check if the pebble layer has been added, then check the installation status of WordPress,
        finally start the server. The installation process only run on the leader unit. This
        operation is idempotence.
        """
        logger.debug("Ensure WordPress server is up")
        self._init_pebble_layer()
        if self.unit.is_leader():
            msg = ""
            for _ in range(30):
                success, msg = self._test_database_connectivity()
                if success:
                    break
                time.sleep(self._DB_CHECK_INTERVAL)
            else:
                raise exceptions.WordPressBlockedStatusException(msg)

            if not self._wp_is_installed():
                self._wp_install()
            if self._current_wp_config() is None:
                # For security reasons, never start WordPress server if wp-config.php not exists
                raise FileNotFoundError(
                    "required file (wp-config.php) for starting WordPress server not exists"
                )
        if not self._container().get_service(self._SERVICE_NAME).is_running():
            self._container().start(self._SERVICE_NAME)

    def _current_wp_config(self):
        """Retrieve the current version of wp-config.php from server, return None if not exists

        Returns:
            The content of the current wp-config.php file, str.
        """
        wp_config_path = self._WP_CONFIG_PATH
        container = self._container()
        if container.exists(wp_config_path):
            return self._container().pull(wp_config_path).read()
        return None

    def _remove_wp_config(self):
        """Remove wp-config.php file on server"""
        logger.debug("Remove wp-config.php in container")
        container = self._container()
        if container.get_service(self._SERVICE_NAME).is_running():
            # For security reasons, prevent removing wp-config.php while WordPress server running
            raise RuntimeError("trying to delete wp-config.php while WordPress server is running")
        self._container().remove_path(self._WP_CONFIG_PATH, recursive=True)

    def _push_wp_config(self, wp_config):
        """Update the content of wp-config.php on server

        Write the wp-config.php file in :attr:`charm.WordpressCharm._WP_CONFIG_PATH`.

        Args:
            wp_config (str): the content of wp-config.php file.
        """
        logger.debug("Update wp-config.php content in container")
        self._container().push(
            self._WP_CONFIG_PATH,
            wp_config,
            user=self._WORDPRESS_USER,
            group=self._WORDPRESS_GROUP,
            permissions=0o600,
        )

    def _core_reconciliation(self):
        """Reconciliation process for the WordPress core services, returns True if successful.

        It will fail under the following two circumstances:
          - Peer relation data not ready
          - Config doesn't provide valid database information and db relation hasn't
            been established

        It will check if the current wp-config.php file matches the desired config.
        If not, update the wp-config.php file.

        It will also check if WordPress is installed (WordPress-related tables exist in db).
        If not, install WordPress (create WordPress required tables in db).

        If any update is needed, it will stop the apache server first to prevent any requests
        during the update for security reasons.
        """
        logger.info("Start core reconciliation process")
        if not self._replica_consensus_reached():
            logger.info("Core reconciliation terminates early, replica consensus is not ready")
            self._stop_server()
            raise exceptions.WordPressWaitingStatusException("Waiting for unit consensus")
        available_db_config = tuple(
            key
            for key in ("db_host", "db_name", "db_user", "db_password")
            if self.model.config[key]
        )
        available_db_relation = tuple(
            key
            for key in ("db_host", "db_name", "db_user", "db_password")
            if getattr(self.state, f"relation_{key}")
        )
        if len(available_db_config) != 4 and len(available_db_relation) != 4:
            logger.info(
                "Core reconciliation terminated early due to db info missing, "
                "available from config: %s, available from relation: %s",
                available_db_config,
                available_db_relation,
            )
            self._stop_server()
            raise exceptions.WordPressBlockedStatusException("Waiting for db relation/config")
        wp_config = self._gen_wp_config()
        if wp_config != self._current_wp_config():
            logger.info("Changes detected in wp-config.php, updating")
            self._stop_server()
            self._push_wp_config(wp_config)
        if self._current_wp_config() is not None:
            self._start_server()

    def _check_addon_type(self, addon_type):
        """Check if addon_type is one of the accepted addon types (theme/plugin).

        Raise a ValueException if not.
        """
        if addon_type not in ("theme", "plugin"):
            raise ValueError(f"Addon type unknown {repr(addon_type)}, accept: (theme, plugin)")

    def _wp_addon_list(self, addon_type):
        """List all installed WordPress addons

        Args:
            addon_type (str): ``"theme"`` or ``"plugin"``

        Returns:
            A named tuple with three fields: success, result and message. If list command failed,
            success will be False, result will be None and message will be the error message.
            Other than that, success will be True, message will be empty and result will be a list
            of dicts represents the status of currently installed addons. Each dict contains four
            keys: name, status, update and version.
        """
        self._check_addon_type(addon_type)
        process = self._run_wp_cli(["wp", addon_type, "list", "--format=json"], timeout=600)
        if process.return_code != 0:
            return self._ExecResult(
                success=False, result=None, message=f"wp {addon_type} list command failed"
            )
        try:
            return self._ExecResult(success=True, result=json.loads(process.stdout), message="")
        except json.decoder.JSONDecodeError:
            return self._ExecResult(
                success=False,
                result=None,
                message=f"wp {addon_type} list command failed, stdout is not json",
            )

    def _wp_addon_install(self, addon_type, addon_name):
        """Install WordPress addon (plugin/theme)

        Args:
            addon_type (str): ``"theme"`` or ``"plugin"``.
            addon_name (str): name of the addon that needs to be installed.
        """
        self._check_addon_type(addon_type)
        if addon_type == "theme":
            # --force will overwrite any installed version of the theme,
            # without prompting for confirmation
            cmd = ["wp", "theme", "install", addon_name, "--force"]
        else:
            cmd = ["wp", "plugin", "install", addon_name]
        return self._wrapped_run_wp_cli(cmd, timeout=600)

    def _wp_addon_uninstall(self, addon_type, addon_name):
        """Uninstall WordPress addon (theme/plugin)

        Args:
            addon_type (str): ``"theme"`` or ``"plugin"``.
            addon_name (str): name of the addon that needs to be uninstalled.
        """
        self._check_addon_type(addon_type)
        if addon_type == "theme":
            cmd = ["wp", "theme", "delete", addon_name, "--force"]
        else:
            cmd = ["wp", "plugin", "uninstall", addon_name, "--deactivate"]
        return self._wrapped_run_wp_cli(cmd, timeout=600)

    def _addon_reconciliation(self, addon_type):
        """Reconciliation process for WordPress addons (theme/plugin)

        Install and uninstall themes/plugins to match the themes/plugins setting in config.

        Args:
            addon_type (str): ``"theme"`` or ``"plugin"``.
        """
        self._check_addon_type(addon_type)
        logger.debug(f"Start {addon_type} reconciliation process")
        current_installed_addons = set(t["name"] for t in self._wp_addon_list(addon_type).result)
        logger.debug(f"Currently installed {addon_type}s %s", current_installed_addons)
        addons_in_config = [
            t.strip() for t in self.model.config[f"{addon_type}s"].split(",") if t.strip()
        ]
        default_addons = (
            self._WORDPRESS_DEFAULT_THEMES
            if addon_type == "theme"
            else self._WORDPRESS_DEFAULT_PLUGINS
        )
        desired_addons = set(itertools.chain(addons_in_config, default_addons))
        install_addons = desired_addons - current_installed_addons
        uninstall_addons = current_installed_addons - desired_addons
        for addon in install_addons:
            logger.debug(f"Install {addon_type}: %s", repr(addon))
            result = self._wp_addon_install(addon_type=addon_type, addon_name=addon)
            if not result.success:
                raise exceptions.WordPressBlockedStatusException(
                    f"failed to install {addon_type} {repr(addon)}"
                )
        for addon in uninstall_addons:
            logger.debug(f"Uninstall {addon}: %s", repr(addon))
            result = self._wp_addon_uninstall(addon_type=addon_type, addon_name=addon)
            if not result.success:
                raise exceptions.WordPressBlockedStatusException(
                    f"failed to uninstall {addon_type} {repr(addon)}"
                )

    def _theme_reconciliation(self):
        """Reconciliation process for WordPress themes

        Install and uninstall themes to match the themes setting in config
        """
        self._addon_reconciliation("theme")

    def _wp_option_update(self, option, value, format="plaintext"):
        """Create or update a WordPress option value

        If the option does not exist, wp option update will create one.

        Args:
            option (str): WordPress option name.
            value (Union[str, dict]): WordPress option value. If the format is ``"plaintext"``,
                then it's a str. If the format is ``"json"``, the value should be a json compatible
                dict.
            format (str): ``"plaintext"`` or ``"json"``

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        return self._wrapped_run_wp_cli(
            ["wp", "option", "update", option, value, f"--format={format}"]
        )

    def _wp_option_delete(self, option):
        """Delete a WordPress option

        It's not an error to delete a non-existent option (it's a warning though).

        Args:
            option (str): option name.

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        return self._wrapped_run_wp_cli(["wp", "option", "delete", option])

    def _wp_plugin_activate(self, plugin):
        """Activate a WordPress plugin

        Args:
            plugin (str): plugin slug.

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        logger.info("activate plugin %s", repr(plugin))
        return self._wrapped_run_wp_cli(["wp", "plugin", "activate", plugin])

    def _wp_plugin_deactivate(self, plugin):
        """Deactivate a WordPress plugin

        Args:
            plugin (str): plugin slug.

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        logger.info("deactivate plugin %s", repr(plugin))
        return self._wrapped_run_wp_cli(["wp", "plugin", "deactivate", plugin])

    def _perform_plugin_activate_or_deactivate(self, plugin, action):
        """Activate a WordPress plugin or deactivate a WordPress plugin.

        It's not an error to activate an active plugin or deactivate an inactive plugin.

        Args:
            plugin (str): plugin slug.
            action (str): ``"activate"`` or ``"deactivate"``

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        if action not in ("activate", "deactivate"):
            raise ValueError(
                f"Unknown activation_status {repr(action)}, " "accept (activate, deactivate)"
            )

        current_plugins = self._wp_addon_list("plugin")
        if not current_plugins.success:
            return self._ExecResult(
                success=False,
                result=None,
                message=f"failed to list installed plugins while {action} plugin {plugin}",
            )
        current_plugins = current_plugins.result
        current_plugins_activate_status = {p["name"]: p["status"] for p in current_plugins}

        if plugin not in current_plugins_activate_status:
            return self._ExecResult(
                success=False, result=None, message=f"{action} a non-existent plugin {plugin}"
            )
        is_active = current_plugins_activate_status[plugin] == "active"
        target_activation_status = action == "activate"

        if is_active != target_activation_status:
            if action == "activate":
                result = self._wp_plugin_activate(plugin)
            else:
                result = self._wp_plugin_deactivate(plugin)
            if not result.success:
                return self._ExecResult(
                    success=False, result=None, message=f"failed to {action} plugin {plugin}"
                )
        return self._ExecResult(success=True, result=None, message="")

    def _activate_plugin(self, plugin, options):
        """Activate a WordPress plugin and set WordPress options after activation

        Args:
            plugin (str): plugin slug.
            options (Dict[str, Union[str, dict])): options related to the plugin, if the value is
                a string, it will be passed as plaintext, else if the value is a dict, the option
                value will be passed as json.

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        activate_result = self._perform_plugin_activate_or_deactivate(plugin, "activate")
        if not activate_result.success:
            return activate_result
        for option, value in options.items():
            if isinstance(value, dict):
                option_update_result = self._wp_option_update(
                    option=option, value=json.dumps(value), format="json"
                )
            else:
                option_update_result = self._wp_option_update(option=option, value=value)
            if not option_update_result.success:
                return self._ExecResult(
                    success=False,
                    result=None,
                    message=f"failed to update option {option} after activating plugin {plugin}",
                )
        return self._ExecResult(success=True, result=None, message="")

    def _deactivate_plugin(self, plugin, options):
        """Deactivate a WordPress plugin and delete WordPress options after deactivation

        Args:
            plugin (str): plugin slug.
            options (List[str]): options related to the plugin that need to be removed.

        Returns:
            An instance of :attr:`charm.WordpressCharm._ExecResult`.
        """
        deactivate_result = self._perform_plugin_activate_or_deactivate(plugin, "deactivate")
        if not deactivate_result.success:
            return deactivate_result
        for option in options:
            option_update_result = self._wp_option_delete(option)
            if not option_update_result.success:
                return self._ExecResult(
                    success=False,
                    result=None,
                    message=f"failed to delete option {option} after deactivating plugin {plugin}",
                )
        return self._ExecResult(success=True, result=None, message="")

    def _plugin_akismet_reconciliation(self):
        """Reconciliation process for the akismet plugin"""
        akismet_key = self.model.config["wp_plugin_akismet_key"].strip()
        if not akismet_key:
            result = self._deactivate_plugin(
                "akismet",
                ["akismet_strictness", "akismet_show_user_comments_approved", "wordpress_api_key"],
            )
        else:
            result = self._activate_plugin(
                "akismet",
                {
                    "akismet_strictness": "0",
                    "akismet_show_user_comments_approved": "0",
                    "wordpress_api_key": akismet_key,
                },
            )
        if not result.success:
            raise exceptions.WordPressBlockedStatusException(
                f"Unable to config akismet plugin, {result.message}"
            )

    @staticmethod
    def _encode_openid_team_map(team_map):
        """Convert wp_plugin_openid_team_map setting to WordPress openid_teams_trust_list option

        example input: site-sysadmins=administrator,site-editors=editor,site-executives=editor

        Args:
            team_map (str): team definition.

        Returns:
            A serialized PHP array, as a Python string.
        """
        team_map_lines = []
        i = 0
        team_map_lines.append("a:{}:{{".format(len(team_map.split(","))))
        for mapping in team_map.split(","):
            i = i + 1
            team, role = mapping.split("=", 2)
            team_map_lines.append("i:{};".format(i))
            team_map_lines.append('O:8:"stdClass":4:{')
            team_map_lines.append('s:2:"id";')
            team_map_lines.append("i:{};".format(i))
            team_map_lines.append('s:4:"team";')
            team_map_lines.append('s:{}:"{}";'.format(len(team), team))
            team_map_lines.append('s:4:"role";')
            team_map_lines.append('s:{}:"{}";'.format(len(role), role))
            team_map_lines.append('s:6:"server";')
            team_map_lines.append('s:1:"0";')
            team_map_lines.append("}")
        team_map_lines.append("}")

        return "".join(team_map_lines)

    def _plugin_openid_reconciliation(self):
        """Reconciliation process for the openid plugin"""
        openid_team_map = self.model.config["wp_plugin_openid_team_map"].strip()
        if not openid_team_map:
            result = self._deactivate_plugin(
                "openid", ["openid_required_for_registration", "openid_teams_trust_list"]
            )
        else:
            result = self._activate_plugin(
                "openid",
                {
                    "openid_required_for_registration": "1",
                    "openid_teams_trust_list": self._encode_openid_team_map(openid_team_map),
                },
            )
        if not result.success:
            raise exceptions.WordPressBlockedStatusException(
                f"Unable to config openid plugin, {result.message}"
            )

    def _apache_config_is_enabled(self, conf_name):
        """Check if a specified apache configuration file is enabled

        Args:
            conf_name (str): name of the apache config, without trailing ``.conf``.

        Returns:
            True if certain apache config is enabled.
        """
        enabled_config = [
            name for name in self._container().list_files("/etc/apache2/conf-enabled")
        ]
        return f"{conf_name}.conf" in enabled_config

    def _apache_enable_config(self, conf_name, conf):
        """Create and enable an apache2 configuration file

        Args:
            conf_name (str): name of the apache config, without trailing ``.conf``.
            conf (str): content of the apache config.
        """
        self._stop_server()
        self._container().push(path=f"/etc/apache2/conf-available/{conf_name}.conf", source=conf)
        self._run_cli(["a2enconf", conf_name])
        self._start_server()

    def _apache_disable_config(self, conf_name):
        """Remove and disable a specified apache2 configuration file

        Args:
            conf_name (str): name of the apache config, without trailing ``.conf``.
        """
        self._stop_server()
        self._container().remove_path(
            f"/etc/apache2/conf-available/{conf_name}.conf", recursive=True
        )
        self._run_cli(["a2disconf", conf_name])
        self._start_server()

    def _plugin_swift_reconciliation(self):
        """Reconciliation process for swift object storage (openstack-objectstorage-k8s) plugin"""
        swift_config_str = self.model.config["wp_plugin_openstack-objectstorage_config"]
        swift_config_key = [
            'auth-url',
            'bucket',
            'password',
            'object-prefix',
            'region',
            'tenant',
            'domain',
            'swift-url',
            'username',
            'copy-to-swift',
            'serve-from-swift',
            'remove-local-file',
        ]
        enable_swift = bool(swift_config_str.strip())
        if not enable_swift:
            result = self._deactivate_plugin("openstack-objectstorage-k8s", ["object_storage"])
        else:
            swift_config = safe_load(swift_config_str)
            for key in swift_config_key:
                if key not in swift_config:
                    raise exceptions.WordPressBlockedStatusException(
                        f"missing {key} in wp_plugin_openstack-objectstorage_config"
                    )
            result = self._activate_plugin(
                "openstack-objectstorage-k8s", {"object_storage": swift_config}
            )
        if not result.success:
            raise exceptions.WordPressBlockedStatusException(
                f"Unable to config openstack-objectstorage-k8s plugin, {result.message}"
            )
        apache_swift_conf = "docker-php-swift-proxy"
        swift_apache_config_enabled = self._apache_config_is_enabled(apache_swift_conf)
        if enable_swift and not swift_apache_config_enabled:
            swift_url = swift_config.get("swift-url")
            bucket = swift_config.get("bucket")
            object_prefix = swift_config.get("object-prefix")
            redirect_url = os.path.join(swift_url, bucket, object_prefix)
            conf = textwrap.dedent(
                f"""\
            SSLProxyEngine on
            ProxyPass /wp-content/uploads/ {redirect_url}
            ProxyPassReverse /wp-content/uploads/ {redirect_url}
            Timeout 300
            """
            )
            self._apache_enable_config(apache_swift_conf, conf)
        elif not enable_swift and swift_apache_config_enabled:
            self._apache_config_is_enabled(apache_swift_conf)

    def _plugin_reconciliation(self):
        """Reconciliation process for WordPress plugins.

        Install and uninstall plugins to match the plugins setting in config.
        Activate and deactivate three charm managed plugins (akismet, openid, openstack-swift)
        and adjust plugin options for these three plugins according to charm config.
        """
        self._addon_reconciliation("plugin")
        if self.unit.is_leader():
            self._plugin_akismet_reconciliation()
            self._plugin_openid_reconciliation()
            self._plugin_swift_reconciliation()

    def _reconciliation(self, _event):
        logger.info("Start reconciliation process, triggered by %s", _event)
        if not self._container().can_connect():
            logger.info("Reconciliation process terminated early, pebble is not ready")
            self.unit.status = WaitingStatus("Waiting for pebble")
            return
        try:
            self._core_reconciliation()
            self._theme_reconciliation()
            self._plugin_reconciliation()
            logger.info("Reconciliation process finished successfully.")
            self.unit.status = ActiveStatus()
        except exceptions.WordPressStatusException as status_exception:
            logger.info("Reconciliation process terminated early, reason: %s", status_exception)
            self.unit.status = status_exception.status


if __name__ == "__main__":  # pragma: no cover
    main(WordpressCharm)
