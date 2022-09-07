#!/usr/bin/env python3
import logging
import re
import os
import secrets
import string
import textwrap

import ops.charm
import ops.pebble
import yaml

import charm
from yaml import safe_load

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

from charms.nginx_ingress_integrator.v0.ingress import IngressRequires
from leadership import LeadershipSettings
from opslib.mysql import MySQLClient

from wordpress import Wordpress, password_generator, WORDPRESS_SECRETS


logger = logging.getLogger()


def juju_setting_to_list(config_string, split_char=" "):
    "Transforms Juju setting strings into a list, defaults to splitting on whitespace."
    return config_string.split(split_char)


class WordpressFirstInstallEvent(EventBase):
    """Custom event for signalling Wordpress initialisation.

    WordpressInitialiseEvent allows us to signal the handler for
    the initial Wordpress setup logic.
    """

    pass


class WordpressStaticDatabaseChanged(EventBase):
    """Custom event for static Database configuration changed.

    WordpressStaticDatabaseChanged provides the same interface as the
    db.on.database_changed event which enables the WordPressCharm's
    on_database_changed handler to update state for both relation and static
    database configuration events.
    """

    @property
    def database(self):
        return self.model.config["db_name"]

    @property
    def host(self):
        return self.model.config["db_host"]

    @property
    def user(self):
        return self.model.config["db_user"]

    @property
    def password(self):
        return self.model.config["db_password"]

    @property
    def model(self):
        return self.framework.model


class WordpressCharmEvents(CharmEvents):
    """Register custom charm events.

    WordpressCharmEvents registers the custom WordpressFirstInstallEvent
    and WordpressStaticDatabaseChanged event to the charm.
    """

    wordpress_initial_setup = EventSource(WordpressFirstInstallEvent)
    wordpress_static_database_changed = EventSource(WordpressStaticDatabaseChanged)


class WordpressCharm(CharmBase):
    class _ReplicaRelationNotReady(Exception):
        pass

    _WP_CONFIG_PATH = "/var/www/html/wp-config.php"
    _CONTAINER_NAME = "wordpress"
    _SERVICE_NAME = "wordpress"

    _container_name = "wordpress"
    _default_service_port = 80

    state = StoredState()
    on = WordpressCharmEvents()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.leader_data = LeadershipSettings()

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

        self.wordpress = Wordpress(c)

        self.ingress = IngressRequires(self, self.ingress_config)
        self.framework.observe(self.on.leader_elected, self._on_leader_elected_replica_data_handler)
        self.framework.observe(self.db.on.database_changed, self._on_relation_database_changed)
        self.framework.observe(self.on.config_changed, self._reconciliation)

    @property
    def container_name(self):
        return self._container_name

    @property
    def service_ip_address(self):
        return os.environ.get("WORDPRESS_SERVICE_SERVICE_HOST")

    @property
    def service_port(self):
        return self._default_service_port

    @property
    def wordpress_workload(self):
        """Returns the WordPress pebble workload configuration."""
        return {
            "summary": "WordPress layer",
            "description": "pebble config layer for WordPress",
            "services": {
                "wordpress-plugins": {
                    "override": "replace",
                    "summary": "WordPress plugin updater",
                    "command": (
                        "bash -c '/srv/wordpress-helpers/plugin_handler.py && "
                        "stat /srv/wordpress-helpers/.ready && "
                        "sleep infinity'"
                    ),
                    "after": ["apache2"],
                    "environment": self._env_config,
                },
                "wordpress-init": {
                    "override": "replace",
                    "summary": "WordPress initialiser",
                    "command": (
                        "bash -c '"
                        "/charm/bin/wordpressInit.sh >> /wordpressInit.log 2>&1"
                        "'"
                    ),
                    "environment": self._env_config,
                },
                "apache2": {
                    "override": "replace",
                    "summary": "Apache2 service",
                    "command": (
                        "bash -c '"
                        "apache2ctl -D FOREGROUND -E /apache-error.log -e debug >>/apache-sout.log 2>&1"
                        "'"
                    ),
                    "requires": ["wordpress-init"],
                    "after": ["wordpress-init"],
                    "environment": self._env_config,
                },
                self.container_name: {
                    "override": "replace",
                    "summary": "WordPress service",
                    "command": "sleep infinity",
                    "requires": ["apache2", "wordpress-plugins"],
                    "environment": self._env_config,
                },
            },
        }

    @property
    def ingress_config(self):
        blog_hostname = self.state.blog_hostname
        ingress_config = {
            "service-hostname": blog_hostname,
            "service-name": self.app.name,
            "service-port": self.service_port,
        }
        tls_secret_name = self.model.config["tls_secret_name"]
        if tls_secret_name:
            ingress_config["tls-secret-name"] = tls_secret_name
        return ingress_config

    @property
    def _db_config(self):
        """Kubernetes Pod environment variables."""
        # TODO: make this less fragile.
        if self.unit.is_leader():
            return {
                "WORDPRESS_DB_HOST": self.state.db_host,
                "WORDPRESS_DB_NAME": self.state.db_name,
                "WORDPRESS_DB_USER": self.state.db_user,
                "WORDPRESS_DB_PASSWORD": self.state.db_password,
            }
        else:
            return {
                "WORDPRESS_DB_HOST": self.leader_data["db_host"],
                "WORDPRESS_DB_NAME": self.leader_data["db_name"],
                "WORDPRESS_DB_USER": self.leader_data["db_user"],
                "WORDPRESS_DB_PASSWORD": self.leader_data["db_password"],
            }

    @property
    def _env_config(self):
        """Kubernetes Pod environment variables."""
        config = dict(self.model.config)
        env_config = {}
        if config["container_config"].strip():
            env_config = safe_load(config["container_config"])

        env_config["WORDPRESS_BLOG_HOSTNAME"] = self.state.blog_hostname
        initial_settings = {}
        if config["initial_settings"].strip():
            initial_settings.update(safe_load(config["initial_settings"]))
        # TODO: make these class default attributes
        env_config["WORDPRESS_ADMIN_USER"] = initial_settings.get("user_name", "admin")
        env_config["WORDPRESS_ADMIN_EMAIL"] = initial_settings.get("admin_email", "nobody@localhost")

        env_config["WORDPRESS_INSTALLED"] = self.state.installed_successfully
        env_config.update(self._wordpress_secrets)

        if not config["tls_secret_name"]:
            env_config["WORDPRESS_TLS_DISABLED"] = "true"
        if config.get("wp_plugin_openid_team_map"):
            env_config["WP_PLUGIN_OPENID_TEAM_MAP"] = config["wp_plugin_openid_team_map"]

        # Add secrets from charm config.
        if config.get("wp_plugin_akismet_key"):
            env_config["WP_PLUGIN_AKISMET_KEY"] = config["wp_plugin_akismet_key"]
        if config.get("wp_plugin_openstack-objectstorage_config"):
            # Actual plugin name is 'openstack-objectstorage', but we're only
            # implementing the 'swift' portion of it.
            wp_plugin_swift_config = safe_load(config.get("wp_plugin_openstack-objectstorage_config"))
            env_config["SWIFT_AUTH_URL"] = wp_plugin_swift_config.get("auth-url")
            env_config["SWIFT_BUCKET"] = wp_plugin_swift_config.get("bucket")
            env_config["SWIFT_PASSWORD"] = wp_plugin_swift_config.get("password")
            env_config["SWIFT_PREFIX"] = wp_plugin_swift_config.get("prefix")
            env_config["SWIFT_REGION"] = wp_plugin_swift_config.get("region")
            env_config["SWIFT_TENANT"] = wp_plugin_swift_config.get("tenant")
            env_config["SWIFT_URL"] = wp_plugin_swift_config.get("url")
            env_config["SWIFT_USERNAME"] = wp_plugin_swift_config.get("username")
            env_config["SWIFT_COPY_TO_SWIFT"] = wp_plugin_swift_config.get("copy-to-swift")
            env_config["SWIFT_SERVE_FROM_SWIFT"] = wp_plugin_swift_config.get("serve-from-swift")
            env_config["SWIFT_REMOVE_LOCAL_FILE"] = wp_plugin_swift_config.get("remove-local-file")

        env_config.update(self._db_config)
        return env_config

    def on_wordpress_uninitialised(self, event):
        """Setup the WordPress service with default values.

        WordPress will expose the setup page to the user to manually
        configure with their browser. This isn't ideal from a security
        perspective so the charm will initialise the site for you and
        expose the admin password via `get_initial_password_action`.

        This method observes all changes to the system by registering
        to the .on.config_changed event. This avoids current state split
        brain issues because all changes to the system sink into
        `on.config_changed`.

        It defines the state of the install ready state as:
          - We aren't leader, so check leader_data install state for the installed state answer.
          - We aren't ready to setup WordPress yet (missing configuration data).
          - We're ready to do the initial setup of WordPress (all dependent configuration data set).
          - We're currently setting up WordPress, lock out any other events from attempting to install.
          - WordPress is operating in a production capacity, no more work to do, no-op.
        """

        if self.unit.is_leader() is False:
            # Poorly named, expect a separate flag for non leader units here.
            self.state.installed_successfully = self.leader_data.setdefault("installed", False)

        if self.state.installed_successfully is True:
            logger.warning("already installed, nothing more to do...")
            return

        # By using sets we're able to follow a state relay pattern. Each event handler that is
        # responsible for setting state adds their flag to the set. Once thet set is complete
        # it will be observed here. During the install phase we use StoredState as a mutex lock
        # to avoid race conditions with future events. By calling .emit() we flush the current
        # state to persistent storage which ensures future events do not observe stale state.
        first_time_ready = {"leader", "db", "ingress", "leader"}
        install_running = {"attempted", "ingress", "db", "leader"}

        logger.debug(
            (
                f"DEBUG: current install ready state is {self.state.install_state}, "
                f"required install ready state is {first_time_ready}"
            )
        )

        if self.state.install_state == install_running:
            logger.info("Install phase currently running...")
            BlockedStatus("WordPress installing...")

        elif self.state.install_state == first_time_ready:
            # TODO:
            # Check if WordPress is already installed.
            # Would be something like
            #   if self.is_vhost_ready():[...]
            WaitingStatus("WordPress not installed yet...")
            self.state.attempted_install = True
            self.state.install_state.add("attempted")
            logger.info("Attempting WordPress install...")
            self.on.wordpress_initial_setup.emit()

    def on_wordpress_initial_setup(self, event):
        logger.info("Beginning WordPress setup process...")
        container = self.unit.get_container(self.container_name)
        container.add_layer(self.container_name, self.wordpress_workload, combine=True)

        # Temporary workaround until the init script is baked into the Dockerimage.
        setup_service = "wordpressInit"
        src_path = f"src/{setup_service}.sh"
        charm_bin = "/charm/bin"
        dst_path = f"{charm_bin}/{setup_service}.sh"
        with open(src_path, "r", encoding="utf-8") as f:
            container.push(dst_path, f, permissions=0o755)

        admin_password = "/admin_password"
        config = self._get_initial_password()
        container.push(admin_password, config, permissions=0o400)

        logger.info("Adding WordPress layer to container...")
        self.ingress.update_config(self.ingress_config)
        container = self.unit.get_container(self.container_name)
        pebble = container.pebble
        wait_on = pebble.start_services([self.container_name])
        pebble.wait_change(wait_on)

        logger.info("first time WordPress install was successful...")
        container.remove_path(admin_password)
        self.unit.status = MaintenanceStatus("WordPress Initialised")

        wait_on = pebble.stop_services([s for s in self.wordpress_workload["services"]])
        self.leader_data["installed"] = True
        self.state.installed_successfully = True
        self.on.config_changed.emit()

    def on_config_changed(self, event):
        """Merge charm configuration transitions."""
        logger.debug(f"Event {event} install ready state is {self.state.install_state}")

        is_valid = self.is_valid_config()
        if not is_valid:
            return

        container = self.unit.get_container(self.container_name)
        services = container.get_plan().to_dict().get("services", {})

        if services != self.wordpress_workload["services"]:
            logger.info("WordPress configuration transition detected...")
            self.unit.status = MaintenanceStatus("Transitioning WordPress configuration")
            container.add_layer(self.container_name, self.wordpress_workload, combine=True)

            self.unit.status = MaintenanceStatus("Restarting WordPress")
            running_services = [s for s in self.wordpress_workload["services"] if container.get_service(s).is_running()]
            if running_services:
                container.pebble.stop_services(running_services)

            # Temporary workaround until the init script is baked into the Dockerimage.
            setup_service = "wordpressInit"
            src_path = f"src/{setup_service}.sh"
            charm_bin = "/charm/bin"
            dst_path = f"{charm_bin}/{setup_service}.sh"
            with open(src_path, "r", encoding="utf-8") as f:
                container.push(dst_path, f, permissions=0o755)

            container.start(self.container_name)

            self.unit.status = ActiveStatus("WordPress service is live!")
            self.ingress.update_config(self.ingress_config)

    def on_database_config_changed(self, event):
        """Handle when the user supplies database details via charm config.
        """
        if self.state.has_db_relation is False:
            db_config = {k: v or None for (k, v) in self.model.config.items() if k.startswith("db_")}
            if any(db_config.values()) is True:  # User has supplied db config.
                current_db_data = {self.state.db_host, self.state.db_name, self.state.db_user, self.state.db_password}
                new_db_data = {db_config.values()}
                db_differences = current_db_data.difference(new_db_data)
                if db_differences:
                    self.on.wordpress_static_database_changed.emit()

    def on_db_relation_created(self, event):
        """Handle the db-relation-created hook.

        We need to handle this hook to switch from database
        credentials being specified in the charm configuration
        to being provided by the relation.
        """

        self.state.db_host = None
        self.state.db_name = None
        self.state.db_user = None
        self.state.db_password = None
        self.state.has_db_relation = True
        self.on.config_changed.emit()

    def on_db_relation_broken(self, event):
        """Handle the db-relation-broken hook.

        We need to handle this hook to switch from database
        credentials being provided by the relation to being
        specified in the charm configuration.
        """
        self.state.db_host = None
        self.state.db_name = None
        self.state.db_user = None
        self.state.db_password = None
        self.state.has_db_relation = False
        self.on.config_changed.emit()

    def on_database_changed(self, event):
        """Handle the MySQL configuration changed event.

        The MySQLClient (self.db) and WordpressStaticDatabaseChanged
        (self.on.wordpress_static_database_changed ) emits this event whenever
        the database credentials have changed, this also includes when they
        disappear as part of relation tear down. In addition to handling the
        MySQLClient relation, this method handles the case where db
        configuration is supplied by the user via model config. See
        WordpressStaticDatabaseChanged for details.
        """
        # TODO: we could potentially remove setting database config from state
        # entirely and just rely on leader_data.
        self.state.db_host = event.host
        self.state.db_name = event.database
        self.state.db_user = event.user
        self.state.db_password = event.password

        if self.unit.is_leader():
            self.leader_data["db_host"] = event.host
            self.leader_data["db_name"] = event.database
            self.leader_data["db_user"] = event.user
            self.leader_data["db_password"] = event.password

        self.state.has_db_relation = True
        self.state.install_state.add("db")
        self.on.config_changed.emit()

    def on_ingress_relation_broken(self, event):
        """Handle the ingress-relation-broken hook.
        """
        self.ingress.update_config({})
        self.state.has_ingress_relation = False
        self.state.install_state.discard("ingress")
        self.on.config_changed.emit()

    def on_ingress_relation_changed(self, event):
        """Store the current ingress IP address on relation changed."""
        self.state.has_ingress_relation = True
        self.state.install_state.add("ingress")
        self.on.config_changed.emit()

    def on_leader_elected(self, event):
        """Setup common workload state.

        This includes:
          - database config.
          - wordpress secrets.
        """
        if self.unit.is_leader() is True:
            if not all(self._wordpress_secrets.values()):
                self._generate_wordpress_secrets()
            self.state.install_state.add("leader")

        else:
            if not all(self._db_config.values()) or not all(self._wordpress_secrets.values()):
                logger.info("Non leader has unexpected db_config or wp secrets...")

        self.on.config_changed.emit()

    def is_valid_config(self):
        """Validate that the current configuration is valid.

        Before the workload can start we must ensure all prerequisite state
        is present, the config_changed handler uses the return value here.
        to guard the WordPress service from prematurely starting.
        """
        # TODO: This method is starting to look a bit wild and should definitely
        # be refactored.
        is_valid = True
        config = dict(self.model.config)

        if self.state.installed_successfully is False:
            logger.info("WordPress has not been setup yet...")
            is_valid = False

        if not config.get("initial_settings"):
            logger.info("No initial_setting provided. Skipping first install.")
            self.model.unit.status = BlockedStatus("Missing initial_settings")
            is_valid = False

        want = ["image"]

        db_state = self._db_config.values()
        if not all(db_state):
            want.extend(["db_host", "db_name", "db_user", "db_password"])
            logger.info("MySQL relation has not yet provided database credentials.")
            is_valid = False

        missing = [k for k in want if config[k].rstrip() == ""]
        if missing:
            message = "Missing required config or relation: {}".format(" ".join(missing))
            logger.info(message)
            self.model.unit.status = BlockedStatus(message)
            is_valid = False

        if config["additional_hostnames"]:
            additional_hostnames = juju_setting_to_list(config["additional_hostnames"])
            valid_domain_name_pattern = re.compile(r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$")
            valid = [re.match(valid_domain_name_pattern, h) for h in additional_hostnames]
            if not all(valid):
                message = "Invalid additional hostnames supplied: {}".format(config["additional_hostnames"])
                logger.info(message)
                self.model.unit.status = BlockedStatus(message)
                is_valid = False
        return is_valid

    def _generate_wordpress_secrets(self):
        """Generate WordPress auth keys and salts.

        Secret data should be in sync for each container workload
        so persist the state in leader_data.
        """
        wp_secrets = {}
        for secret in WORDPRESS_SECRETS:
            # `self.leader_data` itself will never return a KeyError, but
            # checking for the presence of an item before setting it will make
            # it easier to test, as we can simply set `self.leader_data` to
            # be a dictionary.
            if secret not in self.leader_data or not self.leader_data[secret]:
                self.leader_data[secret] = password_generator(64)
            wp_secrets[secret] = self.leader_data[secret]
        return wp_secrets

    @property
    def _wordpress_secrets(self):
        """WordPress auth keys and salts.
        """
        wp_secrets = {}
        for secret in WORDPRESS_SECRETS:
            wp_secrets[secret] = self.leader_data.get(secret)
        return wp_secrets

    def is_service_up(self):
        """Check to see if the HTTP service is up"""
        service_ip = self.service_ip_address
        if service_ip:
            return self.wordpress.is_vhost_ready(service_ip)
        return False

    # TODO: If a non leader unit invokes this method and the data
    # doesn't exist, it will raise an exception. It needs to be refactored.
    def _get_initial_password(self):
        """Get the initial password.

        If a password hasn't been set yet, create one if we're the leader,
        or return an empty string if we're not."""
        initial_password = self.leader_data["initial_password"]
        if not initial_password:
            if self.unit.is_leader():
                initial_password = password_generator()
                self.leader_data["initial_password"] = initial_password
        return initial_password

    def _on_get_initial_password_action(self, event):
        """Handle the get-initial-password action."""
        initial_password = self._get_initial_password()
        if initial_password:
            event.set_results({"password": initial_password})
        else:
            event.fail("Initial password has not been set yet.")

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
            'nonce_salt'
        ]

    def _generate_wp_secret_keys(self):
        def _wp_generate_password(length=64):
            characters = string.ascii_letters + "!@#$%^&*()" + "-_ []{}<>~`+=,.;:/?|"
            return "".join(secrets.choice(characters) for _ in range(length))

        wp_secrets = {
            field: _wp_generate_password()
            for field in self._wordpress_secret_key_fields()
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
        """Test if the synchronized data required for WordPress replication are initialized."""
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
        """
        if not self._replica_consensus_reached() and self.unit.is_leader():
            replica_relation_data = self._replica_relation_data()
            new_replica_data = self._generate_wp_secret_keys()
            for secret_key, secret_value in new_replica_data.items():
                replica_relation_data[secret_key] = secret_value

    def _on_relation_database_changed(self, event):
        """ Callback function to handle db relation changes (data changes/relation breaks)

        This method will set all db relation related states ``relation_db_*`` when db relation
        changes and will reset all that to ``None`` after db relation is broken.
        """
        self.state.relation_db_host = event.host
        self.state.relation_db_name = event.database
        self.state.relation_db_user = event.user
        self.state.relation_db_password = event.password

    def _gen_wp_config(self):
        """Generate the wp-config.php file WordPress needs based on charm config and relations

        This method will not check the validity of the configuration or current state,
        unless they are security related, in that case, an exception will be raised.
        """
        wp_config = [
            textwrap.dedent("""\
            <?php
            # This file is managed by Juju. Do not make local changes.
            if (strpos($_SERVER['HTTP_X_FORWARDED_PROTO'], 'https') !== false) {
                $_SERVER['HTTPS']='on';
            }
            $_w_p_http_protocol = 'http://';
            if (!empty($_SERVER['HTTPS']) && 'off' != $_SERVER['HTTPS']) {
                $_w_p_http_protocol = 'https://';
            }
            define( 'WP_PLUGIN_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] . '/wp-content/plugins' );
            define( 'WP_CONTENT_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] . '/wp-content' );
            define( 'WP_SITEURL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );
            define( 'WP_URL', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );
            define( 'WP_HOME', $_w_p_http_protocol . $_SERVER['HTTP_HOST'] );""")
        ]

        # database info in config takes precedence over database info provided by relations
        database_info = {
            key.upper(): self.model.config[key] for key in
            ["db_host", "db_name", "db_user", "db_password"]
        }
        if any(not value for value in database_info.values()):
            database_info = {
                key.upper(): getattr(self.state, "relation_" + key) for key in
                ["db_host", "db_name", "db_user", "db_password"]
            }
        for db_key, db_value in database_info.items():
            wp_config.append("define( '{}', '{}' );".format(db_key, db_value))

        replica_relation_data = self._replica_relation_data()
        for secret_key in self._wordpress_secret_key_fields():
            secret_value = replica_relation_data.get(secret_key)
            if not secret_value:
                raise ValueError("{} value is empty".format(secret_key))
            wp_config.append("define( '{secret_key}', '{secret_value}' );".format(
                secret_key=secret_key.upper(),
                secret_value=secret_value
            ))

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
        """Get the WordPress workload container"""
        return self.unit.get_container(self._CONTAINER_NAME)

    def _wordpress_service_exists(self):
        return self._SERVICE_NAME in self._container().get_plan().services

    def _stop_server(self):
        """Stop WordPress (apache) server

        This operation is idempotence.
        """
        if self._wordpress_service_exists():
            self._container().stop(self._SERVICE_NAME)

    def _wp_is_installed(self):
        """Check if WordPress is installed (check if WordPress related tables exist in database)"""
        process = self._container().exec(["wp", "core", "is-installed"], timeout=60)
        try:
            process.wait()
            return True
        except ops.pebble.ExecError:
            return False

    def _wp_install_cmd(self):
        """Generate wp-cli command used to install WordPress on database"""
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
            f"--admin_password={admin_password}"
        ]

    def _wp_install(self):
        """Install WordPress (create WordPress required tables in DB)"""
        process = self._container().exec(self._wp_install_cmd(), timeout=60)
        process.wait()

    def _init_pebble_layer(self):
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
        self._init_pebble_layer()
        if self.unit.is_leader():
            if not self._wp_is_installed():
                self._wp_install()
            if self._current_wp_config() is None:
                # For security reasons, never start WordPress server if wp-config.php not exists
                raise FileNotFoundError(
                    "required file (wp-config.php) for starting WordPress server not exists"
                )
        self._container().start(self._SERVICE_NAME)

    def _current_wp_config(self):
        """Retrieve the current version of wp-config.php from server, return None if not exists"""
        wp_config_path = self._WP_CONFIG_PATH
        container = self._container()
        if container.exists(wp_config_path):
            return self._container().pull(wp_config_path).read()
        return None

    def _remove_wp_config(self):
        """Remove wp-config.php file on server"""
        container = self._container()
        if container.get_service(self._SERVICE_NAME).is_running():
            # For security reasons, prevent removing wp-config.php while WordPress server running
            raise RuntimeError("trying to delete wp-config.php while WordPress server is running")
        self._container().remove_path(self._WP_CONFIG_PATH, recursive=True)

    def _push_wp_config(self, wp_config):
        """Update the content of wp-config.php on server"""
        self._container().push(self._WP_CONFIG_PATH, wp_config)

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
        if not self._replica_consensus_reached():
            self.unit.status = WaitingStatus("Waiting for unit consensus")
            self._stop_server()
            return
        db_config_ready = all(
            self.model.config[key] for key in
            ("db_host", "db_name", "db_user", "db_password")
        )
        db_relation_ready = all(
            getattr(self.state, "relation_" + key) for key in
            ("db_host", "db_name", "db_user", "db_password")
        )
        if not db_config_ready and not db_relation_ready:
            self.unit.status = WaitingStatus("Waiting for db relation")
            self._stop_server()
            return
        wp_config = self._gen_wp_config()
        if wp_config != self._current_wp_config():
            self._stop_server()
            self._push_wp_config(wp_config)
        if self._current_wp_config() is not None:
            self._start_server()
        return

    def _reconciliation(self, _event):
        if not self._container().can_connect():
            self.unit.status = WaitingStatus("Waiting for pebble")
            return
        self._core_reconciliation()


if __name__ == "__main__":  # pragma: no cover
    main(WordpressCharm)
