# src/dbt/dbt_runner.py

from __future__ import annotations

import logging
import os
import shlex
import shutil
import subprocess
import time
import tomllib

from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path


logger = logging.getLogger(__name__)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

DEFAULT_DBT_PROJECT_DIR = (
    PROJECT_ROOT
    / "dbt_act"
)

DEFAULT_DBT_EXECUTABLE = (
    PROJECT_ROOT
    / ".venv"
    / "bin"
    / "dbt"
)

DEFAULT_SNOWFLAKE_CONNECTION_FILE = (
    Path.home()
    / ".snowflake"
    / "connections.toml"
)


# ============================================================
# RESULT
# ============================================================

@dataclass
class DbtRunResult:
    """
    Result returned after a dbt command completes.
    """

    command: str

    status: str

    return_code: int

    duration_seconds: float

    project_dir: str

    dbt_executable: str


    def to_dict(self) -> dict:
        """
        Convert result to an Airflow XCom-friendly dictionary.
        """

        return asdict(self)


# ============================================================
# DBT RUNNER
# ============================================================

class DbtRunner:
    """
    Execute ACT dbt commands from Python.

    Important architecture:

        Airflow
            |
            | Python subprocess
            v
        project .venv/bin/dbt
            |
            v
        dbt_act
            |
            v
        Snowflake

    Airflow therefore does NOT need dbt installed inside the
    dedicated .airflow-venv.
    """

    def __init__(
        self,
        project_dir: str | Path | None = None,
        dbt_executable: str | Path | None = None,
        connection_name: str | None = None,
        connection_file: str | Path | None = None,
    ) -> None:

        # ====================================================
        # DBT PROJECT
        # ====================================================

        configured_project_dir = (
            project_dir
            or os.getenv(
                "DBT_PROJECT_DIR"
            )
            or DEFAULT_DBT_PROJECT_DIR
        )


        self.project_dir = (
            Path(
                configured_project_dir
            )
            .expanduser()
            .resolve()
        )


        # ====================================================
        # DBT EXECUTABLE
        # ====================================================

        configured_dbt_executable = (
            dbt_executable
            or os.getenv(
                "DBT_EXECUTABLE"
            )
            or DEFAULT_DBT_EXECUTABLE
        )


        self.dbt_executable = (
            Path(
                configured_dbt_executable
            )
            .expanduser()
            .resolve()
        )


        # ====================================================
        # SNOWFLAKE NAMED CONNECTION
        # ====================================================

        self.connection_name = (
            connection_name
            or os.getenv(
                "SNOWFLAKE_CONNECTION_NAME"
            )
            or "SNOWFLAKE_ACT_DEV"
        )


        configured_connection_file = (
            connection_file
            or os.getenv(
                "SNOWFLAKE_CONNECTION_FILE"
            )
            or DEFAULT_SNOWFLAKE_CONNECTION_FILE
        )


        self.connection_file = (
            Path(
                configured_connection_file
            )
            .expanduser()
            .resolve()
        )


        # ====================================================
        # VALIDATE
        # ====================================================

        self._validate_configuration()


    # ========================================================
    # CONFIGURATION VALIDATION
    # ========================================================

    def _validate_configuration(
        self,
    ) -> None:
        """
        Validate all local dbt configuration required
        before starting a subprocess.
        """

        if not self.project_dir.exists():

            raise RuntimeError(
                (
                    "dbt project directory does not exist: "
                    f"{self.project_dir}"
                )
            )


        dbt_project_file = (
            self.project_dir
            / "dbt_project.yml"
        )


        if not dbt_project_file.exists():

            raise RuntimeError(
                (
                    "dbt_project.yml was not found: "
                    f"{dbt_project_file}"
                )
            )


        profiles_file = (
            self.project_dir
            / "profiles.yml"
        )


        if not profiles_file.exists():

            raise RuntimeError(
                (
                    "dbt profiles.yml was not found: "
                    f"{profiles_file}"
                )
            )


        if not self.dbt_executable.exists():

            fallback_dbt = (
                shutil.which(
                    "dbt"
                )
            )


            if fallback_dbt:

                self.dbt_executable = (
                    Path(
                        fallback_dbt
                    )
                    .resolve()
                )


            else:

                raise RuntimeError(
                    (
                        "dbt executable was not found. "
                        "Expected: "
                        f"{self.dbt_executable}"
                    )
                )


        if not os.access(
            self.dbt_executable,
            os.X_OK,
        ):

            raise RuntimeError(
                (
                    "dbt executable is not executable: "
                    f"{self.dbt_executable}"
                )
            )


        if not self.connection_file.exists():

            raise RuntimeError(
                (
                    "Snowflake connection file does not exist: "
                    f"{self.connection_file}"
                )
            )


    # ========================================================
    # SNOWFLAKE ENVIRONMENT
    # ========================================================

    def _build_snowflake_environment(
        self,
    ) -> dict[str, str]:
        """
        Load the Snowflake named connection and expose
        only the environment variables required by dbt.

        Password is never logged.
        """

        with self.connection_file.open(
            "rb"
        ) as handle:

            config = (
                tomllib.load(
                    handle
                )
            )


        if (
            self.connection_name
            not in config
        ):

            raise RuntimeError(
                (
                    "Snowflake connection "
                    f"'{self.connection_name}' "
                    "was not found in "
                    f"{self.connection_file}"
                )
            )


        connection = (
            config[
                self.connection_name
            ]
        )


        # ====================================================
        # REQUIRED CONNECTION VALUES
        # ====================================================

        required_values = [
            "account",
            "user",
            "role",
            "warehouse",
            "database",
        ]


        missing_values = [

            name

            for name in required_values

            if not str(
                connection.get(
                    name,
                    "",
                )
            ).strip()
        ]


        if missing_values:

            raise RuntimeError(
                (
                    "Snowflake named connection is missing: "
                    + ", ".join(
                        missing_values
                    )
                )
            )


        # ====================================================
        # CURRENT DBT PROFILE USES PASSWORD AUTHENTICATION
        # ====================================================

        password = (
            connection.get(
                "password"
            )
        )


        if not password:

            raise RuntimeError(
                (
                    "Snowflake password is missing from "
                    f"connection '{self.connection_name}'. "
                    "The current ACT dbt profile requires "
                    "SNOWFLAKE_PASSWORD."
                )
            )


        environment = (
            os.environ.copy()
        )


        environment[
            "SNOWFLAKE_ACCOUNT"
        ] = str(
            connection[
                "account"
            ]
        )


        environment[
            "SNOWFLAKE_USER"
        ] = str(
            connection[
                "user"
            ]
        )


        environment[
            "SNOWFLAKE_PASSWORD"
        ] = str(
            password
        )


        environment[
            "SNOWFLAKE_ROLE"
        ] = str(
            connection[
                "role"
            ]
        )


        environment[
            "SNOWFLAKE_WAREHOUSE"
        ] = str(
            connection[
                "warehouse"
            ]
        )


        environment[
            "SNOWFLAKE_DATABASE"
        ] = str(
            connection.get(
                "database",
                "ACT_DB",
            )
        )


        environment[
            "DBT_TARGET"
        ] = os.getenv(
            "DBT_TARGET",
            "dev",
        )


        environment[
            "DBT_DEV_SCHEMA"
        ] = os.getenv(
            "DBT_DEV_SCHEMA",
            "DBT_DEV",
        )


        return environment


    # ========================================================
    # GENERIC DBT COMMAND
    # ========================================================

    def run(
        self,
        arguments: list[str],
    ) -> DbtRunResult:
        """
        Execute one dbt command.

        Example:

            runner.run(["build"])

        becomes:

            .venv/bin/dbt build
                --project-dir dbt_act
                --profiles-dir dbt_act
        """

        if not arguments:

            raise ValueError(
                "dbt arguments cannot be empty"
            )


        environment = (
            self._build_snowflake_environment()
        )


        command = [

            str(
                self.dbt_executable
            ),

            *arguments,

            "--project-dir",
            str(
                self.project_dir
            ),

            "--profiles-dir",
            str(
                self.project_dir
            ),
        ]


        safe_command = " ".join(

            shlex.quote(
                item
            )

            for item in command
        )


        logger.info(
            (
                "dbt_command_started "
                "connection=%s "
                "project_dir=%s "
                "command=%s"
            ),
            self.connection_name,
            self.project_dir,
            safe_command,
        )


        started_at = (
            time.monotonic()
        )


        # ====================================================
        # STREAM DBT OUTPUT DIRECTLY INTO AIRFLOW LOGS
        # ====================================================

        process = subprocess.Popen(

            command,

            cwd=str(
                self.project_dir
            ),

            env=environment,

            stdout=subprocess.PIPE,

            stderr=subprocess.STDOUT,

            text=True,

            bufsize=1,
        )


        if process.stdout is None:

            process.kill()

            raise RuntimeError(
                "Unable to capture dbt process output"
            )


        for line in process.stdout:

            logger.info(
                "dbt | %s",
                line.rstrip(),
            )


        return_code = (
            process.wait()
        )


        duration_seconds = round(
            (
                time.monotonic()
                - started_at
            ),
            2,
        )


        if return_code == 0:

            status = "SUCCESS"


        else:

            status = "FAILED"


        result = DbtRunResult(

            command=
                safe_command,

            status=
                status,

            return_code=
                return_code,

            duration_seconds=
                duration_seconds,

            project_dir=
                str(
                    self.project_dir
                ),

            dbt_executable=
                str(
                    self.dbt_executable
                ),
        )


        logger.info(
            (
                "dbt_command_completed "
                "status=%s "
                "return_code=%s "
                "duration_seconds=%s"
            ),
            result.status,
            result.return_code,
            result.duration_seconds,
        )


        if return_code != 0:

            raise RuntimeError(
                (
                    "dbt command failed "
                    f"return_code={return_code} "
                    f"command={safe_command}"
                )
            )


        return result


    # ========================================================
    # DBT BUILD
    # ========================================================

    def run_build(
        self,
    ) -> DbtRunResult:
        """
        Execute the complete ACT dbt project.

        Includes:

            models
            tests
            seeds
            unit tests
        """

        return self.run(
            [
                "build",
            ]
        )


# ============================================================
# LOCAL TEST
# ============================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,

        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
    )


    result = (
        DbtRunner()
        .run_build()
    )


    print(
        result.to_dict()
    )