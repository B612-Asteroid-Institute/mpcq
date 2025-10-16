"""
Configuration management for ADES observatory contexts.

This module handles loading, validating, and storing observatory configurations
that are used to generate ADES headers for MPC submissions.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from adam_core.observations.ades import (
    ObsContext,
    ObservatoryObsContext,
    SoftwareObsContext,
    SubmitterObsContext,
    TelescopeObsContext,
)


class ConfigError(Exception):
    """Raised when there's an error with configuration."""

    pass


class ObservatoryConfig:
    """
    Manages observatory configurations for ADES submissions.

    Observatory configurations can be stored in YAML files and/or in the
    database. This class handles loading, validation, and conversion between
    formats.
    """

    DEFAULT_CONFIG_PATHS = [
        "./mpcq_config.yaml",
        "~/.mpcq/config.yaml",
        "/etc/mpcq/config.yaml",
    ]

    @staticmethod
    def _validate_obscontext_dict(
        config: Dict[str, Any], mpc_code: str, require_submitter: bool = True
    ) -> None:
        """
        Validate that a configuration dictionary has all required fields.

        Parameters
        ----------
        config : Dict[str, Any]
            Configuration dictionary for an observatory.
        mpc_code : str
            The MPC observatory code.
        require_submitter : bool
            If True, require submitter field in config. Default True.

        Raises
        ------
        ConfigError
            If required fields are missing or invalid.
        """
        required_top_level = ["observatory", "telescope", "measurers"]
        if require_submitter:
            required_top_level.append("submitter")

        for field in required_top_level:
            if field not in config:
                raise ConfigError(
                    f"Missing required field '{field}' for observatory {mpc_code}"
                )

        # Validate observatory
        if "mpcCode" not in config["observatory"]:
            raise ConfigError(f"Missing 'mpcCode' in observatory config for {mpc_code}")

        # Validate submitter (if present)
        if require_submitter and "submitter" in config:
            if "name" not in config["submitter"]:
                raise ConfigError(f"Missing 'name' in submitter config for {mpc_code}")

        # Validate telescope
        required_telescope = ["design", "aperture", "detector"]
        for field in required_telescope:
            if field not in config["telescope"]:
                raise ConfigError(
                    f"Missing required telescope field '{field}' for observatory {mpc_code}"
                )

        # Validate measurers
        if not isinstance(config["measurers"], list) or len(config["measurers"]) == 0:
            raise ConfigError(
                f"'measurers' must be a non-empty list for observatory {mpc_code}"
            )

    @staticmethod
    def dict_to_obscontext(
        config: Dict[str, Any], submitter: Optional[SubmitterObsContext] = None
    ) -> ObsContext:
        """
        Convert a configuration dictionary to an ObsContext object.

        Parameters
        ----------
        config : Dict[str, Any]
            Configuration dictionary with observatory information.
        submitter : Optional[SubmitterObsContext]
            If provided, use this submitter instead of the one in config.
            Useful for populating submitter from database.

        Returns
        -------
        ObsContext
            The ObsContext object.

        Raises
        ------
        ConfigError
            If the configuration is invalid or no submitter is provided.
        """
        try:
            # Build nested dataclasses
            observatory = ObservatoryObsContext(**config["observatory"])

            # Use provided submitter or get from config
            if submitter is not None:
                submitter_ctx = submitter
            elif "submitter" in config and config["submitter"]:
                submitter_ctx = SubmitterObsContext(**config["submitter"])
            else:
                raise ConfigError(
                    "No submitter provided. Either include 'submitter' in config or "
                    "pass submitter parameter."
                )

            telescope = TelescopeObsContext(**config["telescope"])

            software = None
            if "software" in config and config["software"]:
                software = SoftwareObsContext(**config["software"])

            # Build ObsContext
            obscontext = ObsContext(
                observatory=observatory,
                submitter=submitter_ctx,
                measurers=config["measurers"],
                telescope=telescope,
                observers=config.get("observers"),
                software=software,
                coinvestigators=config.get("coinvestigators"),
                collaborators=config.get("collaborators"),
                fundingSource=config.get("fundingSource"),
                comments=config.get("comments"),
            )
            return obscontext
        except (KeyError, TypeError, AssertionError) as e:
            raise ConfigError(f"Invalid configuration: {e}") from e

    @staticmethod
    def obscontext_to_dict(
        obscontext: ObsContext, include_submitter: bool = False
    ) -> Dict[str, Any]:
        """
        Convert an ObsContext object to a configuration dictionary.

        Parameters
        ----------
        obscontext : ObsContext
            The ObsContext object.
        include_submitter : bool, optional
            Whether to include the submitter field in the output. Default is False.
            If False, the submitter will be omitted and should be populated from
            the database when loading.

        Returns
        -------
        Dict[str, Any]
            Configuration dictionary.
        """
        from dataclasses import asdict

        config_dict = asdict(obscontext)

        # Remove submitter if requested (default behavior)
        if not include_submitter:
            config_dict.pop("submitter", None)

        return config_dict

    @staticmethod
    def load_yaml(file_path: Union[str, Path]) -> Dict[str, Dict[str, Any]]:
        """
        Load observatory configurations from a YAML file.

        The YAML file should have the structure:
        ```yaml
        observatories:
          X05:
            observatory:
              mpcCode: "X05"
              name: "..."
            ...
          G96:
            ...
        ```

        Parameters
        ----------
        file_path : Union[str, Path]
            Path to the YAML configuration file.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            Dictionary mapping MPC codes to configuration dictionaries.

        Raises
        ------
        ConfigError
            If the file cannot be read or parsed.
        """
        file_path = Path(file_path).expanduser()
        if not file_path.exists():
            raise ConfigError(f"Configuration file not found: {file_path}")

        try:
            with open(file_path, "r") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Failed to parse YAML file: {e}") from e

        if not data or "observatories" not in data:
            raise ConfigError("Invalid configuration file: missing 'observatories' key")

        return data["observatories"]

    @staticmethod
    def save_yaml(
        configs: Dict[str, Dict[str, Any]], file_path: Union[str, Path]
    ) -> None:
        """
        Save observatory configurations to a YAML file.

        Parameters
        ----------
        configs : Dict[str, Dict[str, Any]]
            Dictionary mapping MPC codes to configuration dictionaries.
        file_path : Union[str, Path]
            Path where the YAML file should be saved.
        """
        file_path = Path(file_path).expanduser()
        file_path.parent.mkdir(parents=True, exist_ok=True)

        data = {"observatories": configs}

        with open(file_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def save_obscontexts(
        cls,
        obscontexts: Dict[str, ObsContext],
        file_path: Union[str, Path],
        include_submitter: bool = False,
    ) -> None:
        """
        Save ObsContext objects to a YAML configuration file.

        By default, the submitter field is NOT saved to the file. This ensures
        the submitter is always populated from the database (single source of truth).
        This makes configs reusable across different submitters/institutions.

        Parameters
        ----------
        obscontexts : Dict[str, ObsContext]
            Dictionary mapping configuration names to ObsContext objects.
            Keys can be anything (e.g., "X05", "X05_LSSTCAM", "X05_COMCAM").
        file_path : Union[str, Path]
            Path where the YAML file should be saved.
        include_submitter : bool, optional
            Whether to include submitter information in the saved file.
            Default is False (submitter will be populated from database).

        Examples
        --------
        >>> # Save without submitter (recommended - reusable config)
        >>> obscontexts = {
        ...     "X05_LSSTCAM": lsstcam_obscontext,
        ...     "X05_COMCAM": comcam_obscontext,
        ... }
        >>> ObservatoryConfig.save_obscontexts(obscontexts, "my_config.yaml")
        >>>
        >>> # Later, load with submitter from database
        >>> obscontexts = manager.load_obscontexts_with_submitter("my_config.yaml")
        """
        # Convert all ObsContexts to dictionaries
        config_dicts = {}
        for key, obscontext in obscontexts.items():
            config_dicts[key] = cls.obscontext_to_dict(obscontext, include_submitter)

        # Save using the existing save_yaml method
        cls.save_yaml(config_dicts, file_path)

    @classmethod
    def load_obscontexts(
        cls,
        file_path: Optional[Union[str, Path]] = None,
        submitter: Optional[SubmitterObsContext] = None,
    ) -> Dict[str, ObsContext]:
        """
        Load ObsContext objects from a configuration file.

        If no file path is provided, searches default locations.

        Parameters
        ----------
        file_path : Optional[Union[str, Path]]
            Path to configuration file. If None, searches default locations.
        submitter : Optional[SubmitterObsContext]
            If provided, use this submitter for all observatories instead of
            the submitter in the config file. Useful for populating from database.

        Returns
        -------
        Dict[str, ObsContext]
            Dictionary mapping MPC codes to ObsContext objects.

        Raises
        ------
        ConfigError
            If no configuration file is found or configurations are invalid.
        """
        if file_path is None:
            # Search default locations
            for default_path in cls.DEFAULT_CONFIG_PATHS:
                path = Path(default_path).expanduser()
                if path.exists():
                    file_path = path
                    break

            if file_path is None:
                raise ConfigError(
                    f"No configuration file found in default locations: {cls.DEFAULT_CONFIG_PATHS}"
                )

        # Load configurations from YAML
        config_dicts = cls.load_yaml(file_path)

        # Validate and convert to ObsContext objects
        obscontexts = {}
        for mpc_code, config in config_dicts.items():
            # Only require submitter in config if not provided as parameter
            cls._validate_obscontext_dict(
                config, mpc_code, require_submitter=(submitter is None)
            )
            obscontexts[mpc_code] = cls.dict_to_obscontext(config, submitter=submitter)

        return obscontexts

    @staticmethod
    def create_example_config() -> str:
        """
        Create an example configuration file content.

        Returns
        -------
        str
            Example YAML configuration content.
        """
        example = """# MPCQ Observatory Configuration
# 
# This file defines observatory contexts for ADES submissions to the MPC.
# Each observatory configuration is keyed by a unique identifier.
#
# IMPORTANT: The 'submitter' field should NOT be included in this file.
# It will be automatically populated from the database when you call:
#   manager.load_obscontexts_with_submitter("config.yaml")
#
# This ensures:
#   - Single source of truth (database)
#   - ADES attribution matches MPC API authentication
#   - Configs are reusable across different submitters/institutions

observatories:
  X05:
    # Observatory information [required]
    observatory:
      mpcCode: "X05"  # Placeholder - replace with actual MPC code
      name: "Vera C. Rubin Observatory"  # [optional]
    
    # Submitter is auto-populated from database - DO NOT include it here
    
    # Telescope specifications [required]
    telescope:
      name: "Simonyi Survey Telescope - LSSTCam"
      design: "Modified Paul-Baker"
      aperture: 8.4  # meters [optional]
      detector: "CCD"  # [optional]
      fRatio: 1.234  # [optional]
      filter: null  # [optional] Use null if not applicable
      arraySize: null  # [optional]
      pixelScale: 0.2  # arcsec/pixel [optional]
    
    # Software used for data reduction [optional]
    software:
      astrometry: "LSST Science Pipelines"
      fitOrder: null  # [optional]
      photometry: "LSST Science Pipelines"
      objectDetection: "heliolinx"
    
    # Required: People who made the measurements
    measurers:
      - "P. H. Bernardinelli"
      - "A. Heinze"
      - "M. Juric"
      - "J. Kurlander"
      - "J. Moeyens"
      - "E. Nourbakhsh"
    
    # Optional: Observers
    observers: null  # Use null if not applicable
    
    # Optional: Additional people and funding
    coinvestigators: null
    collaborators: null
    fundingSource: "National Science Foundation; Department of Energy"
    
    # Optional: Comments (can be multiple lines)
    comments:
      - "ADES generated and submitted using B612 Asteroid Institute ADAM mpcq"
  
  # Example: Second observatory (e.g., ComCam)
  # X05_COMCAM:
  #   observatory:
  #     mpcCode: "X05"
  #     name: "Vera C. Rubin Observatory"
  #   
  #   telescope:
  #     name: "Simonyi Survey Telescope - ComCam"
  #     design: "Modified Paul-Baker"
  #     aperture: 8.4
  #     detector: "CCD"
  #     fRatio: 1.234
  #     filter: null
  #     arraySize: null
  #     pixelScale: 0.2
  #   
  #   software:
  #     astrometry: "LSST Science Pipelines"
  #     photometry: "LSST Science Pipelines"
  #     objectDetection: "heliolinx"
  #   
  #   measurers:
  #     - "P. H. Bernardinelli"
  #     - "A. Heinze"
  #     - "M. Juric"
  #     - "J. Kurlander"
  #     - "J. Moeyens"
  #     - "E. Nourbakhsh"
  #   
  #   fundingSource: "National Science Foundation; Department of Energy"
  #   comments:
  #     - "ADES generated and submitted using B612 Asteroid Institute ADAM mpcq"
"""
        return example
