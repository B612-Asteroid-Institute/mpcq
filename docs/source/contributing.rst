Contributing
============

We welcome contributions to ``mpcq``! This document provides guidelines and instructions for contributing to the project.

Development Setup
--------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

       git clone https://github.com/your-username/mpcq.git
       cd mpcq

3. Install PDM if you haven't already:

   .. code-block:: bash

       pip install pdm

4. Install development dependencies:

   .. code-block:: bash

       pdm install -G dev


Available Commands
---------------

PDM provides several useful commands for development:

**Code Quality**:

.. code-block:: bash

    # Run all checks (lint, typecheck, test)
    pdm run check

    # Format code
    pdm run format  # Runs black and isort

    # Auto-fixes some issues using ruff
    pdm run fix     # Auto-fixes ruff issues


**Testing**:

.. code-block:: bash

    # Run tests
    pdm run test           # Run all tests (excluding benchmarks)
    pdm run doctest        # Run doctests only
    pdm run benchmark      # Run benchmarks only
    pdm run coverage       # Run tests with coverage report

**Documentation**:

.. code-block:: bash

    # Build documentation
    pdm run docs-build     # Build HTML documentation
    pdm run docs-clean     # Clean documentation build
    pdm run docs-serve     # Serve documentation locally
    pdm run docs-live      # Live-reload documentation during editing

Development Workflow
-----------------

1. Create a new branch for your feature:

   .. code-block:: bash

       git checkout -b feature-name

2. Make your changes, following our coding standards
3. Run the test suite and linters:

   .. code-block:: bash

       pdm run check  # Runs all checks

4. Commit your changes:

   .. code-block:: bash

       git add .
       git commit -m "Description of changes"

5. Push to your fork:

   .. code-block:: bash

       git push origin feature-name

6. Open a Pull Request on GitHub

Coding Standards
-------------

- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Write docstrings for all public functions and classes
- Keep functions focused and single-purpose
- Write clear, descriptive variable and function names

Testing
------

- Write tests for all new functionality
- Maintain or improve test coverage
- Use pytest for testing
- Place tests in the ``tests/`` directory
- Name test files with ``test_`` prefix

Documentation
-----------

- Update documentation for any modified functionality
- Write clear docstrings with examples
- Follow Google style for docstrings
- Include doctest examples where appropriate
- Update the changelog

Example docstring format:

.. code-block:: python

    def function_name(param1: type1, param2: type2) -> return_type:
        """Short description of function.

        Longer description of function if needed.

        Args:
            param1: Description of param1
            param2: Description of param2

        Returns:
            Description of return value

        Raises:
            ErrorType: Description of when this error is raised

        Examples:
            >>> function_name(1, 2)
            3
        """

Pull Request Process
-----------------

1. Update the changelog under "Unreleased"
2. Ensure all tests pass
3. Update documentation as needed
4. Request review from maintainers
5. Address review feedback
6. Maintainers will merge after approval

Questions and Support
------------------

- Open an issue on GitHub for bugs or feature requests
- Join our community discussions
- Contact maintainers for sensitive issues

License
-------

By contributing, you agree that your contributions will be licensed under the MIT License. 