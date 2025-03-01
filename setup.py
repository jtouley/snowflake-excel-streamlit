"""Setup script for Excel to Bronze package."""
from setuptools import find_packages, setup

setup(
    name="excel_to_bronze",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "streamlit>=1.20.0",
        "pandas>=1.5.0",
        "snowflake-connector-python>=3.0.0",
        "pyarrow>=12.0.0",
        "PyYAML>=6.0",
        "openpyxl>=3.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "ruff>=0.0.262",
            "mypy>=1.0.0",
            "python-dotenv>=1.0.0",
            "pre-commit>=3.3.1",
            "bandit>=1.7.5",
        ],
        "docs": [
            "sphinx>=6.0.0",
        ],
    },
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "excel-to-bronze=excel_to_bronze.__main__:main",
        ],
    },
    author="Jason Touleyrou",
    author_email="jason.touleyrou@jtouley.com",
    description="Excel to Snowflake Bronze Layer Ingestion Tool",
    keywords="excel, snowflake, bronze, iceberg, data",
    url="https://github.com/yourusername/snowflake-excel-streamlit",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
