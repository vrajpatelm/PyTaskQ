from pathlib import Path

from setuptools import find_packages, setup

setup(
    name="pytaskq",
    version="0.2.0",
    description="Background jobs for Python — simple to start, safe in production.",
    long_description=Path(__file__).with_name("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="PyTaskQ Contributors",
    license="MIT",
    python_requires=">=3.10",
    packages=find_packages(include=["pytaskq", "pytaskq.*"]),
    include_package_data=True,
    package_data={"pytaskq": ["static/*", "static/assets/*"]},
    install_requires=[
        "fastapi>=0.100.0",
        "uvicorn>=0.23.0",
        "redis>=5.0.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "croniter>=2.0.0",
        "python-dotenv>=1.0.0",
        "opentelemetry-api",
        "opentelemetry-sdk",
    ],
    extras_require={
        "tracing": ["opentelemetry-exporter-otlp"],
        "demo": ["numpy>=1.26.0", "Pillow>=10.0.1"],
        "dev": ["pytest>=8.0.0", "pytest-asyncio>=0.23.0", "fakeredis[lua]>=2.20.0", "httpx>=0.26.0"],
    },
    entry_points={
        "console_scripts": [
            "pytaskq=pytaskq.cli:main",
        ],
    },
)
