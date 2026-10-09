from setuptools import setup, find_packages

setup(
    name="pytaskq",
    version="0.1.0",
    description="A highly scalable, asynchronous distributed task queue for Python.",
    author="PyTaskQ Contributors",
    packages=find_packages(include=['pytaskq', 'pytaskq.*']),
    install_requires=[
        "fastapi",
        "redis",
        "pydantic",
        "croniter",
        "opentelemetry-api",
        "opentelemetry-sdk",
    ],
    entry_points={
        "console_scripts": [
            "pytaskq=pytaskq.cli:main",
        ],
    },
)
