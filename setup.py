from setuptools import setup

setup(
    name="ib_sync",
    version="0.2.1",
    url="https://github.com/stopdesign/ib_sync.git",
    author="IBKR and Gregory",
    description="IBKR API Client",
    packages=["ib_sync", "ibapi"],
    package_dir={"": "src"},
    install_requires=[],
)
