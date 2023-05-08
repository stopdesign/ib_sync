from setuptools import setup

setup(
    name="ib_sync",
    version="0.2.5",
    url="https://github.com/stopdesign/ib_sync.git",
    author="IBKR and Gregory",
    description="IBKR API Client and some tools",
    packages=["ib_sync", "ibapi", "mcal", "mcal.rules"],
    package_dir={"": "src"},
    install_requires=["pandas_market_calendars ~= 4.1, < 5"],
)
