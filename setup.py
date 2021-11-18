from setuptools import setup

setup(
    name = 'pycivi',
    packages = ['pycivi'],
    install_requires = [
        'requests',
        'chardet',
        'charset_normalizer',
    ],
)