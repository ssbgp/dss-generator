from setuptools import setup, find_packages

setup(
    name='ssbgp-dss-generator',
    version='0.2',
    description='Tool to generate simulation for the SS-BGP distributed simulation system',
    url='https://github.com/ssbgp/dss-generator',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    package_data={
        'dss_generator': [
            'tables.sql',
        ],
    },

    entry_points={
        'console_scripts': [
            'dss-generator=dss_generator.main:main',
        ],
    }
)
