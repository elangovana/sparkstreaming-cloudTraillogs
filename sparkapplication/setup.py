from setuptools import setup

setup(
    name='SparkStreaming',
    version='.1',
    packages=['sparkapplication', 'sparkapplication.tests'],
    url='',
    license='',
    author='Aparna Elangovan',
    author_email='',
    description='Demo Spark sql application integrated with kinesis',
    install_requires=['locustio', "pyspark", "boto"]
)
