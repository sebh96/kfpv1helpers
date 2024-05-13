from setuptools import setup, find_packages

setup(
    name='kfpv1helper',
    version='0.1',
    description='Kubeflow Pipelines v1 helper functions for easy deployment and running pipelines.',
    author='Sebastian Hocke',
    author_email='sebastian.hocke@dfki.de',
    packages=find_packages(),
    install_requires=[
        'kfp==1.8.22',
        'requests',
        'urllib3',
        'kubernetes',
        'requests_oauthlib',
        # other dependencies
    ],
)