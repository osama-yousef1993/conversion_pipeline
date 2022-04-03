import setuptools

REQUIRED_PACKAGES = ["apache-beam[gcp]==2.37.0",
                     "google-cloud-firestore==2.4.0",
                     "pandas==1.4.1"]
PACKAGE_NAME = 'my_package'
PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='conversions pipelines package',
    install_requires=REQUIRED_PACKAGES,
    author="osama",
    author_email='test@test.com'

)