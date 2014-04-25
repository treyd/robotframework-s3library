from keywords import Keywords

__version__ = '0.01'

class S3Library(Keywords):
    """
    CloudScalerLibrary provides Robot Framework keywords for interacting with 
    a Caringo CAStor Object Store via a CloudScaler Gateway
    """
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    ROBOT_LIBRARY_VERSION = __version__