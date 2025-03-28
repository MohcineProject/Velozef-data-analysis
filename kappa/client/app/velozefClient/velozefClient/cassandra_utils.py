from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from django.conf import settings

def get_cassandra_connection():
    """Function to establish a Cassandra connection"""
    cluster = Cluster(
        contact_points=[settings.CASSANDRA_HOST], 
        port=settings.CASSANDRA_PORT
    )
    session = cluster.connect(settings.CASSANDRA_KEYSPACE)
    return session
