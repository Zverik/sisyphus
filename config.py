# OSM User IDs
USERS = [1234]

# Where to send a mail in case processing a changeset fails
FAIL_MAIL = 'ilya@zverev.info'

# Absolute path to sendmail binary
SENDMAIL = '/usr/sbin/sendmail'

# Delay in processing the changeset stream
DELAY_MINUTES = 30

# String for created_by changeset tag
CREATED_BY = 'Sisyphus 1.0'

# Filter out changesets based on a number of changes
MAX_DIFFS = 50

# Your OpenStreetMap credentials. DO NOT UPLOAD anywhere!
OSM_USERNAME = ''
OSM_PASSWORD = ''

try:
    from config_local import *
except ImportError:
    pass
