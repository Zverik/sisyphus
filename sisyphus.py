#!/usr/bin/env python
import sys, os, re, requests, gzip, smtplib
import config
from StringIO import StringIO
from email.mime.text import MIMEText
from simple_revert.simple_revert import download_changesets, revert_changes
from simple_revert.common import API_ENDPOINT, changeset_xml, changes_to_osc

try:
    from lxml import etree
except ImportError:
    try:
        import xml.etree.cElementTree as etree
    except ImportError:
        import xml.etree.ElementTree as etree

REPLICATION_BASE_URL = 'http://planet.openstreetmap.org/replication/changesets'
STATE_FILE = os.path.join(os.path.dirname(__file__), 'state.txt')


def download_last_state():
    """Downloads last changeset replication sequence number from the planet website."""
    response = requests.get(REPLICATION_BASE_URL + '/state.yaml')
    if response.status_code != 200:
        raise IOError('Cannot read state.yaml')
    m = re.search(r'sequence:\s+(\d+)', response.text)
    # Not checking to throw exception in case of an error
    return int(m.group(1))


def download_replication(state):
    """Downloads replication archive for a given state, and returns a list of changeset data to process."""
    url = '{0}/{1:03}/{2:03}/{3:03}.osm.gz'.format(REPLICATION_BASE_URL, int(state / 1000000), int(state / 1000) % 1000, state % 1000)
    response = requests.get(url)
    if response.status_code != 200:
        raise IOError('Cannot download {0}: error {1}'.format(url, response.status_code))
    gz = gzip.GzipFile(fileobj=StringIO(response.content))
    changesets = []
    for event, element in etree.iterparse(gz, events=('end')):
        if element.tag == 'changeset':
            if int(element.get('uid')) in config.USERS:
                changesets.append(int(element.get('id')))
    return changesets


def revert(changeset):
    print(changeset)

    def print_status(changeset_id, obj_type=None, obj_id=None, count=None, total=None):
        if changeset_id == 'flush':
            pass
        elif changeset_id is not None:
            print('downloading')
        else:
            print('reverting')

    diffs, ch_users = download_changesets([changeset], print_status)

    if len(diffs) > config.MAX_DIFFS:
        raise ValueError('Would not revert {0} changes'.format(len(diffs)))

    changes = revert_changes(diffs, print_status)

    if not changes:
        print('Already reverted')
        return

    # TODO: no oauth?
    oauth = OAuth1(config.OAUTH_KEY, config.OAUTH_SECRET, task.token, task.secret)

    tags = {
        'created_by': config.CREATED_BY,
        'comment': 'Reverting {0}'.format(
            ', '.join(['{0} by {1}'.format(str(x), ch_users[x]) for x in changesets]))
    }

    resp = requests.put(API_ENDPOINT + '/api/0.6/changeset/create', data=changeset_xml(tags), auth=oauth)
    if resp.status_code == 200:
        changeset_id = resp.text
    else:
        raise IOError('Failed to create changeset: {0} {1}.'.format(resp.status_code, resp.reason))

    try:
        osc = changes_to_osc(changes, changeset_id)
        resp = requests.post('{0}/api/0.6/changeset/{1}/upload'.format(API_ENDPOINT, changeset_id), osc, auth=oauth)
        if resp.status_code == 200:
            print('Reverted: {0}'.format(changeset_id))
        else:
            raise IOError('Server rejected the changeset with code {0}: {1}'.format(resp.code, resp.text))
    finally:
        resp = requests.put('{0}/api/0.6/changeset/{1}/close'.format(API_ENDPOINT, changeset_id), auth=oauth)


def mail_error(changeset_id, error):
    # TODO
    msg = MIMEText('Error reverting changeset {0}:\n\n{1}\n\nSisyphus'.format(changeset_id, error))
    msg['Subject'] = 'Error reverting a changeset'
    msg['From'] = config.CREATED_BY
    msg['To'] = config.FAIL_MAIL

    s = smtplib.SMTP('localhost')
    s.sendmail(config.CREATED_BY, [config.FAIL_MAIL], msg.as_string())
    s.quit()


if __name__ == '__main__':
    try:
        cur_state = download_last_state()
    except Exception as e:
        print('Failed to download last state:' + e)
        sys.exit(1)

    try:
        with open(STATE_FILE, 'r') as f:
            state = int(f.next().strip())
    except:
        state = cur_state - config.DELAY

    for i in range(state, cur_state - config.DELAY):
        changesets = download_replication(i)
        for ch in changesets:
            try:
                revert(ch)
            except Exception as e:
                mail_error(ch, e)
        with open(STATE_FILE, 'w') as f:
            f.write(i)
