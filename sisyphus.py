#!/usr/bin/env python
import sys, os, re, requests, gzip, subprocess
import config
from requests.auth import HTTPBasicAuth
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
    url = '{0}/{1:03}/{2:03}/{3:03}.osm.gz'.format(
        REPLICATION_BASE_URL, int(state / 1000000), int(state / 1000) % 1000, state % 1000)
    response = requests.get(url)
    if response.status_code != 200:
        raise IOError('Cannot download {0}: error {1}'.format(url, response.status_code))
    gz = gzip.GzipFile(fileobj=StringIO(response.content))
    changesets = []
    for event, element in etree.iterparse(gz, events=('end',)):
        if element.tag == 'changeset':
            if int(element.get('uid')) in config.USERS:
                changesets.append(int(element.get('id')))
            element.clear()
    return changesets


def revert(changesets):
    def print_status(changeset_id, obj_type=None, obj_id=None, count=None, total=None):
        pass

    diffs, ch_users = download_changesets(changesets, print_status)

    if len(diffs) > config.MAX_DIFFS:
        raise ValueError('Would not revert {0} changes'.format(len(diffs)))

    changes = revert_changes(diffs, print_status)

    if not changes:
        return

    # Yup, just a plain basic auth. OAuth is too hard to initialize.
    oauth = HTTPBasicAuth(config.OSM_USERNAME, config.OSM_PASSWORD)

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
            print('Reverted in {0}'.format(changeset_id))
        else:
            raise IOError('Server rejected the changeset with code {0}: {1}'.format(resp.code, resp.text))
    finally:
        resp = requests.put('{0}/api/0.6/changeset/{1}/close'.format(API_ENDPOINT, changeset_id), auth=oauth)


def mail_error(changeset_id, error):
    body = 'Error reverting changeset {0}:\n{1}\n'.format(changeset_id, error)
    sys.stderr.write(body)
    if not config.FAIL_MAIL:
        return

    msg = MIMEText('{0}\nSisyphus'.format(body), _charset='utf-8')
    msg['Subject'] = 'Error reverting a changeset'
    sender = 'sisyphus@example.com'
    msg['From'] = sender
    msg['To'] = config.FAIL_MAIL

    try:
        p = subprocess.Popen([config.SENDMAIL, '-t', '-oi'], stdin=subprocess.PIPE)
        p.communicate(msg.as_string())
        if p.returncode != 0:
            raise IOError('sendmail returned code {0}'.format(p.returncode))
    except Exception as e:
        sys.stderr.write('Error sending email: {0}\n'.format(e))


if __name__ == '__main__':
    try:
        cur_state = download_last_state()
    except Exception as e:
        print('Failed to download last state: {0}'.format(e))
        sys.exit(1)

    try:
        with open(STATE_FILE, 'r') as f:
            state = int(f.next().strip())
    except:
        state = cur_state - config.DELAY_MINUTES - 1

    for i in range(state, cur_state - config.DELAY_MINUTES):
        changesets = download_replication(i)
        for ch in changesets:
            try:
                print(ch)
                revert([ch])
            except Exception as e:
                mail_error(ch, e)
        with open(STATE_FILE, 'w') as f:
            f.write(str(i))
