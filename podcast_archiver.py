#!/usr/bin/env python3
"""
Podcast Archiver v0.3: Feed parser for local podcast archive creation

Copyright (c) 2014-2017 Jan Willhaus

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
import datetime
import http
import logging
import platform
import sys
import argparse
from argparse import ArgumentTypeError
from pathlib import Path

import feedparser
from feedparser import CharacterEncodingOverride
from urllib.request import urlopen, Request
import urllib.error
from shutil import copyfileobj
from os import path, remove, makedirs, access, W_OK
import os
from urllib.parse import urlparse
import unicodedata
import re
import xml.etree.ElementTree as etree

logger = logging.getLogger(__name__)


class writeable_dir(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        if not path.isdir(prospective_dir):
            raise ArgumentTypeError("%s is not a valid path" % prospective_dir)
        if access(prospective_dir, W_OK):
            setattr(namespace, self.dest, prospective_dir)
        else:
            raise ArgumentTypeError("%s is not a writeable dir" % prospective_dir)


def getMaxFilenameLength():
    if platform.system() == "Linux":
        return 255
    if platform.system() == "Windows":
        return 255
    if platform.system() == "Darwin":
        return 255


def shortenFilename(filename):
    filename, file_extension = os.path.splitext(filename)
    return filename[:(getMaxFilenameLength()-len(file_extension)-1)] + "…" + file_extension


class PodcastArchiver:
    _feed_title = ''
    _feedobj = None
    _feed_info_dict = {}

    _userAgent = 'Podcast-Archiver/0.4 (https://github.com/janwh/podcast-archiver)'
    _headers = {'User-Agent': _userAgent}
    _global_info_keys = ['author', 'language', 'link', 'subtitle', 'title', ]
    _episode_info_keys = ['author', 'link', 'subtitle', 'title', ]
    _date_keys = ['published', ]

    savedir = ''
    verbose = 0
    subdirs = False
    update = False
    progress = False
    maximumEpisodes = None

    feedlist = []

    skippedDownloads = 0
    successfulDownloads = 0
    failedDownloads = 0

    def __init__(self):



        feedparser.USER_AGENT = self._userAgent

        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(module)s.%(name)s:%(lineno)d - %(levelname)-8s - %(message)s')

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        logfilename = "podcast_archiver_" + datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".log"
        logfilename_with_path = os.path.join(os.path.dirname(__file__), logfilename)

        fh = logging.FileHandler(logfilename_with_path,
                                 encoding='UTF-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    def addArguments(self, args):

        # if type(args) is argparse.ArgumentParser:
        #     args = parser.parse_args()

        self.verbose = args.verbose or 0
        if self.verbose > 2:
            logger.info('Input arguments: %s' % args)

        for feed in (args.feed or []):
            self.addFeed(feed)

        for opml in (args.opml or []):
            self.parseOpmlFile(opml)

        if args.dir:
            self.savedir = args.dir

        self.subdirs = args.subdirs
        self.update = args.update
        self.progress = args.progress
        self.slugify = args.slugify
        self.dryrun = args.dry_run
        self.maximumEpisodes = args.max_episodes or None
        self.retitle = args.re_title
        self.overwrite_on_size_mismatch = args.overwrite_on_size_mismatch
        self.delete_illegal_characters = args.delete_illegal_characters

        if self.verbose > 1:
            logger.info("Verbose level: %d", self.verbose)

    def addFeed(self, feed):
        if path.isfile(feed):
            self.feedlist += open(feed, 'r').read().strip().splitlines()
        else:
            self.feedlist.append(feed)

    def parseOpmlFile(self, opml):
        with opml as file:
            tree = etree.fromstringlist(file)

        for feed in [node.get('xmlUrl') for node
                     in tree.findall("*/outline/[@type='rss']")
                     if node.get('xmlUrl') is not None]:
            self.addFeed(feed)

    def processFeeds(self):

        if self.verbose > 0 and self.update:
            logger.info("Updating archive")

        for feed in self.feedlist:
            if self.verbose > 0:
                logger.info("Downloading archive for: " + feed)
            linklist = self.processPodcastLink(feed)
            self.downloadPodcastFiles(linklist)

        if self.verbose > 0:
            logger.info("Done.")
            logger.info("Downloads skipped: %d, successful: %d, failed: %d",
                        self.skippedDownloads,
                        self.successfulDownloads,
                        self.failedDownloads)

    def parseGlobalFeedInfo(self, feedobj=None):
        if feedobj is None:
            feedobj = self._feedobj

        self._feed_info_dict = {}
        if 'feed' in feedobj:
            for key in self._global_info_keys:
                self._feed_info_dict['feed_' + key] = feedobj['feed'].get(key, None)

        return self._feed_info_dict

    def slugifyString(filename):
        filename = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore')
        filename = re.sub('[^\w\s\-\.]', '', filename.decode('ascii')).strip()
        filename = re.sub('[-\s]+', '-', filename)

        return filename

    def linkToTargetFilename(self, link, title=None):

        # Remove HTTP GET parameters from filename by parsing URL properly
        linkpath = urlparse(link).path
        basename = path.basename(linkpath)
        filename, fileextension = path.splitext(basename)

        if self.retitle and title is not None:
            basename = title + fileextension

        # If requested, slugify the filename
        if self.slugify:
            basename = PodcastArchiver.slugifyString(basename)
            self._feed_title = PodcastArchiver.slugifyString(self._feed_title)
        else:
            basename.replace(path.pathsep, '_')
            basename.replace(path.sep, '_')
            self._feed_title.replace(path.pathsep, '_')
            self._feed_title.replace(path.sep, '_')
            if platform.system() == "Windows":
                basename = self.replaceCharactersOnWindows(basename)

        # Generate local path and check for existence
        if self.subdirs:
            filename = path.join(self.savedir, self._feed_title, basename)
        else:
            filename = path.join(self.savedir, basename)

        return filename

    def replaceCharactersOnWindows(self, basename):
        if self.delete_illegal_characters:
            basename = basename.replace(":", "")
            basename = basename.replace("|", "")
            basename = basename.replace("?", "")
            basename = basename.replace("/", "")
            basename = basename.replace("\\", "")
            basename = basename.replace('"', "")
            basename = basename.replace('*', "")
            basename = basename.replace('>', "")
            basename = basename.replace('<', "")
            self._feed_title = self._feed_title.replace(":", "")
            self._feed_title = self._feed_title.replace("|", "")
            self._feed_title = self._feed_title.replace("?", "")
            self._feed_title = self._feed_title.replace("/", "")
            self._feed_title = self._feed_title.replace("\\", "")
            self._feed_title = self._feed_title.replace('"', "")
            self._feed_title = self._feed_title.replace('*', "")
            self._feed_title = self._feed_title.replace('>', "")
            self._feed_title = self._feed_title.replace('<', "")
        else:
            basename = basename.replace(":", " -")
            basename = basename.replace("|", "-")
            basename = basename.replace("?", "")
            basename = basename.replace("/", "-")
            basename = basename.replace("\\", "-")
            basename = basename.replace('"', "'")
            basename = basename.replace('*', "-")
            basename = basename.replace('>', "-")
            basename = basename.replace('<', "-")
            self._feed_title = self._feed_title.replace(":", " -")
            self._feed_title = self._feed_title.replace("|", "-")
            self._feed_title = self._feed_title.replace("?", "")
            self._feed_title = self._feed_title.replace("/", "-")
            self._feed_title = self._feed_title.replace("\\", "-")
            self._feed_title = self._feed_title.replace('"', "'")
            self._feed_title = self._feed_title.replace('*', "-")
            self._feed_title = self._feed_title.replace('>', "-")
            self._feed_title = self._feed_title.replace('<', "-")
        return basename

    def parseFeedToNextPage(self, feedobj=None):

        if feedobj is None:
            feedobj = self._feedobj

        # Assuming there will only be one link declared as 'next'
        self._feed_next_page = [link['href'] for link in feedobj['feed']['links']
                                if link['rel'] == 'next']

        if len(self._feed_next_page) > 0:
            self._feed_next_page = self._feed_next_page[0]
        else:
            self._feed_next_page = None

        return self._feed_next_page

    def parseFeedToLinks(self, feed=None):

        if feed is None:
            feed = self._feedobj

        # Try different feed episode layouts: 'items' or 'entries'
        episodeList = feed.get('items', False) or feed.get('entries', False)
        if episodeList:
            linklist = [self.parseEpisode(episode) for episode in episodeList]
            linklist = [link for link in linklist if len(link) > 0]
        else:
            linklist = []

        return linklist

    def parseEpisode(self, episode):
        url = None
        episode_info = {}
        for link in episode['links']:
            if 'type' in link.keys():
                if link['type'].startswith('audio'):
                    url = link['href']
                elif link['type'].startswith('video'):
                    url = link['href']

                if url is not None:
                    for key in self._episode_info_keys + self._date_keys:
                        episode_info[key] = episode.get(key, None)
                    episode_info['url'] = url

        return episode_info

    def processPodcastLink(self, link):
        if self.verbose > 0:
            logger.info("1. Gathering link list ...")

        self._feed_title = None
        self._feed_next_page = link
        first_page = True
        linklist = []
        page = 2
        while self._feed_next_page is not None:
            if self.verbose > 0:
                logger.info("Loading page #%d", page)
                page += 1

            self._feedobj = feedparser.parse(self._feed_next_page)

            # Escape improper feed-URL
            if 'status' in self._feedobj.keys() and self._feedobj['status'] >= 400:
                logger.error("Query returned HTTP error" + self._feedobj['status'])
                return None

            # Escape malformatted XML
            if self._feedobj['bozo'] == 1:

                # If the character encoding is wrong, we continue as long as the reparsing succeeded
                if type(self._feedobj['bozo_exception']) is not CharacterEncodingOverride:
                    logger.error('Downloaded feed is malformatted on' + self._feed_next_page)
                    return None

            if first_page:
                self.parseGlobalFeedInfo()
                first_page = False

            # Parse the feed object for episodes and the next page
            linklist += self.parseFeedToLinks(self._feedobj)
            self._feed_next_page = self.parseFeedToNextPage(self._feedobj)

            if self._feed_title is None:
                self._feed_title = self._feedobj['feed']['title']

            numberOfLinks = len(linklist)

            # On given option, run an update, break at first existing episode
            if self.update:
                for index, episode_dict in enumerate(linklist):
                    link = episode_dict['url']
                    filename = self.linkToTargetFilename(link, episode_dict['title'])

                    if path.isfile(filename):
                        del (linklist[index:])
                        break
                numberOfLinks = len(linklist)

            # On given option, crop linklist to maximum number of episodes
            if self.maximumEpisodes is not None and self.maximumEpisodes < numberOfLinks:
                linklist = linklist[0:self.maximumEpisodes]
                numberOfLinks = self.maximumEpisodes

            if self.maximumEpisodes is not None or self.update:
                break

        linklist.reverse()

        if self.verbose > 0:
            logger.info("Found %d episodes" % numberOfLinks)

        if self.verbose > 2:
            import json
            logger.info('Feed info: %s' % json.dumps(self._feed_info_dict, ensure_ascii=False))

        return linklist

    def downloadPodcastFiles(self, linklist):
        if linklist is None or self._feed_title is None:
            return

        nlinks = len(linklist)
        if nlinks > 0:
            if self.verbose > 0:
                logger.info("2. Downloading content ... ")

        for cnt, episode_dict in enumerate(linklist):
            link = episode_dict['url']
            if self.verbose > 0:
                logger.info("Downloading file no. %d/%d: %s", cnt + 1, nlinks, link)

                if self.verbose > 2:
                    import json
                    logger.info('Episode info:')
                    for key in episode_dict.keys():
                        logger.info(" * %10s: %s" % (key, episode_dict[key]))
            # Check existence once ...
            filename = self.linkToTargetFilename(link, episode_dict['title'])
            filename = self.shortenOnDemand(filename)

            if self.verbose > 1:
                logger.info("Local filename: %s", filename)

            if path.isfile(filename):
                if self.verbose > 1:
                    logger.info("✓ Already exists.")
                    self.skippedDownloads += 1
                continue

            # Begin downloading
            prepared_request = Request(urllib.parse.quote(link, safe="%/:=&?~#+!$,;'@()*[]"), headers=self._headers)
            try:
                with urlopen(prepared_request) as response:

                    # Check existence another time, with resolved link
                    link = response.geturl()
                    total_size = int(response.getheader('content-length', '0'))
                    old_filename = filename
                    filename = self.linkToTargetFilename(link, episode_dict['title'])
                    filename = self.shortenOnDemand(filename)

                    if old_filename != filename:
                        if self.verbose > 1:
                            logger.info("Resolved filename: %s", filename)

                        if path.isfile(filename):
                            if total_size == path.getsize(filename):
                                if self.verbose > 1:
                                    logger.info("✓ Already exists.")
                                    self.skippedDownloads += 1
                                continue
                            else:
                                if self.overwrite_on_size_mismatch:
                                    remove(filename)
                                    if self.verbose > 1:
                                        logger.info("File deleted.")
                                else:
                                    continue

                    # Create the subdir, if it does not exist
                    makedirs(path.dirname(filename), exist_ok=True)

                    if self.progress and total_size > 0:
                        from tqdm import tqdm
                        with tqdm(total=total_size, unit='B',
                                  unit_scale=True, unit_divisor=1024) as progress_bar:
                            if not self.dryrun:
                                with open(filename, 'wb') as outfile:
                                    self.prettyCopyfileobj(response, outfile,
                                                           callback=progress_bar.update)
                            else:
                                Path(filename).touch()
                    else:
                        if not self.dryrun:
                            with open(filename, 'wb') as outfile:
                                copyfileobj(response, outfile)
                        else:
                            Path(filename).touch()

                if self.verbose > 1:
                    logger.info("✓ Download successful.")
                    self.successfulDownloads += 1

            except http.client.HTTPException as error:
                logger.error("✗ Download failed. Query returned '%s'" % error)
                self.failedDownloads += 1
            except (urllib.error.HTTPError,
                    urllib.error.URLError) as error:
                if self.verbose > 1:
                    logger.error("✗ Download failed. Query returned '%s'" % error)
                    self.failedDownloads += 1
            except KeyboardInterrupt:
                if self.verbose > 0:
                    logger.error("✗ Unexpected interruption. Deleting unfinished file.")
                    self.failedDownloads += 1

                remove(filename)
                raise

    def shortenOnDemand(self, filename):
        if len(filename) > getMaxFilenameLength():
            filename = shortenFilename(filename)
            if self.verbose > 1:
                logger.info("Filename has been shortened")
        return filename

    def prettyCopyfileobj(self, fsrc, fdst, callback, block_size=8 * 1024):
        while True:
            buf = fsrc.read(block_size)
            if not buf:
                break
            fdst.write(buf)
            callback(len(buf))


if __name__ == "__main__":
    try:

        parser = argparse.ArgumentParser()
        parser.add_argument('-o', '--opml', action='append', type=argparse.FileType('r'),
                            help='''Provide an OPML file (as exported by many other podcatchers)
                                 containing your feeds. The parameter can be used multiple
                                 times, once for every OPML file.''')
        parser.add_argument('-f', '--feed', action='append',
                            help='''Add a feed URl to the archiver. The parameter can be used
                                 multiple times, once for every feed.''')
        parser.add_argument('-d', '--dir', action=writeable_dir,
                            help='''Set the output directory of the podcast archive.''')
        parser.add_argument('-s', '--subdirs', action='store_true',
                            help='''Place downloaded podcasts in separate subdirectories per
                                 podcast (named with their title).''')
        parser.add_argument('-u', '--update', action='store_true',
                            help='''Force the archiver to only update the feeds with newly added
                                 episodes. As soon as the first old episode found in the
                                 download directory, further downloading is interrupted.''')
        parser.add_argument('-v', '--verbose', action='count',
                            help='''Increase the level of verbosity while downloading.''')
        parser.add_argument('-p', '--progress', action='store_true',
                            help='''Show progress bars while downloading episodes.''')
        parser.add_argument('-S', '--slugify', action='store_true',
                            help='''Clean all folders and filename of potentially weird
                                 characters that might cause trouble with one or another
                                 target filesystem.''')
        parser.add_argument('-m', '--max-episodes', type=int,
                            help='''Only download the given number of episodes per podcast
                                 feed. Useful if you don't really need the entire backlog.''')
        parser.add_argument('-D', '--dry-run', action='store_true',
                            help='''If this is set, the archiver will only create empty files that have
                                the same name as the one that would be downloaded. This allows for
                                testing a configuration without actually downloading large amounts
                                of data.''')
        parser.add_argument('-r', '--re-title', action='store_true',
                            help='''Will cause the title of the episode to be used as the name of the
                            downloaded file instead of the name of the file as it is named on the server.''')
        parser.add_argument('-O', '--overwrite-on-size-mismatch', action='store_true',
                            help='''Will overwrite existing files if the size on the server differs from
                            the size of the local file. This is helpful if an episode has been downloaded
                            incompletely or has been re-published because of errors.''')
        parser.add_argument('-l', '--delete-illegal-characters', action='store_true',
                            help='''Will cause characters that are not allowed in file names to be deleted.
                            Otherwise the characters will be replaced with characters that make sense in the
                            context. The default behavior will result in prettier file names, but deletion
                            might improve compatibility with naming schemes of other podcast clients.''')

        args = parser.parse_args()

        pa = PodcastArchiver()
        pa.addArguments(args)
        pa.processFeeds()
    except KeyboardInterrupt:
        logger.error("Interrupted by user")
        sys.exit('\nERROR: Interrupted by user')
    except FileNotFoundError as error:
        logger.error("%s", error)
        sys.exit('\nERROR: %s' % error)
    except ArgumentTypeError as error:
        logger.error("Your config is invalid: %s", error)
        sys.exit('\nERROR: Your config is invalid: %s' % error)
