"""This file will pull DataSet Accounting Record from a REST endpoint."""

from ssm import __version__, set_up_logging, LOG_BREAK
from ssm.ssm2 import Ssm2, Ssm2Exception

import logging.config
import sys
import os
from optparse import OptionParser
import ConfigParser

def get_dois(doi_file):
    '''
    Retrieve a list of DOIs from a file.
    '''
    dois = []
    f = None
    try:
        f = open(doi_file, 'r')
        lines = f.readlines()
        for line in lines:
            dois.append(line.strip())
    finally:
        if f is not None:
            f.close()
    # If no valid DOIs, SSM cannot pull down any usage.
    if len(dois) == 0:
        raise Ssm2Exception('No valid DOIs found in %s.  SSM will not start' % doi_file)

    logging.debug('%s DOIs found.', len(dois))
    return dois


def main():
    """Set up connection, and pull down accounting information."""
    ver = "SSM %s.%s.%s" % __version__
    option_parser = OptionParser(description=__doc__, version=ver)
    option_parser.add_option('-c', '--config', help='location of config file',
                             default='/etc/apel/receiver.cfg')
    option_parser.add_option('-l', '--log_config',
                             help='location of logging config file (optional)',
                             default='/etc/apel/logging.cfg')
    option_parser.add_option('-d', '--doi_file',
                             help='location of the file containing DOIs '
                                  'to account for.',
                             default='/etc/apel/dois')

    (options, _unused_args) = option_parser.parse_args()

    config_parser = ConfigParser.ConfigParser()
    config_parser.read(options.config)

    # set up logging
    try:
        if os.path.exists(options.log_config):
            logging.config.fileConfig(options.log_config)
        else:
            set_up_logging(config_parser.get('logging', 'logfile'),
                           config_parser.get('logging', 'level'),
                           config_parser.getboolean('logging', 'console'))
    except (ConfigParser.Error, ValueError, IOError), err:
        print 'Error configuring logging: %s' % str(err)
        print 'SSM will exit.'
        sys.exit(1)

    logger = logging.getLogger('ssmpull')
    logger.info(LOG_BREAK)
    logger.info('Starting pulling SSM version %s.%s.%s.', *__version__)

    try:
        destination_type = config_parser.get('SSM Type', 'destination type')
        protocol = config_parser.get('SSM Type', 'protocol')

    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        logger.debug('No options supplied for destination_type '
                     'and/or protocol.')

    finally:
        if protocol is not "REST" and destination_type not in ["ONEDATA"]:
            logger.error('Dest_Type/Protocol combination not supported:')
            logger.error('Dest Type: %s, Protocol : %s',
                      destination_type, protocol)
            print 'SSM failed to start.  See log file for details.'
            sys.exit(1)

    logger.info('Setting up SSM with Dest Type: %s, Protocol : %s',
             destination_type, protocol)

    # Read the destination from the config file
    try:
        destination = config_parser.get('messaging', 'destination')

    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        logger.error('No destination is configured.')
        logger.error('SSM will exit.')
        print 'SSM failed to start.  See log file for details.'
        sys.exit(1)

    # Read the file path the SSM will save messages to
    try:
        path = config_parser.get('messaging', 'path')

    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        logger.error('No message queue is configured.')
        logger.error('SSM will exit.')
        print 'SSM failed to start.  See log file for details.'
        sys.exit(1)

    # Regardless of protocol, the SSM needs a certificate and a key
    # for HTTPS connections
    try:
        cert = config_parser.get('certificates', 'certificate')
        key = config_parser.get('certificates', 'key')

    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError), error:
        logger.error('No certificate or key set in conf file')
        logger.error(error)
        logger.error('SSM will exit')

        print 'SSM failed to start.  See log file for details.'

        sys.exit(1)

    try:
        user = config_parser.get('Auth', 'user')
        pwd = config_parser.get('Auth', 'pass')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError), error:
        logger.error('No Auth info provided.')
        logger.error(error)
        logger.error('SSM will exit')

        print 'SSM failed to start.  See log file for details.'

        sys.exit(1)

    puller = Ssm2(None,  # hosts_and_ports,
                  path,
                  cert=cert,
                  key=key,
                  dest=destination,
                  listen=True,
                  username=user,
                  password=pwd,
                  protocol=protocol,
                  dest_type=destination_type)

    dois = get_dois(options.doi_file)
    puller.set_dois(dois)

    try:
        puller.pull_msg_rest()
    except Ssm2Exception, e:
        print 'SSM failed to complete successfully. See log file for details.'
        logger.error('SSM failed to complete successfully: %s', e)
    except Exception, e:
        print 'SSM failed to complete successfully. See log file for details.'
        logger.error('Unexpected exception in SSM: %s', e)
        logger.error('Exception type: %s', e.__class__)

    logger.info('SSM has shut down.')
    logger.info(LOG_BREAK)

    print 'SSM has shut down.'
    print LOG_BREAK

if __name__ == '__main__':
    main()
