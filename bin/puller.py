from ssm import __version__, set_up_logging, LOG_BREAK
from ssm.ssm2 import Ssm2, Ssm2Exception
from ssm.crypto import CryptoException
from ssm.brokers import StompBrokerGetter, STOMP_SERVICE, STOMP_SSL_SERVICE

import logging.config
import ldap
import sys
import os
from optparse import OptionParser
import ConfigParser

import mock

def main():
    qpath = "/tmp/apel/spool/"
    source = "https://datahub.plgrid.pl/api/v3/oneprovider/metrics/space/1I8DOQUXXiezOAcTpAewz40HHVNzy-Sr2mlBZZtEmpA?metric=storage_quota&step=1m"

    puller = Ssm2(None, # hosts_and_ports,
                  qpath,
                  "/etc/httpd/ssl/apache.crt", # cert
                  "/etc/httpd/ssl/apache.key", # key
                  dest=source, 
                  listen=True,
                  capath=None,
                  check_crls=False,
                  use_ssl=False,
                  username="",
                  password="",
                  enc_cert=None,
                  verify_enc_cert=False,
                  pidfile=None,
                  protocol="REST")

    puller._pull_msg_rest()

if __name__ == '__main__':
    main()
