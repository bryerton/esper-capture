.. image:: https://travis-ci.org/bryerton/esper-wave.svg?branch=master
    :target: https://travis-ci.org/bryerton/esper-wave

==========
ESPER WAVE
==========

Overview
--------
A python-based utility to display waveforms and perform stripline-like analysis of ESPER-based experiments

Installation
------------
The recommended installation method is via pip

  To install:
    `pip install esper-wave`
  To upgrade:
    `pip install -U esper-wave`
  To run it locally from the github source:
    `python -m esper_wave`

Interactive
-----------
 Command:
  `esper-wave [-h] [-u USER] [-p PASS] <url>`

 Purpose:
  Connects to an esper service located at `url`
 
 Options:
  `-h`
  
  `--help`
   Print out help for this subcommand 
 
  `-u USER`
  
  `--user USER`
   User to use for HTTP basic authentication
 
  `-p PASS`
  
  `--password PASS`
   Password to use for HTTP basic authentication. If `-u` is specified, but `-p` is not, the user will be prompted for a password

  `url`
   Location of ESPER web service given in standard web URL format. If the port is excluded, it defaults to 80
