# ChiTools

This repo is a grab-bag of various scripts, snippets, and tools for working with assorted systems and data in Chicago. Feel free to dig around and see if there's anything useful to you.

## 311

The `311` directory contains assorted scripts for working with 311-related data, largely extracted from Chicago's data portal (data.cityofchicago.org).

## NightlyServer

The `nightlyserver` directory contains two scripts which can, together, be used to create an Open311 server from Chicago's reporting database (which is updated nightly). The reporting database is restricted access; you'll need to be on the city's internal network or VPN to use it.

The `collector` portion should be run on a computer that has access to the reporting database; it is a simple script that gathers all the service requests created or updated over a given time period and sends them off to your Open311 server (the `server` portion detailed below) to later be formatted and served as Open311 data.

The `server` portion can be run anywhere and does not require access to the city's reporting database. The `collector` above gathers data from the reporting database and sends it to this server, which is a Python Flask app that uses MongoDB as its backend. Simply configure and run it by doing:

```python app.py```
