# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Fixed
- platform\_datasets hbase table creation

### Changed
- Datasets with empty source are ignored
- Externalized build logic from Jenkins to shell script so it can be reused
- Archive container in s3 or swift is created automatically by hdfs-cleaner

## [0.1.1] 2016-09-13
### Changed
- Enhanced CI support

## [0.1.0] 2016-07-01
### First version
- Data management API and daemons
