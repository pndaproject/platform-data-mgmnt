# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.3.0] 2018-08-28
### Added:
- PNDA-4426: Added a config to control the log level for the dataset service
### Fixed:
- PNDA-4879: Added command to create sub directories in s3 or swift
- PNDA-4897: S3 container create issue fixed at the time of data-archiving

## [0.2.2] 2018-02-10
### Changed:
- PNDA-3601: Disable emailtext in Jenkins file and replace it with notifier stage and job

## [0.2.1] 2017-11-24
### Added:
- PNDA-2445: Support for Hortonworks HDP

### Fixed
- PNDA-3427: Configure data-service with the webhdfs services.

## [0.2.0] 2017-01-20
### Changed
- PNDA-2485: Pinned all python libraries to strict version numbers

## [0.1.2] 2016-12-12
### Fixed
- Platform\_datasets hbase table creation

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
