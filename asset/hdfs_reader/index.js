'use strict';

const { legacyReaderShim } = require('@terascope/job-components');
const Fetcher = require('./fetcher');
const Slicer = require('./slicer');
const Schema = require('./schema');

exports = legacyReaderShim(Slicer, Fetcher, Schema);
