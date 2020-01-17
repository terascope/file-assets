'use strict';

const { legacyReaderShim } = require('@terascope/job-components');
const Processor = require('./processor');
const Schema = require('./schema');

exports = legacyReaderShim(Processor, Schema);
