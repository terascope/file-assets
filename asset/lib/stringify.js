'use strict';

function stringify(record, fields) {
    let serializedRecord = '{';
    fields.forEach((field) => {
        // Can't just check for `record[field]` since `null`, `undefined`, and `0` will drop
        // fields
        if (Object.keys(record).includes(field)) {
            serializedRecord = `${serializedRecord}"${field}":${JSON.stringify(record[field])},`;
        }
    });
    return `${serializedRecord.slice(0, -1)}}`;
}

module.exports = {
    stringify
};
