import {
    Format, CSVSenderConfig, Formatter,
    JSONSenderConfig, ChunkedFileSenderConfig,
    LDJSONSenderConfig,
    SendRecords
} from '../../src';

describe('Formatter', () => {
    it('incorrect format will throw', () => {
        const config: ChunkedFileSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: 'something' as any,
        };

        expect(() => new Formatter(config)).toThrowError(
            'Unsupported output format "something"'
        );
    });

    it('incorrect line_delimiter will throw', () => {
        const config: ChunkedFileSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.json,
            line_delimiter: 23423 as any,
        };

        expect(() => new Formatter(config)).toThrowError(
            'Invalid parameter line_delimiter, it must be provided and be of type string, was given Number'
        );
    });

    it('can format json data', () => {
        const config: JSONSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.json,
            fields: [],
            line_delimiter: '\n',
        };
        const data = [{ some: 'stuff' }, { other: 'things' }];
        const expectedResults = `${JSON.stringify(data)}${config.line_delimiter}`;
        const formatter = new Formatter(config);

        expectFormatResult(formatter, data, expectedResults);
    });

    it('can format raw data', () => {
        const config: ChunkedFileSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.raw,
            line_delimiter: '\t',
        };
        const data = [{ data: 'stuff' }, { data: 'things' }];

        const formatter = new Formatter(config);

        expectFormatResult(formatter, data, 'stuff\tthings\t');
    });

    it('can format tsv data', () => {
        const config: CSVSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.tsv,
            fields: [],
            include_header: false,
        };
        const data = [{ some: 'stuff', other: 'things' }];

        const formatter = new Formatter(config);

        expectFormatResult(formatter, data, '"stuff"\t"things"\n');
    });

    it('can format csv data', () => {
        const config: CSVSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.csv,
            fields: [],
            line_delimiter: '\n',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ some: 'stuff', other: 'things' }];

        const formatter = new Formatter(config);

        expectFormatResult(formatter, data, '"stuff","things"\n');
    });

    it('can format ldjson data', () => {
        const config: LDJSONSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.ldjson,
            fields: [],
            line_delimiter: '\n',
        };
        const data = [{ some: 'stuff' }, { other: 'things' }];
        const expectedData = data.map((obj) => JSON.stringify(obj)).join('\n');

        const formatter = new Formatter(config);

        expectFormatResult(formatter, data, `${expectedData}\n`);
    });

    it('can format ldjson data with an empty slice', () => {
        const config: LDJSONSenderConfig = {
            id: 'foo',
            path: 'foo',
            format: Format.ldjson,
        };

        const formatter = new Formatter(config);
        expectFormatResult(formatter, [], '');
    });

    describe('fields parameter', () => {
        it('can restrict csv output', async () => {
            const config: CSVSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.csv,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                '"stuff"\n"person"\n'
            );
        });

        it('should return an empty string if given an empty row', async () => {
            const config: CSVSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.csv,
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{}];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                ''
            );
        });

        it('can restrict csv output with header', async () => {
            const config: CSVSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.csv,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: true,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                '"some"\n"stuff"\n"person"\n'
            );
        });

        it('can restrict tsv output', async () => {
            const config: CSVSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.tsv,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                '"stuff"\n"person"\n'
            );
        });

        it('can restrict json output', async () => {
            const config: JSONSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.json,
                fields: ['some'],
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                '[{"some":"stuff"},{"some":"person"}]\n'
            );
        });

        it('can restrict ldjson output', async () => {
            const config: LDJSONSenderConfig = {
                id: 'foo',
                path: 'foo',
                format: Format.ldjson,
                fields: ['some'],
                line_delimiter: '\n',
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new Formatter(config);

            expectFormatResult(
                formatter,
                data,
                '{"some":"stuff"}\n{"some":"person"}\n'
            );
        });
    });
});

function expectFormatResult(
    formatter: Formatter,
    input: SendRecords,
    expected: string
): void {
    expect(formatter.format(input)).toEqual(expected);

    if (formatter.type === Format.ldjson
        || formatter.type === Format.tsv
        || formatter.type === Format.csv) {
        const output = [...formatter.formatIterator(input)]
            .map(([str]) => str)
            .join('');
        expect(output).toEqual(expected);
    }
}
