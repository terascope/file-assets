import { Format, CSVConfig, FileFormatter } from '../src';

describe('FileFormatter', () => {
    it('incorrect format will throw', () => {
        const format = 'something';
        const config = {
            format,
            fields: [],
            line_delimiter: '\n'
        };

        // @ts-expect-error
        expect(() => new FileFormatter(format, config)).toThrow();
    });

    it('incorrect line_delimiter will throw', () => {
        const format = Format.json;
        const config = {
            format,
            fields: [],
        };

        const config2 = {
            format,
            line_delimiter: 23423,
            fields: [],
        };

        // @ts-expect-error
        expect(() => new FileFormatter(format, config)).toThrow();
        // @ts-expect-error
        expect(() => new FileFormatter(format, config2)).toThrow();
    });

    it('can format json data', () => {
        const format = Format.json;
        const config: CSVConfig = {
            format,
            fields: [],
            line_delimiter: '\n',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ some: 'stuff' }, { other: 'things' }];

        const formatter = new FileFormatter(format, config);

        expect(formatter.format(data)).toEqual(`${JSON.stringify(data)}${config.line_delimiter}`);
    });

    it('can format raw data', () => {
        const format = Format.raw;
        const config: CSVConfig = {
            format,
            fields: [],
            line_delimiter: '\t',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ data: 'stuff' }, { data: 'things' }];

        const formatter = new FileFormatter(format, config);

        expect(formatter.format(data)).toEqual('stuff\tthings\t');
    });

    it('can format tsv data', () => {
        const format = Format.tsv;
        const config: CSVConfig = {
            format,
            fields: [],
            line_delimiter: '\t',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ some: 'stuff', other: 'things' }];

        const formatter = new FileFormatter(format, config);

        expect(formatter.format(data)).toEqual('"stuff"\t"things"\t');
    });

    it('can format csv data', () => {
        const format = Format.csv;
        const config: CSVConfig = {
            format,
            fields: [],
            line_delimiter: '\n',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ some: 'stuff', other: 'things' }];

        const formatter = new FileFormatter(format, config);

        expect(formatter.format(data)).toEqual('"stuff","things"\n');
    });

    it('can format ldjson data', () => {
        const format = Format.ldjson;
        const config: CSVConfig = {
            format,
            fields: [],
            line_delimiter: '\n',
            include_header: false,
            field_delimiter: ','
        };
        const data = [{ some: 'stuff' }, { other: 'things' }];
        const expectedData = data.map((obj) => JSON.stringify(obj)).join('\n');

        const formatter = new FileFormatter(format, config);

        expect(formatter.format(data)).toEqual(`${expectedData}\n`);
    });

    describe('fields parameter', () => {
        it('can restrict csv output', async () => {
            const format = Format.csv;
            const config: CSVConfig = {
                format,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new FileFormatter(format, config);

            expect(formatter.format(data)).toEqual(`"stuff"${'\n'}"person"${'\n'}`);
        });

        it('can restrict tsv output', async () => {
            const format = Format.tsv;
            const config: CSVConfig = {
                format,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new FileFormatter(format, config);

            expect(formatter.format(data)).toEqual(`"stuff"${'\n'}"person"${'\n'}`);
        });

        it('can restrict json output', async () => {
            const format = Format.json;
            const config: CSVConfig = {
                format,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new FileFormatter(format, config);

            expect(formatter.format(data)).toEqual(`[{"some":"stuff"},{"some":"person"}]${'\n'}`);
        });

        it('can restrict ldjson output', async () => {
            const format = Format.ldjson;
            const config: CSVConfig = {
                format,
                fields: ['some'],
                line_delimiter: '\n',
                include_header: false,
                field_delimiter: ','
            };
            const data = [{ some: 'stuff', other: 'things' }, { some: 'person', key: 'field' }];

            const formatter = new FileFormatter(format, config);

            expect(formatter.format(data)).toEqual(`{"some":"stuff"}${'\n'}{"some":"person"}${'\n'}`);
        });
    });
});
