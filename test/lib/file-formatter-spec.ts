import FileFormatter from '../../asset/src/__lib/file-formatter';
import { Format, CSVConfig } from '../../asset/src/__lib/interfaces';

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
});
