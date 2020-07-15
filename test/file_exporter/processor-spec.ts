import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { DataEntity } from '@terascope/job-components';
import path from 'path';
import fs from 'fs';
import { remove, ensureDir, readJson } from 'fs-extra';
import { Format } from '../../asset/src/__lib/interfaces';

function getTestFilePath(filename?: string) {
    if (filename) return path.join(__dirname, 'test_output/test', filename);
    return path.join(__dirname, 'test_output/test');
}

async function cleanTestDir() {
    const filepath = getTestFilePath();
    if (fs.existsSync(filepath)) {
        await remove(filepath);
    }
    await ensureDir(filepath);
}

describe('File exporter processor', () => {
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];
    let data2: DataEntity[];
    let data3: DataEntity[];
    let complexData: DataEntity[];
    let emptySlice: DataEntity[];
    let routeSlice: DataEntity[];
    const metaRoute1 = '0';
    const metaRoute2 = '1';

    async function makeTest(config?: any) {
        const _op = {
            _op: 'file_exporter',
            path: `${getTestFilePath()}`,
            compression: 'none',
            format: 'csv',
            line_delimiter: '\n',
            field_delimiter: ',',
            fields: [
                'field3',
                'field1'
            ],
            file_per_slice: true,
            include_header: false
        };

        const opConfig = config ? Object.assign({}, _op, config) : _op;
        harness = WorkerTestHarness.testProcessor(opConfig);

        await harness.initialize();

        workerId = harness.context.cluster.worker.id;

        return harness;
    }

    beforeEach(async () => {
        await cleanTestDir();

        data = [
            DataEntity.make(
                {
                    field1: 42,
                    field3: 'test data',
                    field2: 55
                }
            ),
            DataEntity.make({
                field1: 43,
                field3: 'more test data',
                field2: 56
            }),
            DataEntity.make({
                field1: 44,
                field3: 'even more test data',
                field2: 57
            })
        ];

        data2 = [
            DataEntity.make({ data: 'record1' }),
            DataEntity.make({ data: 'record2' }),
            DataEntity.make({ data: 'record3' })
        ];

        data3 = [DataEntity.make(
            {
                field1: 42,
                field3: 'test data',
                field2: 55,
                field4: 88
            }
        )];

        complexData = [
            DataEntity.make(
                {
                    field1: {
                        subfield1: 22,
                        subfield2: 44
                    },
                    field2: 66
                }
            ),
            DataEntity.make({
                field1: [
                    {
                        subfield1: 22,
                        subfield2: 44
                    }
                ],
                field2: 66
            })
        ];

        routeSlice = [
            DataEntity.make(
                {
                    field1: 'first',

                },
                { 'standard:route': metaRoute1 }
            ),
            DataEntity.make(
                {
                    field1: 'second',
                },
                { 'standard:route': metaRoute2 }
            )
        ];

        emptySlice = [];
    });

    afterEach(async () => {
        await cleanTestDir();
        if (harness) await harness.shutdown();
    });

    it('creates multiple CSV files with specific fields', async () => {
        const test = await makeTest();

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${workerId}.0`), 'utf-8')).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${workerId}.1`), 'utf-8')).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
    });

    it('ignores empty slices', async () => {
        const test = await makeTest();

        await test.runSlice(emptySlice);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(0);
    });

    it('creates multiple CSV files with all fields', async () => {
        const config = { fields: [] };
        const test = await makeTest(config);

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${workerId}.0`), 'utf-8')).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${workerId}.1`), 'utf-8')).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
    });

    it('creates multiple CSV files with all fields and headers', async () => {
        const config = { fields: [], include_header: true };
        const test = await makeTest(config);

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${workerId}.0`), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${workerId}.1`), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });

    it('creates a single csv file with custom fields', async () => {
        const config = { fields: ['field3', 'field1'], include_header: false, file_per_slice: false };
        const test = await makeTest(config);

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
            + '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
        );
    });

    it('creates a single csv file with custom complex fields', async () => {
        const config = { fields: ['field2', 'field1'], include_header: false, file_per_slice: false };
        const test = await makeTest(config);

        await test.runSlice(complexData);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '66,"{""subfield1"":22,""subfield2"":44}"\n'
            + '66,"[{""subfield1"":22,""subfield2"":44}]"\n'
        );
    });

    it('creates a single csv file with all fields', async () => {
        const config = { fields: [], include_header: false, file_per_slice: false };
        const test = await makeTest(config);

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });

    it('creates a single csv file and adds a header properly', async () => {
        const config = { fields: [], include_header: true, file_per_slice: false };
        const test = await makeTest(config);

        await test.runSlice(data);
        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });

    it('creates a single tsv file with a tab delimiter', async () => {
        const config = {
            fields: [],
            field_delimiter: '\t',
            include_header: false,
            file_per_slice: false
        };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '42\t"test data"\t55\n'
            + '43\t"more test data"\t56\n'
            + '44\t"even more test data"\t57\n'
        );
    });

    it('creates a single csv file with a custom delimiter', async () => {
        const config = {
            fields: [],
            field_delimiter: '^',
            include_header: false,
            file_per_slice: false
        };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '42^"test data"^55\n'
            + '43^"more test data"^56\n'
            + '44^"even more test data"^57\n'
        );
    });

    it('creates a single csv file with a custom line delimiter', async () => {
        const config = {
            fields: [],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '^'
        };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '42,"test data",55^'
            + '43,"more test data",56^'
            + '44,"even more test data",57^'
        );
    });

    it('creates a single file with line-delimited JSON records', async () => {
        const config = {
            fields: [],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.ldjson
        };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '{"field1":42,"field3":"test data","field2":55}\n'
            + '{"field1":43,"field3":"more test data","field2":56}\n'
            + '{"field1":44,"field3":"even more test data","field2":57}\n'
        );
    });

    it('filters and orders line-delimited JSON fields', async () => {
        const config = {
            fields: ['field3', 'field1'],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.ldjson
        };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '{"field3":"test data","field1":42}\n'
            + '{"field3":"more test data","field1":43}\n'
            + '{"field3":"even more test data","field1":44}\n'
        );
    });

    it('filters and line-delimited JSON fields with nested objects', async () => {
        const fields = [
            'field2',
            'field1',
            'subfield1',
            'subfield2'
        ];
        const config = {
            fields,
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.ldjson
        };
        const test = await makeTest(config);

        await test.runSlice(complexData);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            '{"field2":66,"field1":{"subfield1":22,"subfield2":44}}\n'
            + '{"field2":66,"field1":[{"subfield1":22,"subfield2":44}]}\n'
        );
    });

    it('creates a single file with a JSON record for `json` format', async () => {
        const config = {
            fields: [],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: true,
            line_delimiter: '\n',
            format: Format.json
        };
        const test = await makeTest(config);

        await test.runSlice(data3);
        await test.runSlice(data3);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${workerId}.0`), 'utf-8')).toEqual(
            '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${workerId}.1`), 'utf-8')).toEqual(
            '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
        );
    });

    it('ignores non-existant fields in ldjson', async () => {
        const config = {
            fields: ['field1', 'field2', 'field8'],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: true,
            line_delimiter: '\n',
            format: Format.ldjson
        };
        const test = await makeTest(config);

        await test.runSlice(data3);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(`${workerId}.0`), 'utf-8')).toEqual(
            '{"field1":42,"field2":55}\n'
        );
    });

    it('creates a single file wiht raw records on each line', async () => {
        const config = {
            fields: ['field1', 'field2', 'field8'],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.raw
        };
        const test = await makeTest(config);

        await test.runSlice(data2);

        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(workerId), 'utf-8')).toEqual(
            'record1\n'
            + 'record2\n'
            + 'record3\n'
        );
    });

    it('will not respect metadata routing when used normally', async () => {
        const config = {
            fields: ['field1'],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.json,
        };

        const test = await makeTest(config);

        const routePath = getTestFilePath();

        await test.runSlice(routeSlice);

        const slice = `${workerId}.0`;
        const results = fs.readFileSync(getTestFilePath(slice), 'utf-8');

        expect(fs.readdirSync(routePath)).toBeArrayOfSize(1);
        expect(JSON.parse(results)).toBeArrayOfSize(2);
    });

    it('can respect metadata routing when used as part of routed_sender', async () => {
        const config = {
            fields: ['field1'],
            field_delimiter: ',',
            include_header: false,
            file_per_slice: false,
            line_delimiter: '\n',
            format: Format.json,
            _key: 'a'
        };
        const test = await makeTest(config);

        const expectedResults1 = Object.assign({}, routeSlice[0]);
        const expectedResults2 = Object.assign({}, routeSlice[1]);

        await test.runSlice(routeSlice);

        const routePath1 = getTestFilePath(metaRoute1);
        const routePath2 = getTestFilePath(metaRoute2);

        const slice = `${workerId}.0`;

        const results1 = await readJson(`${routePath1}/${slice}`);
        const results2 = await readJson(`${routePath2}/${slice}`);

        expect(fs.readdirSync(routePath1).length).toEqual(1);
        expect(results1).toEqual([expectedResults1]);

        expect(fs.readdirSync(routePath2).length).toEqual(1);
        expect(results2).toEqual([expectedResults2]);
    });
});
