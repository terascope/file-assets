import type { DataFrame } from '@terascope/data-mate';

function getRecordsToSample(size: number): number {
    if (size < 1 && size !== -1) return 1;
    if (size < 100) return 3;
    return 10;
}

function getRandomIndex(size: number, collected: number[], retries = 5): number | undefined {
    const idx = Math.floor(Math.random() * size);

    if (collected.includes(idx)) {
        // if we already used this index try again
        if (retries) return getRandomIndex(size, collected, retries - 1);
    } else {
        collected.push(idx);
        return idx;
    }
}

export function estimateRecordsPerUpload(frame: DataFrame, maxBytes: number) {
    const indicesToSample: number[] = [];
    const sampleSize = getRecordsToSample(frame.size);

    let frameBytes = 0;
    for (let i = 0; i < sampleSize; i++) {
        const index = getRandomIndex(frame.size, indicesToSample, 5);
        if (index != null) {
            frameBytes += JSON.stringify(frame.getRow(index))?.length ?? 0;
            if (frameBytes >= maxBytes) {
                // stop if already at the max
                break;
            }
        }
    }

    // in case failed to sample
    // if (!indicesToSample?.length) return 1;

    const avgRecordSize = frameBytes / indicesToSample.length;
    const total = avgRecordSize * frame.size;
    const recordsPerChunk = Math.ceil(total / maxBytes);
    const totalChunks = Math.ceil(frame.size / recordsPerChunk);
    return { recordsPerChunk, totalChunks, avgRecordSize };
}

// function simpleSend(frame: DataFrame) {
//     const { recordsPerChunk, totalChunks } =
// // estimateRecordsPerUpload(frame, MAX_CHUNK_SIZE_BYTES);

//     function* generator(_i) {
//         let start = 0;
//         for (let i = 0; i < totalChunks; i++) {
//             const records = frame.slice(start, recordsPerChunk + start);
//             const body = records.toJSON()
//                 .map((el) => JSON.stringify(el))
//                 .join('\n');
//             start = start + recordsPerChunk;

//             if (body.length < MAX_CHUNK_SIZE_BYTES) {
//                 yield body;
//             } else {

//             }

//             // try convert to generator to share code instead
//             // if (totalChunks > 0) {
//             //     // start upload
//             // } else {
//             //     // start multi
//             // }
//         }
//     }
// }
