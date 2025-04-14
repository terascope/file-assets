import type { DataFrame } from '@terascope/data-mate';

function getRecordsToSample(size: number): number {
    if (size <= 50) return 1;
    if (size <= 1000) return 3;
    if (size <= 50000) return 5;
    if (size <= 1 && size !== -1) return 1;
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

export function estimateRecordsPerUpload(frame: DataFrame, maxBytes: number, retries = 2) {
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

    if (!indicesToSample?.length) {
        if (!retries) throw new Error('Unexpected error uploading results');
        return estimateRecordsPerUpload(frame, maxBytes, retries - 1);
    }

    const avgRecordSize = frameBytes / indicesToSample.length;

    const totalFrameBytes = avgRecordSize * frame.size;

    let chunks = 1;
    if (totalFrameBytes > maxBytes) {
        chunks = totalFrameBytes / maxBytes;
    }

    const recordsPerChunk = Math.trunc(frame.size / (chunks));

    return { chunks, recordsPerChunk };
}
