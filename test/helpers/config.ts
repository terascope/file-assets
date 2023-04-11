const {
    MINIO_HOST = 'http://127.0.0.1:9000',
    MINIO_ACCESS_KEY = 'minioadmin',
    MINIO_SECRET_KEY = 'minioadmin',
} = process.env;

export { MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY };
