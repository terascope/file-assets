const {
    ENCRYPT_MINIO = 'false',
    MINIO_HOST = 'http://127.0.0.1:9000',
    MINIO_ACCESS_KEY = 'minioadmin',
    MINIO_SECRET_KEY = 'minioadmin',
    CERT_PATH = '',
} = process.env;

export {
    ENCRYPT_MINIO, MINIO_HOST, MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY, CERT_PATH
};
