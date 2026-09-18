const {
    ENCRYPT_CEPH = 'false',
    CEPH_HOST = 'http://127.0.0.1:9500',
    CEPH_ACCESS_KEY = 'cephtestaccesskey',
    CEPH_SECRET_KEY = 'cephtestsecretkey',
    CERT_PATH = '',
} = process.env;

export {
    ENCRYPT_CEPH, CEPH_HOST, CEPH_ACCESS_KEY,
    CEPH_SECRET_KEY, CERT_PATH
};
