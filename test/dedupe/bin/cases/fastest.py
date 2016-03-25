import test.dedupe.bin.chunkstore_test as case_test


if __name__ == '__main__':
    case_test.compress = False
    case_test.decompress = False
    case_test.async_compress = False
    case_test.write_delay = False
    case_test.read_delay = False
    case_test.start_test(True, True)